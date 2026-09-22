"""Per-visit settings for the XChem autoprocessing pipelines.

Settings live in a `.user.yaml` the users own, under a visit's `processing`
directory, which the pipeline only ever reads::

    # <visit>/processing/.user.yaml
    autoprocessing:
      enabled: true               # process this visit at all?
      comparator_threshold: 150   # datasets PanDDA2 waits for before starting
      pipedream: false            # run Pipedream?
      pandda:                     # extra PanDDA2 --key=value arguments
        high_res_lower_limit: 2.5
    notify: someone@diamond.ac.uk   # who to mail when collate finishes

The target acronym is cached separately, in a `.user.yaml` at the top of the
visit, because that is the one the pipeline can write::

    # <visit>/.user.yaml
    data:
      acronym: A71EV2A

Visits predating the split keep their settings alongside the cached acronym,
and are still read from there when they have no `processing` file.

A recipe that set the parameter explicitly beats the file, the file
beats the parameter model's default.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pydantic
import yaml

# written by the pipeline, holding only the cached acronym
CACHE_FILE = ".user.yaml"
# written by users; the pipeline never writes here
SETTINGS_FILE = "processing/.user.yaml"


class VisitConfig(pydantic.BaseModel):
    """A visit's config file, flattened: `acronym` comes from the `data`
    section, `notify` is top level, the rest come from `autoprocessing`."""

    # extras are forbidden so a misspelled key is reported rather than
    # silently ignored; load_visit_config drops it and keeps the rest
    model_config = pydantic.ConfigDict(extra="forbid")

    acronym: str | None = None
    enabled: bool | None = None
    comparator_threshold: int | None = pydantic.Field(default=None, gt=0)
    pipedream: bool | None = None
    pandda: dict[str, Any] | None = None
    notify: str | None = None

    def resolve(self, key: str, parameters):
        """The value to use for a setting: the recipe's where it set one
        explicitly, else this visit's config, else the `parameters` default.

        `key` names the field on both this model and the trigger's `parameters`
        model, so the two have to agree on what a setting is called.
        """
        recipe_value = getattr(parameters, key)
        if key in parameters.model_fields_set:
            return recipe_value
        value = getattr(self, key)
        return recipe_value if value is None else value


def _mapping(value) -> dict:
    """A config section, or {} where it is absent or empty."""
    return value if isinstance(value, dict) else {}


def _read(path: Path, logger) -> dict | None:
    """A config file's parsed contents, or None if it is there but unreadable."""
    try:
        text = path.read_text() if path.is_file() else ""
        return _mapping(yaml.safe_load(text))
    except Exception as e:
        logger.warning(f"Ignoring unreadable visit config {path}: {e}")
        return None


def load_visit_config(visit_dir, logger) -> VisitConfig:
    """Read a visit's settings and cached acronym, or an all-defaults config.

    A setting that fails validation is dropped on its own and the rest of the
    file still applies, so one typo cannot cost a visit its whole config.
    """
    visit_dir = Path(visit_dir)
    cache_path = visit_dir / CACHE_FILE
    cache = _read(cache_path, logger) or {}

    # visits predating the split keep their settings in the cache file
    settings_path = visit_dir / SETTINGS_FILE
    if settings_path.is_file():
        settings = _read(settings_path, logger) or {}
    else:
        settings_path, settings = cache_path, cache

    values = {str(k): v for k, v in _mapping(settings.get("autoprocessing")).items()}
    values["acronym"] = _mapping(cache.get("data")).get("acronym")
    values["notify"] = settings.get("notify")
    try:
        return VisitConfig(**values)
    except pydantic.ValidationError as e:
        # only return settings that validate
        bad = {str(err["loc"][0]) for err in e.errors() if err["loc"]}
        logger.warning(
            f"Ignoring invalid {', '.join(sorted(bad))} in {settings_path}: {e}"
        )
        return VisitConfig(**{k: v for k, v in values.items() if k not in bad})


def cache_acronym(visit_dir, acronym: str, logger) -> None:
    """Cache a visit's target acronym so that a beamline visit can be linked
    with a labxchem one. Does nothing if an acronym is already recorded.

    Written at the top of the visit, not under `processing`, because that is
    where the pipeline can write.
    """
    path = Path(visit_dir) / CACHE_FILE
    raw = _read(path, logger)
    if raw is None:
        return
    data = _mapping(raw.get("data"))
    if data.get("acronym") is not None:
        return

    raw["data"] = {**data, "acronym": acronym}
    try:
        path.write_text(yaml.dump(raw, default_flow_style=False, sort_keys=False))
    except OSError as e:
        logger.warning(f"Could not cache acronym {acronym} to {path}: {e}")
