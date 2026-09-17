"""Per-visit settings for the XChem autoprocessing pipelines.

A labxchem visit may hold a `.user.yaml` naming its target and steering how
it is processed::

    data:
      acronym: A71EV2A            # cached by the pipeline
    autoprocessing:
      enabled: true               # process this visit at all?
      comparator_threshold: 150   # datasets PanDDA2 waits for before starting
      pipedream: false            # run Pipedream?
      pandda:                     # extra PanDDA2 --key=value arguments
        high_res_lower_limit: 2.5
    notify: someone@diamond.ac.uk   # who to mail when collate finishes

Every key is optional, and leaving one out is not the same as setting it false,
so the fields below default to None to mean "the user said nothing".

Precedence: a recipe that set the parameter explicitly beats the file, the file
beats the parameter model's default. `VisitConfig.resolve` implements that from
pydantic's `model_fields_set`, so a recipe hardcoding a parameter pins it for
every visit it covers -- leave it out of the recipe for the file to have a say.

The files are hand-edited, so nothing here raises on bad input: a setting that
fails to parse is logged and dropped on its own, leaving the rest of the file
in force.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pydantic
import yaml

CONFIG_FILENAME = ".user.yaml"

log = logging.getLogger("dlstbx.util.xchem_config")


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

    def resolve(self, key: str, parameters, param_key: str | None = None):
        """The value to use for a setting: the recipe's where it set one
        explicitly, else this visit's config, else the `parameters` default.

        `param_key` names the field on the trigger's parameter model when it
        differs from the config's `key`.
        """
        param_key = param_key or key
        recipe_value = getattr(parameters, param_key)
        if param_key in parameters.model_fields_set:
            return recipe_value
        value = getattr(self, key)
        return recipe_value if value is None else value


def _mapping(value) -> dict:
    """A config section, or {} where it is absent or empty (`data:` alone)."""
    return value if isinstance(value, dict) else {}


def _read(visit_dir, logger) -> tuple[Path, str, dict | None]:
    """A visit config file's path, text and parsed contents, the last being
    None if the file is there but could not be read or parsed."""
    path = Path(visit_dir) / CONFIG_FILENAME
    try:
        text = path.read_text() if path.is_file() else ""
        return path, text, _mapping(yaml.safe_load(text))
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as e:
        logger.warning(f"Ignoring unreadable visit config {path}: {e}")
        return path, "", None


def load_visit_config(visit_dir, logger=log) -> VisitConfig:
    """Read a visit's config file, or an all-defaults config if it has none.

    A setting that fails validation is dropped on its own and the rest of the
    file still applies, so one typo cannot cost a visit its whole config.
    """
    path, _, raw = _read(visit_dir, logger)
    raw = raw or {}
    values = {str(k): v for k, v in _mapping(raw.get("autoprocessing")).items()}
    values["acronym"] = _mapping(raw.get("data")).get("acronym")
    values["notify"] = raw.get("notify")
    try:
        return VisitConfig(**values)
    except pydantic.ValidationError as e:
        # every bad field is reported in one pass, so dropping them all leaves
        # only settings that validate
        bad = {str(err["loc"][0]) for err in e.errors() if err["loc"]}
        logger.warning(f"Ignoring invalid {', '.join(sorted(bad))} in {path}: {e}")
        return VisitConfig(**{k: v for k, v in values.items() if k not in bad})


def cache_acronym(visit_dir, acronym: str, logger=log) -> None:
    """Append a visit's target acronym to its config file, leaving whatever the
    user wrote untouched. Does nothing if one is already recorded.

    A file that already carries a `data` section gets a second one; YAML takes
    the last, and `acronym` is the only key the pipeline puts there.
    """
    path, text, raw = _read(visit_dir, logger)
    if raw is None:
        # the file is there but unreadable; appending would destroy it
        return
    if _mapping(raw.get("data")).get("acronym") is not None:
        return
    separator = "" if not text or text.endswith("\n") else "\n"
    # dump just the new fragment: appending keeps the user's comments and key
    # order
    entry = yaml.dump({"data": {"acronym": acronym}}, default_flow_style=False)
    try:
        path.write_text(f"{text}{separator}{entry}")
    except OSError as e:
        logger.warning(f"Could not cache acronym {acronym} to {path}: {e}")
