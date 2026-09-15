"""Per-visit settings for the XChem autoprocessing pipelines.

A labxchem visit may hold a `.config.yaml` (or the older `.user.yaml`) naming
its target and steering how it is processed::

    data:
      acronym: A71EV2A            # cached by the pipeline
    autoprocessing:
      enabled: true               # process this visit at all?
      comparator_threshold: 150   # datasets PanDDA2 waits for before starting
      pipedream: false            # run Pipedream?
      pandda:                     # false to skip PanDDA2, or a mapping of
        high_res_lower_limit: 2.5 # extra --key=value args to run it with
      notify:                     # who to mail when collate finishes
        - someone@diamond.ac.uk

Every key is optional, and leaving one out is not the same as setting it false,
so the fields below default to None to mean "the user said nothing".

Precedence: a recipe that set the parameter explicitly beats the file, the file
beats the parameter model's default. `VisitConfig.resolve` implements that from
pydantic's `model_fields_set`, so a recipe hardcoding a parameter pins it for
every visit it covers -- leave it out of the recipe for the file to have a say.

The files are hand-edited, so nothing here raises on bad input: settings that
fail to parse are logged and dropped, keeping the cached acronym either way.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import pydantic
import yaml

CONFIG_FILENAMES = (".config.yaml", ".user.yaml")

log = logging.getLogger("dlstbx.util.xchem_config")


class VisitConfig(pydantic.BaseModel):
    """A visit's config file, flattened: `acronym` comes from the `data`
    section, the rest from `autoprocessing`."""

    model_config = pydantic.ConfigDict(extra="allow")

    acronym: str | None = None
    enabled: bool | None = None
    comparator_threshold: int | None = pydantic.Field(default=None, gt=0)
    pipedream: bool | None = None
    pandda: bool | dict[str, Any] | None = None
    notify: list[str] = pydantic.Field(default_factory=list)

    @pydantic.field_validator("notify", mode="before")
    @classmethod
    def _addresses(cls, value):
        """Accept one address, or a comma-separated string, as well as a list."""
        if isinstance(value, str):
            value = value.split(",")
        return [str(v).strip() for v in value or [] if str(v).strip()]

    @property
    def pandda_args(self) -> dict:
        """PanDDA2 arguments, where `pandda` was given as a mapping."""
        return self.pandda if isinstance(self.pandda, dict) else {}

    @property
    def run_pandda(self) -> bool | None:
        """Whether to run PanDDA2. A mapping says how to run it, not whether."""
        return self.pandda if isinstance(self.pandda, bool) else None

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


def config_path(visit_dir) -> Path:
    """A visit's config file: whichever name is already there, else the
    preferred one."""
    visit_dir = Path(visit_dir)
    for name in CONFIG_FILENAMES:
        if (visit_dir / name).is_file():
            return visit_dir / name
    return visit_dir / CONFIG_FILENAMES[0]


def _read(visit_dir, logger) -> tuple[str, dict]:
    """A visit config file's text and its parsed contents."""
    path = config_path(visit_dir)
    try:
        text = path.read_text() if path.is_file() else ""
        return text, _mapping(yaml.safe_load(text))
    except (OSError, yaml.YAMLError) as e:
        logger.warning(f"Ignoring unreadable visit config {path}: {e}")
        return "", {}


def load_visit_config(visit_dir, logger=log) -> VisitConfig:
    """Read a visit's config file, or an all-defaults config if it has none."""
    _, raw = _read(visit_dir, logger)
    acronym = _mapping(raw.get("data")).get("acronym")
    try:
        return VisitConfig(acronym=acronym, **_mapping(raw.get("autoprocessing")))
    except pydantic.ValidationError as e:
        # keep the acronym: losing it would make the visit undiscoverable
        logger.warning(f"Ignoring invalid settings in {config_path(visit_dir)}: {e}")
        return VisitConfig(acronym=acronym)


def cache_acronym(visit_dir, acronym: str, logger=log) -> None:
    """Add a visit's target acronym to its config file, leaving whatever the
    user wrote untouched. Does nothing if one is already recorded."""
    text, raw = _read(visit_dir, logger)
    if _mapping(raw.get("data")).get("acronym") is not None:
        return

    entry = f"data:\n  acronym: {acronym}"
    empty_section = re.compile(r"^data:[ \t]*$", re.MULTILINE)
    if empty_section.search(text):
        updated = empty_section.sub(entry, text, count=1)
    else:
        separator = "" if not text or text.endswith("\n") else "\n"
        updated = f"{text}{separator}{entry}\n"

    path = config_path(visit_dir)
    try:
        path.write_text(updated)
    except OSError as e:
        logger.warning(f"Could not cache acronym {acronym} to {path}: {e}")
