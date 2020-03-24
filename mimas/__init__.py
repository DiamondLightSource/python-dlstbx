import collections
import enum
import functools

MimasEvent = enum.Enum("MimasEvent", "START END")

MimasScenario = collections.namedtuple(
    "MimasScenario",
    (
        "DCID",
        "event",  # MimasEvent
        "beamline",
        "runstatus",
        "spacegroup",
        "unitcell",
        "default_recipes",
        "isitagridscan",
        "getsweepslistfromsamedcg",
    ),
)

MimasRecipeInvocation = collections.namedtuple(
    "MimasRecipeInvocation", ("DCID", "recipe")
)

MimasISPyBJobInvocation = collections.namedtuple(
    "MimasISPyBJobInvocation",
    (
        "DCID",
        "autostart",
        "comment",
        "displayname",
        "parameters",
        "recipe",
        "source",
        "sweeps",
        "triggervariables",
    ),
)

MimasISPyBParameter = collections.namedtuple("MimasISPyBParameter", "key, value")

MimasISPyBSweep = collections.namedtuple("MimasISPyBSweep", "DCID, start, end")

MimasISPyBTriggerVariable = collections.namedtuple(
    "MimasISPyBTriggerVariable", "key, value"
)


@functools.singledispatch
def validate(mimasobject, expectedtype=None):
    """
    A generic validation function that can (recursively) validate any Mimas*
    object for consistency and semantic correctness.
    If any issues are found a ValueError is raised, returns None otherwise.
    """
    raise ValueError(f"{mimasobject!r} is not a known Mimas object")


@validate.register(MimasScenario)
def _(mimasobject: MimasScenario, expectedtype=None):
    if expectedtype and not isinstance(mimasobject, expectedtype):
        raise ValueError(f"{mimasobject!r} is not a {expectedtype}")
    if type(mimasobject.DCID) != int:
        raise ValueError(f"{mimasobject!r} has non-integer DCID")
    validate(mimasobject.event, expectedtype=MimasEvent)


@validate.register(MimasEvent)
def _(mimasobject: MimasEvent, expectedtype=None):
    if expectedtype and not isinstance(mimasobject, expectedtype):
        raise ValueError(f"{mimasobject!r} is not a {expectedtype}")


@validate.register(MimasRecipeInvocation)
def _(mimasobject: MimasRecipeInvocation, expectedtype=None):
    if expectedtype and not isinstance(mimasobject, expectedtype):
        raise ValueError(f"{mimasobject!r} is not a {expectedtype}")
    if type(mimasobject.DCID) != int:
        raise ValueError(f"{mimasobject!r} has non-integer DCID")
    if type(mimasobject.recipe) != str:
        raise ValueError(f"{mimasobject!r} has non-string recipe")
    if not mimasobject.recipe:
        raise ValueError(f"{mimasobject!r} has empty recipe string")
