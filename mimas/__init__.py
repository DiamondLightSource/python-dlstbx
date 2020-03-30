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
    if type(mimasobject.isitagridscan) != bool:
        raise ValueError(f"{mimasobject!r} has non-boolean isitagridscan")
    validate(mimasobject.event, expectedtype=MimasEvent)
    if type(mimasobject.getsweepslistfromsamedcg) not in (list, tuple):
        raise ValueError(
            f"{mimasobject!r} getsweepslistfromsamedcg must be a tuple, not {type(mimasobject.getsweepslistfromsamedcg)}"
        )
    for sweep in mimasobject.getsweepslistfromsamedcg:
        validate(sweep, expectedtype=MimasISPyBSweep)


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


@validate.register(MimasISPyBJobInvocation)
def _(mimasobject: MimasISPyBJobInvocation, expectedtype=None):
    if expectedtype and not isinstance(mimasobject, expectedtype):
        raise ValueError(f"{mimasobject!r} is not a {expectedtype}")
    if type(mimasobject.DCID) != int:
        raise ValueError(f"{mimasobject!r} has non-integer DCID")
    if mimasobject.autostart not in (True, False):
        raise ValueError(f"{mimasobject!r} has invalid autostart property")
    if type(mimasobject.parameters) not in (list, tuple):
        raise ValueError(
            f"{mimasobject!r} parameters must be a tuple, not {type(mimasobject.parameters)}"
        )
    for parameter in mimasobject.parameters:
        validate(parameter, expectedtype=MimasISPyBParameter)
    if type(mimasobject.recipe) != str:
        raise ValueError(f"{mimasobject!r} has non-string recipe")
    if not mimasobject.recipe:
        raise ValueError(f"{mimasobject!r} has empty recipe string")
    if type(mimasobject.sweeps) not in (list, tuple):
        raise ValueError(
            f"{mimasobject!r} sweeps must be a tuple, not {type(mimasobject.sweeps)}"
        )
    for sweep in mimasobject.sweeps:
        validate(sweep, expectedtype=MimasISPyBSweep)


@validate.register(MimasISPyBParameter)
def _(mimasobject: MimasISPyBParameter, expectedtype=None):
    if expectedtype and not isinstance(mimasobject, expectedtype):
        raise ValueError(f"{mimasobject!r} is not a {expectedtype}")
    if type(mimasobject.key) != str:
        raise ValueError(f"{mimasobject!r} has non-string key")
    if not mimasobject.key:
        raise ValueError(f"{mimasobject!r} has an empty key")
    if type(mimasobject.value) != str:
        raise ValueError(
            f"{mimasobject!r} value must be a string, not {type(mimasobject.value)}"
        )


@validate.register(MimasISPyBSweep)
def _(mimasobject: MimasISPyBSweep, expectedtype=None):
    if expectedtype and not isinstance(mimasobject, expectedtype):
        raise ValueError(f"{mimasobject!r} is not a {expectedtype}")
    if type(mimasobject.DCID) != int:
        raise ValueError(f"{mimasobject!r} has non-integer DCID")
    if mimasobject.DCID <= 0:
        raise ValueError(f"{mimasobject!r} has an invalid DCID")
    if type(mimasobject.start) != int:
        raise ValueError(f"{mimasobject!r} has non-integer start image")
    if mimasobject.start <= 0:
        raise ValueError(f"{mimasobject!r} has an invalid start image")
    if type(mimasobject.end) != int:
        raise ValueError(f"{mimasobject!r} has non-integer end image")
    if mimasobject.end < mimasobject.start:
        raise ValueError(f"{mimasobject!r} has an invalid end image")
