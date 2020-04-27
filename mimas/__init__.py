import collections
import enum
import functools
import numbers

MimasDCClass = enum.Enum("MimasDCClass", "GRIDSCAN ROTATION SCREENING UNDEFINED")

MimasEvent = enum.Enum("MimasEvent", "START END")

MimasScenario = collections.namedtuple(
    "MimasScenario",
    (
        "DCID",
        "dcclass",  # MimasDCClass
        "event",  # MimasEvent
        "beamline",
        "runstatus",
        "spacegroup",
        "unitcell",  # None or MimasISPyBUnitCell
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

MimasISPyBUnitCell = collections.namedtuple(
    "MimasISPyBUnitCell", "a, b, c, alpha, beta, gamma"
)
MimasISPyBUnitCell.string = property(
    lambda uc: ",".join(map(str, uc._asdict().values()))
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
    validate(mimasobject.dcclass, expectedtype=MimasDCClass)
    validate(mimasobject.event, expectedtype=MimasEvent)
    if type(mimasobject.getsweepslistfromsamedcg) not in (list, tuple):
        raise ValueError(
            f"{mimasobject!r} getsweepslistfromsamedcg must be a tuple, not {type(mimasobject.getsweepslistfromsamedcg)}"
        )
    for sweep in mimasobject.getsweepslistfromsamedcg:
        validate(sweep, expectedtype=MimasISPyBSweep)
    if mimasobject.unitcell is not None:
        validate(mimasobject.unitcell, expectedtype=MimasISPyBUnitCell)


@validate.register(MimasDCClass)
def _(mimasobject: MimasDCClass, expectedtype=None):
    if expectedtype and not isinstance(mimasobject, expectedtype):
        raise ValueError(f"{mimasobject!r} is not a {expectedtype}")


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


@validate.register(MimasISPyBUnitCell)
def _(mimasobject: MimasISPyBUnitCell, expectedtype=None):
    if expectedtype and not isinstance(mimasobject, expectedtype):
        raise ValueError(f"{mimasobject!r} is not a {expectedtype}")
    if not isinstance(mimasobject.a, numbers.Real) or mimasobject.a <= 0:
        raise ValueError(f"{mimasobject!r} has invalid length a")
    if not isinstance(mimasobject.b, numbers.Real) or mimasobject.b <= 0:
        raise ValueError(f"{mimasobject!r} has invalid length b")
    if not isinstance(mimasobject.c, numbers.Real) or mimasobject.c <= 0:
        raise ValueError(f"{mimasobject!r} has invalid length c")
    if (
        not isinstance(mimasobject.alpha, numbers.Real)
        or not 0 < mimasobject.alpha < 180
    ):
        raise ValueError(f"{mimasobject!r} has invalid angle alpha")
    if not isinstance(mimasobject.beta, numbers.Real) or not 0 < mimasobject.beta < 180:
        raise ValueError(f"{mimasobject!r} has invalid angle beta")
    if (
        not isinstance(mimasobject.gamma, numbers.Real)
        or not 0 < mimasobject.gamma < 180
    ):
        raise ValueError(f"{mimasobject!r} has invalid angle gamma")


@functools.singledispatch
def zocalo_message(mimasobject):
    """
    A generic function that (recursively) transforms any Mimas* object
    into serializable objects that can be sent via zocalo.
    If any issues are found a ValueError is raised.
    """
    if isinstance(mimasobject, (bool, int, float, str, type(None))):
        # trivial base types
        return mimasobject
    raise ValueError(f"{mimasobject!r} is not a known Mimas object")


@zocalo_message.register(MimasRecipeInvocation)
def _(mimasobject: MimasRecipeInvocation):
    return {
        "recipes": [mimasobject.recipe],
        "parameters": {"ispyb_dcid": mimasobject.DCID},
    }


@zocalo_message.register(MimasISPyBJobInvocation)
def _(mimasobject: MimasISPyBJobInvocation):
    return {key: zocalo_message(value) for key, value in mimasobject._asdict().items()}


@zocalo_message.register(MimasISPyBSweep)
def _(mimasobject: MimasISPyBSweep):
    return mimasobject._asdict()


@zocalo_message.register(MimasISPyBParameter)
def _(mimasobject: MimasISPyBParameter):
    return mimasobject._asdict()


@zocalo_message.register(MimasISPyBUnitCell)
def _(mimasobject: MimasISPyBUnitCell):
    return tuple(mimasobject._asdict().values())


@zocalo_message.register(list)
def _(list_: list):
    return [zocalo_message(element) for element in list_]


@zocalo_message.register(tuple)
def _(tuple_: tuple):
    return tuple(zocalo_message(element) for element in tuple_)


@functools.singledispatch
def zocalo_command_line(mimasobject):
    """
    Return the command line equivalent to execute a given Mimas* object
    """
    raise ValueError(f"{mimasobject!r} is not a known Mimas object")


@zocalo_command_line.register(MimasRecipeInvocation)
def _(mimasobject: MimasRecipeInvocation):
    return f"zocalo.go -r {mimasobject.recipe} {mimasobject.DCID}"


@zocalo_command_line.register(MimasISPyBJobInvocation)
def _(mimasobject: MimasISPyBJobInvocation):
    if mimasobject.comment:
        comment = f"--comment={mimasobject.comment!r} "
    else:
        comment = ""
    if mimasobject.displayname:
        displayname = f"--display={mimasobject.displayname!r} "
    else:
        displayname = ""
    parameters = " ".join(
        f"--add-param={p.key}:{p.value}" for p in mimasobject.parameters
    )
    sweeps = " ".join(
        f"--add-sweep={s.DCID}:{s.start}:{s.end}" for s in mimasobject.sweeps
    )
    if mimasobject.autostart:
        trigger = "--trigger "
    else:
        trigger = ""
    triggervars = " ".join(
        f"--trigger-variable={tv.key}:{tv.value}" for tv in mimasobject.triggervariables
    )

    return (
        f"ispyb.job --new --dcid={mimasobject.DCID} --source={mimasobject.source} --recipe={mimasobject.recipe} "
        f"{sweeps} {parameters} {displayname}{comment}{trigger}{triggervars}"
    )
