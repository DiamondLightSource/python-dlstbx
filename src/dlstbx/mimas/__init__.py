from __future__ import annotations

import dataclasses
import enum
import functools
import numbers
from typing import Callable, List, Optional, Tuple, Union

import gemmi
import pkg_resources

from dlstbx.mimas.specification import BaseSpecification

MimasDCClass = enum.Enum("MimasDCClass", "GRIDSCAN ROTATION SCREENING UNDEFINED")

MimasDetectorClass = enum.Enum("MimasDetectorClass", "PILATUS EIGER")

MimasEvent = enum.Enum("MimasEvent", "START END")


@dataclasses.dataclass(frozen=True)
class MimasISPyBUnitCell:
    a: float
    b: float
    c: float
    alpha: float
    beta: float
    gamma: float

    @property
    def string(self):
        return f"{self.a},{self.b},{self.c},{self.alpha},{self.beta},{self.gamma}"


@dataclasses.dataclass(frozen=True)
class MimasISPyBSpaceGroup:
    symbol: str

    @property
    def string(self):
        return gemmi.SpaceGroup(self.symbol).hm.replace(" ", "")


@dataclasses.dataclass(frozen=True)
class MimasISPyBAnomalousScatterer:
    symbol: str

    @property
    def string(self):
        return gemmi.Element(self.symbol).name


@dataclasses.dataclass(frozen=True)
class MimasISPyBSweep:
    DCID: int
    start: int
    end: int


@dataclasses.dataclass(frozen=True)
class MimasScenario:
    DCID: int
    dcclass: MimasDCClass
    event: MimasEvent
    beamline: str
    visit: str
    runstatus: str
    spacegroup: Optional[MimasISPyBSpaceGroup] = None
    unitcell: Optional[MimasISPyBUnitCell] = None
    getsweepslistfromsamedcg: Tuple[MimasISPyBSweep, ...] = ()
    preferred_processing: Optional[str] = None
    detectorclass: Optional[MimasDetectorClass] = None
    anomalous_scatterer: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class MimasISPyBParameter:
    key: str
    value: str


@dataclasses.dataclass(frozen=True)
class MimasISPyBTriggerVariable:
    key: str
    value: str


@dataclasses.dataclass(frozen=True)
class MimasISPyBJobInvocation:
    DCID: int
    autostart: bool
    recipe: str
    source: str
    comment: str = ""
    displayname: str = ""
    parameters: Tuple[MimasISPyBParameter, ...] = ()
    sweeps: Tuple[MimasISPyBSweep, ...] = ()
    triggervariables: Tuple[MimasISPyBTriggerVariable, ...] = ()


@dataclasses.dataclass(frozen=True)
class MimasRecipeInvocation:
    DCID: int
    recipe: str


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
    if mimasobject.visit is not None:
        if type(mimasobject.visit) != str:
            raise ValueError(f"{mimasobject!r} has non-string visit")
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
    if mimasobject.spacegroup is not None:
        validate(mimasobject.spacegroup, expectedtype=MimasISPyBSpaceGroup)
    if mimasobject.detectorclass is not None:
        validate(mimasobject.detectorclass, expectedtype=MimasDetectorClass)
    if mimasobject.anomalous_scatterer:
        validate(
            mimasobject.anomalous_scatterer, expectedtype=MimasISPyBAnomalousScatterer
        )


@validate.register(MimasDCClass)  # type: ignore
def _(mimasobject: MimasDCClass, expectedtype=None):
    if expectedtype and not isinstance(mimasobject, expectedtype):
        raise ValueError(f"{mimasobject!r} is not a {expectedtype}")


@validate.register(MimasEvent)  # type: ignore
def _(mimasobject: MimasEvent, expectedtype=None):
    if expectedtype and not isinstance(mimasobject, expectedtype):
        raise ValueError(f"{mimasobject!r} is not a {expectedtype}")


@validate.register(MimasDetectorClass)  # type: ignore
def _(mimasobject: MimasDetectorClass, expectedtype=None):
    if expectedtype and not isinstance(mimasobject, expectedtype):
        raise ValueError(f"{mimasobject!r} is not a {expectedtype}")


@validate.register(MimasRecipeInvocation)  # type: ignore
def _(mimasobject: MimasRecipeInvocation, expectedtype=None):
    if expectedtype and not isinstance(mimasobject, expectedtype):
        raise ValueError(f"{mimasobject!r} is not a {expectedtype}")
    if type(mimasobject.DCID) != int:
        raise ValueError(f"{mimasobject!r} has non-integer DCID")
    if type(mimasobject.recipe) != str:
        raise ValueError(f"{mimasobject!r} has non-string recipe")
    if not mimasobject.recipe:
        raise ValueError(f"{mimasobject!r} has empty recipe string")


@validate.register(MimasISPyBJobInvocation)  # type: ignore
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


@validate.register(MimasISPyBParameter)  # type: ignore
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


@validate.register(MimasISPyBSweep)  # type: ignore
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


@validate.register(MimasISPyBUnitCell)  # type: ignore
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


@validate.register(MimasISPyBSpaceGroup)  # type: ignore
def _(mimasobject: MimasISPyBSpaceGroup, expectedtype=None):
    if expectedtype and not isinstance(mimasobject, expectedtype):
        raise ValueError(f"{mimasobject!r} is not a {expectedtype}")
    gemmi.SpaceGroup(mimasobject.symbol)


@validate.register(MimasISPyBAnomalousScatterer)  # type: ignore
def _(mimasobject: MimasISPyBAnomalousScatterer, expectedtype=None):
    if expectedtype and not isinstance(mimasobject, expectedtype):
        raise ValueError(f"{mimasobject!r} is not a {expectedtype}")
    if (
        not mimasobject.symbol.isdigit() and len(mimasobject.symbol) > 2
    ) or gemmi.Element(mimasobject.symbol).atomic_number == 0:
        raise ValueError(
            f"{mimasobject!r} anomalous_scatterer {mimasobject.symbol} is not a valid element"
        )


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


@zocalo_message.register(MimasRecipeInvocation)  # type: ignore
def _(mimasobject: MimasRecipeInvocation):
    return {
        "recipes": [mimasobject.recipe],
        "parameters": {"ispyb_dcid": mimasobject.DCID},
    }


@zocalo_message.register(MimasISPyBJobInvocation)  # type: ignore
def _(mimasobject: MimasISPyBJobInvocation):
    return dataclasses.asdict(mimasobject)


@zocalo_message.register(MimasISPyBSweep)  # type: ignore
def _(mimasobject: MimasISPyBSweep):
    return dataclasses.asdict(mimasobject)


@zocalo_message.register(MimasISPyBParameter)  # type: ignore
def _(mimasobject: MimasISPyBParameter):
    return dataclasses.asdict(mimasobject)


@zocalo_message.register(MimasISPyBUnitCell)  # type: ignore
def _(mimasobject: MimasISPyBUnitCell):
    return dataclasses.astuple(mimasobject)


@zocalo_message.register(MimasISPyBSpaceGroup)  # type: ignore
def _(mimasobject: MimasISPyBSpaceGroup):
    return mimasobject.string


@zocalo_message.register(list)  # type: ignore
def _(list_: list):
    return [zocalo_message(element) for element in list_]


@zocalo_message.register(tuple)  # type: ignore
def _(tuple_: tuple):
    return tuple(zocalo_message(element) for element in tuple_)


@functools.singledispatch
def zocalo_command_line(mimasobject):
    """
    Return the command line equivalent to execute a given Mimas* object
    """
    raise ValueError(f"{mimasobject!r} is not a known Mimas object")


@zocalo_command_line.register(MimasRecipeInvocation)  # type: ignore
def _(mimasobject: MimasRecipeInvocation):
    return f"zocalo.go -r {mimasobject.recipe} {mimasobject.DCID}"


@zocalo_command_line.register(MimasISPyBJobInvocation)  # type: ignore
def _(mimasobject: MimasISPyBJobInvocation):
    if mimasobject.comment:
        comment = (f"--comment={mimasobject.comment!r}",)
    else:
        comment = ()
    if mimasobject.displayname:
        displayname = (f"--display={mimasobject.displayname!r}",)
    else:
        displayname = ()
    parameters = (f"--add-param={p.key}:{p.value}" for p in mimasobject.parameters)
    sweeps = (f"--add-sweep={s.DCID}:{s.start}:{s.end}" for s in mimasobject.sweeps)
    if mimasobject.autostart:
        trigger = ("--trigger",)
    else:
        trigger = ()
    triggervars = (
        f"--trigger-variable={tv.key}:{tv.value}" for tv in mimasobject.triggervariables
    )

    return " ".join(
        (
            "ispyb.job",
            "--new",
            f"--dcid={mimasobject.DCID}",
            f"--source={mimasobject.source}",
            f"--recipe={mimasobject.recipe}",
            *sweeps,
            *parameters,
            *displayname,
            *comment,
            *trigger,
            *triggervars,
        )
    )


Invocation = Union[MimasISPyBJobInvocation, MimasRecipeInvocation]


def match_specification(specification: BaseSpecification):
    def outer_wrapper(handler: Callable):
        @functools.wraps(handler)
        def inner_wrapper(scenario: MimasScenario) -> List[Invocation]:
            if specification.is_satisfied_by(scenario):
                return handler(scenario)
            return []

        return inner_wrapper

    return outer_wrapper


@functools.lru_cache
def _get_handlers() -> dict[str, Callable]:
    return {
        e.name: e.load()
        for e in pkg_resources.iter_entry_points("zocalo.mimas.handlers")
    }


def handle_scenario(scenario: MimasScenario) -> List[Invocation]:
    tasks: List[Invocation] = []
    for handler in _get_handlers().values():
        tasks.extend(handler(scenario))
    return tasks
