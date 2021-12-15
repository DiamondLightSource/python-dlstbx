from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

import dlstbx.mimas


class BaseSpecification:
    @abstractmethod
    def is_satisfied_by(self, candidate: Any) -> bool:
        raise NotImplementedError()

    def __and__(self, other: BaseSpecification) -> AndSpecification:
        return AndSpecification(self, other)

    def __or__(self, other: BaseSpecification) -> OrSpecification:
        return OrSpecification(self, other)

    def __invert__(self) -> InvertSpecification:
        return InvertSpecification(self)


@dataclass(frozen=True)
class AndSpecification(BaseSpecification):
    first: BaseSpecification
    second: BaseSpecification

    def is_satisfied_by(self, candidate: Any) -> bool:
        return self.first.is_satisfied_by(candidate) and self.second.is_satisfied_by(
            candidate
        )


@dataclass(frozen=True)
class OrSpecification(BaseSpecification):
    first: BaseSpecification
    second: BaseSpecification

    def is_satisfied_by(self, candidate: Any) -> bool:
        return self.first.is_satisfied_by(candidate) or self.second.is_satisfied_by(
            candidate
        )


@dataclass(frozen=True)
class InvertSpecification(BaseSpecification):
    subject: BaseSpecification

    def is_satisfied_by(self, candidate: Any) -> bool:
        return not self.subject.is_satisfied_by(candidate)


class ScenarioSpecification(BaseSpecification):
    @abstractmethod
    def is_satisfied_by(self, candidate: dlstbx.mimas.MimasScenario) -> bool:
        raise NotImplementedError()


@dataclass(frozen=True)
class BeamlineSpecification(ScenarioSpecification):
    beamline: Optional[str] = None
    beamlines: Optional[set[str]] = None

    def is_satisfied_by(self, candidate: dlstbx.mimas.MimasScenario) -> bool:
        return (
            (self.beamline and candidate.beamline == self.beamline)
            or (self.beamlines and candidate.beamline in self.beamlines)
            or False
        )


@dataclass(frozen=True)
class EventSpecification(ScenarioSpecification):
    event: dlstbx.mimas.MimasEvent

    def is_satisfied_by(self, candidate: dlstbx.mimas.MimasScenario) -> bool:
        return candidate.event == self.event


@dataclass(frozen=True)
class DCClassSpecification(ScenarioSpecification):
    dcclass: dlstbx.mimas.MimasDCClass

    def is_satisfied_by(self, candidate: dlstbx.mimas.MimasScenario) -> bool:
        return candidate.dcclass == self.dcclass


@dataclass(frozen=True)
class DetectorClassSpecification(ScenarioSpecification):
    detectorclass: dlstbx.mimas.MimasDetectorClass

    def is_satisfied_by(self, candidate: dlstbx.mimas.MimasScenario) -> bool:
        return candidate.detectorclass == self.detectorclass


from typing import Set


@dataclass(frozen=True)
class VisitSpecification(ScenarioSpecification):
    visits: Set[str]

    def is_satisfied_by(self, candidate: dlstbx.mimas.MimasScenario) -> bool:
        return (
            candidate.visit and candidate.visit.startswith(tuple(self.visits))
        ) or False


# class HasSpaceGroupSpecification(ScenarioSpecification):
#     def is_satisfied_by(self, candidate: dlstbx.mimas.MimasScenario) -> bool:
#         return candidate.spacegroup is not None
