from __future__ import annotations

from typing import Any, Dict, Iterable, List, Type, Union

from detectors.base import Detector
from detectors.bull_flag_detector import BullFlagDetector
from detectors.example_detector import ExamplePriceCrossDetector

REGISTERED_DETECTORS: Dict[str, Type[Detector]] = {
    ExamplePriceCrossDetector.name: ExamplePriceCrossDetector,
    BullFlagDetector.name: BullFlagDetector,
}

DetectorSpec = Union[str, Dict[str, Any]]

def build_detectors(specs: Iterable[DetectorSpec]) -> List[Detector]:
    detectors: List[Detector] = []
    for spec in specs:
        if isinstance(spec, str):
            name = spec
            args: Dict[str, Any] = {}
        else:
            name = spec["name"]
            args = spec.get("args", {})

        cls = REGISTERED_DETECTORS.get(name)
        if not cls:
            raise ValueError(f"Unknown detector {name}")

        detectors.append(cls(**args))
    return detectors
