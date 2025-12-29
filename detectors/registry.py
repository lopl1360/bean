from __future__ import annotations

from typing import Dict, Iterable, List, Type

from detectors.base import Detector
from detectors.example_detector import ExamplePriceCrossDetector

REGISTERED_DETECTORS: Dict[str, Type[Detector]] = {
    ExamplePriceCrossDetector.name: ExamplePriceCrossDetector,
}


def build_detectors(names: Iterable[str], **kwargs) -> List[Detector]:
    detectors: List[Detector] = []
    for name in names:
        cls = REGISTERED_DETECTORS.get(name)
        if not cls:
            raise ValueError(f"Unknown detector {name}")
        detectors.append(cls(**kwargs))
    return detectors
