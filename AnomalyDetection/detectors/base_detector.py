from abc import ABC, abstractmethod
from typing import Optional

import numpy as np


# interface for drift detectors
class BaseDetector(ABC):

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def update(self,
               current: np.ndarray,
               reference: np.ndarray) -> Optional[dict]:
        pass