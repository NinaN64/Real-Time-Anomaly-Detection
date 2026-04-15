import logging
from collections import deque

import numpy as np

import config

log = logging.getLogger("embedding_store")


class EmbeddingStore:

    def __init__(self, max_size: int = config.EMBEDDING_STORE_SIZE):
        self.max_size = max_size
        self._buffer: deque = deque(maxlen=max_size)
        self.total_seen = 0

    def add(self, embeddings: np.ndarray) -> None:
        if embeddings.ndim != 2 or embeddings.shape[0] == 0:
            return

        for vec in embeddings:
            self._buffer.append(vec)

        self.total_seen += len(embeddings)
        log.debug("Store: %d/%d embeddings | total seen: %d",
                  len(self._buffer), self.max_size, self.total_seen)

    def get_reference(self) -> np.ndarray:
        if not self._buffer:
            return np.empty((0,))
        return np.stack(list(self._buffer))

    def is_ready(self) -> bool:
        return self.total_seen >= config.MIN_EMBEDDINGS_BEFORE_DETECTION

    def size(self) -> int:
        return len(self._buffer)