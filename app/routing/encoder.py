"""Encoder implementations for semantic routing."""

from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from typing import List

import numpy as np

logger = logging.getLogger(__name__)


class BaseEncoder(ABC):
    """Abstract base class for text encoders."""

    @abstractmethod
    def encode(self, texts: List[str]) -> np.ndarray:
        """Encode a list of texts into embeddings.

        Args:
            texts: List of text strings to encode

        Returns:
            numpy array of shape (len(texts), embedding_dim)
        """
        pass

    @property
    @abstractmethod
    def dimension(self) -> int:
        """Return the embedding dimension."""
        pass


class OpenAIEncoder(BaseEncoder):
    """OpenAI embedding encoder using text-embedding-3-small."""

    def __init__(self, model: str = "text-embedding-3-small"):
        self.model = model
        self._client = None
        self._dimension = 1536  # Default for text-embedding-3-small

    @property
    def client(self):
        """Lazy-load the OpenAI client."""
        if self._client is None:
            try:
                from openai import OpenAI

                self._client = OpenAI()
            except ImportError:
                raise ImportError("openai package is required for OpenAIEncoder")
        return self._client

    def encode(self, texts: List[str]) -> np.ndarray:
        """Encode texts using OpenAI embeddings API."""
        if not texts:
            return np.array([])

        try:
            response = self.client.embeddings.create(
                model=self.model,
                input=texts,
            )
            embeddings = [item.embedding for item in response.data]
            return np.array(embeddings)
        except Exception as e:
            logger.error("OpenAI encoding failed: %s", e)
            raise

    @property
    def dimension(self) -> int:
        return self._dimension


class LocalEncoder(BaseEncoder):
    """Local sentence transformer encoder using all-MiniLM-L6-v2."""

    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        self.model_name = model_name
        self._model = None
        self._dimension = 384  # Default for all-MiniLM-L6-v2

    @property
    def model(self):
        """Lazy-load the sentence transformer model."""
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer

                logger.info("Loading local encoder model: %s", self.model_name)
                self._model = SentenceTransformer(self.model_name)
                self._dimension = self._model.get_sentence_embedding_dimension()
            except ImportError:
                raise ImportError(
                    "sentence-transformers package is required for LocalEncoder"
                )
        return self._model

    def encode(self, texts: List[str]) -> np.ndarray:
        """Encode texts using local sentence transformer."""
        if not texts:
            return np.array([])

        try:
            embeddings = self.model.encode(texts, convert_to_numpy=True)
            return embeddings
        except Exception as e:
            logger.error("Local encoding failed: %s", e)
            raise

    @property
    def dimension(self) -> int:
        # Trigger model load to get accurate dimension
        if self._model is None:
            _ = self.model
        return self._dimension


class CachedEncoder(BaseEncoder):
    """Wrapper that caches embeddings to avoid redundant API calls."""

    def __init__(self, encoder: BaseEncoder, cache_size: int = 1000):
        self._encoder = encoder
        self._cache: dict[str, np.ndarray] = {}
        self._cache_size = cache_size

    def encode(self, texts: List[str]) -> np.ndarray:
        """Encode with caching."""
        if not texts:
            return np.array([])

        # Check cache for each text
        results = []
        uncached_texts = []
        uncached_indices = []

        for i, text in enumerate(texts):
            if text in self._cache:
                results.append((i, self._cache[text]))
            else:
                uncached_texts.append(text)
                uncached_indices.append(i)

        # Encode uncached texts
        if uncached_texts:
            new_embeddings = self._encoder.encode(uncached_texts)

            # Update cache (with size limit)
            for j, text in enumerate(uncached_texts):
                if len(self._cache) >= self._cache_size:
                    # Remove oldest entry
                    self._cache.pop(next(iter(self._cache)))
                self._cache[text] = new_embeddings[j]
                results.append((uncached_indices[j], new_embeddings[j]))

        # Sort by original index and return
        results.sort(key=lambda x: x[0])
        return np.array([r[1] for r in results])

    @property
    def dimension(self) -> int:
        return self._encoder.dimension


def get_encoder(encoder_type: str | None = None) -> BaseEncoder:
    """Get the appropriate encoder based on configuration.

    Args:
        encoder_type: 'openai', 'local', or None (auto-detect from env)

    Returns:
        Configured encoder instance
    """
    if encoder_type is None:
        encoder_type = os.getenv("SEMANTIC_ROUTER_ENCODER", "openai")

    encoder_type = encoder_type.lower()

    if encoder_type == "openai":
        logger.info("Using OpenAI encoder (text-embedding-3-small)")
        return CachedEncoder(OpenAIEncoder())
    elif encoder_type == "local":
        logger.info("Using local encoder (all-MiniLM-L6-v2)")
        return CachedEncoder(LocalEncoder())
    else:
        raise ValueError(f"Unknown encoder type: {encoder_type}")
