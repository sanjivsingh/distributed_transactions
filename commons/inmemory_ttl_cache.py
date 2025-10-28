import threading
import time
from abc import ABC
from typing import Callable

class InMemoryCache(ABC):

    def __init__(self, ttl_seconds: int = 3600, loader_func=Callable):
        self._cache: dict[str, dict] = {}
        self._ttl_seconds = ttl_seconds
        self._loader_func = loader_func
        self._locks: dict[str, threading.RLock] = {}
        self._global_lock = threading.RLock()

    def get(self, key: str):
        """
        Get value from cache, loading lazily if not found or expired
        """
        # Fast path - data exists and is fresh
        if self._is_cache_valid(key):
            return self._cache[key]["value"]

        # Slow path - need to fetch data
        # Get or create lock for this specific key
        lock = self._get_or_create_lock(key)

        # Acquire key-specific lock
        with lock:
            # Double-check after acquiring lock
            if self._is_cache_valid(key):
                return self._cache[key]["value"]

            try:
                data = self._loader_func(key)
                # Cache the loaded data
                self._set(key, data)
                return data
            except Exception as e:
                raise

    def set(self, key: str, value):
        """
        Manually set a value in the cache (useful for pre-population)
        """
        lock = self._get_or_create_lock(key)
        with lock:
            self._set(key, value)

    def invalidate(self, key: str):
        """
        Remove a specific key from cache
        """
        lock = self._get_or_create_lock(key)
        with lock:
            if key in self._cache:
                del self._cache[key]

    def _get_or_create_lock(self, key: str) -> threading.RLock:
        """
        Get or create a lock for the given key
        """
        with self._global_lock:
            if key not in self._locks:
                self._locks[key] = threading.RLock()
            return self._locks[key]

    def _is_cache_valid(self, key: str) -> bool:
        """
        Check if the cache entry for the given key is still valid.
        """
        if key in self._cache:
            if self._cache[key]["expires_at"] > time.time():
                return True
            else:
                # Remove expired entry without acquiring additional locks
                del self._cache[key]
        return False

    def _set(self, key: str, value):
        """
        Set a value in the cache without acquiring locks.
        This is used internally for pre-population or bulk loading.
        """
        self._cache[key] = {"value": value, "expires_at": time.time() + self._ttl_seconds}