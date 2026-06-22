"""
Query cache layer for Athena queries.

Wraps sensorfabric's Needle/athena execQuery with a file-based cache.
Past gestational weeks are cached permanently. Current/recent data
uses a configurable TTL.

Usage:
    from query_cache import CachedNeedle

    needle = CachedNeedle(method="mdh")
    result = needle.execQuery(query, ttl_seconds=1800)

    # For queries about past data that won't change:
    result = needle.execQuery(query, ttl_seconds=None)  # cache forever
"""

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Optional

import pandas as pd
from sensorfabric.needle import Needle
from sensorfabric.athena import athena


CACHE_DIR = Path(".cache/queries")


class CachedNeedle:
    """
    Wrapper around sensorfabric.Needle that caches query results to disk.

    Cache files are stored as parquet in .cache/queries/{hash}.parquet
    with a companion .meta JSON file tracking TTL and timestamp.
    """

    def __init__(self, method: str = "mdh", cache_dir: Optional[Path] = None):
        self._needle = Needle(method=method)
        self._cache_dir = cache_dir or CACHE_DIR
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def execQuery(
        self,
        query: str,
        ttl_seconds: Optional[int] = 1800,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        """
        Execute a query with caching.

        Args:
            query: SQL query string
            ttl_seconds: Cache TTL in seconds. None = cache forever. Default 30 min.
            force_refresh: Ignore cache and re-query.

        Returns:
            pandas DataFrame with query results
        """
        cache_key = self._hash_query(query)
        cache_file = self._cache_dir / f"{cache_key}.parquet"
        meta_file = self._cache_dir / f"{cache_key}.meta"

        if not force_refresh and self._is_cache_valid(cache_file, meta_file, ttl_seconds):
            return pd.read_parquet(cache_file)

        # Execute query
        result = self._needle.execQuery(query)

        # Cache result
        self._save_cache(result, cache_file, meta_file, ttl_seconds)

        return result

    def _hash_query(self, query: str) -> str:
        """Generate a stable hash for the query string."""
        # Normalize whitespace for consistent hashing
        normalized = " ".join(query.split())
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]

    def _is_cache_valid(
        self, cache_file: Path, meta_file: Path, ttl_seconds: Optional[int]
    ) -> bool:
        """Check if a cached result exists and is still valid."""
        if not cache_file.exists() or not meta_file.exists():
            return False

        try:
            with open(meta_file, "r") as f:
                meta = json.load(f)
        except (json.JSONDecodeError, IOError):
            return False

        # If ttl is None in meta, it's cached forever
        cached_ttl = meta.get("ttl_seconds")
        if cached_ttl is None:
            return True

        # Check expiry
        cached_at = meta.get("cached_at", 0)
        elapsed = time.time() - cached_at
        return elapsed < cached_ttl

    def _save_cache(
        self,
        df: pd.DataFrame,
        cache_file: Path,
        meta_file: Path,
        ttl_seconds: Optional[int],
    ):
        """Save query result and metadata to cache."""
        try:
            df.to_parquet(cache_file, index=False)
            meta = {
                "cached_at": time.time(),
                "ttl_seconds": ttl_seconds,
                "rows": len(df),
            }
            with open(meta_file, "w") as f:
                json.dump(meta, f)
        except Exception:
            # Don't fail the query if caching fails
            pass


class CachedAthena:
    """
    Wrapper around sensorfabric.athena that caches query results to disk.

    Same interface as CachedNeedle but for direct AWS Athena connections.
    """

    def __init__(
        self,
        profile_name: Optional[str] = None,
        database: Optional[str] = None,
        s3_location: Optional[str] = None,
        workgroup: Optional[str] = None,
        cache_dir: Optional[Path] = None,
    ):
        self._athena = athena(
            profile_name=profile_name,
            database=database,
            s3_location=s3_location,
            workgroup=workgroup,
            offlineCache=False,
        )
        self._cache_dir = cache_dir or CACHE_DIR
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def execQuery(
        self,
        query: str,
        ttl_seconds: Optional[int] = 1800,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        """
        Execute a query with caching.

        Args:
            query: SQL query string
            ttl_seconds: Cache TTL in seconds. None = cache forever. Default 30 min.
            force_refresh: Ignore cache and re-query.

        Returns:
            pandas DataFrame with query results
        """
        cache_key = self._hash_query(query)
        cache_file = self._cache_dir / f"{cache_key}.parquet"
        meta_file = self._cache_dir / f"{cache_key}.meta"

        if not force_refresh and self._is_cache_valid(cache_file, meta_file, ttl_seconds):
            return pd.read_parquet(cache_file)

        # Execute query
        result = self._athena.execQuery(query)

        # Cache result
        self._save_cache(result, cache_file, meta_file, ttl_seconds)

        return result

    def _hash_query(self, query: str) -> str:
        normalized = " ".join(query.split())
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]

    def _is_cache_valid(
        self, cache_file: Path, meta_file: Path, ttl_seconds: Optional[int]
    ) -> bool:
        if not cache_file.exists() or not meta_file.exists():
            return False
        try:
            with open(meta_file, "r") as f:
                meta = json.load(f)
        except (json.JSONDecodeError, IOError):
            return False
        cached_ttl = meta.get("ttl_seconds")
        if cached_ttl is None:
            return True
        cached_at = meta.get("cached_at", 0)
        elapsed = time.time() - cached_at
        return elapsed < cached_ttl

    def _save_cache(self, df, cache_file, meta_file, ttl_seconds):
        try:
            df.to_parquet(cache_file, index=False)
            meta = {
                "cached_at": time.time(),
                "ttl_seconds": ttl_seconds,
                "rows": len(df),
            }
            with open(meta_file, "w") as f:
                json.dump(meta, f)
        except Exception:
            pass
