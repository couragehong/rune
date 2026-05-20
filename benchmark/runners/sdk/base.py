"""SDK-version adapters for the unified latency benchmark.

The latency runner (`latency_bench.py`) is SDK-agnostic. Everything that
differs between pyenvector 1.2.x and 1.4.x lives behind `SdkAdapter`.

Design rationale (decided after auditing all three layers — the runner,
the `EnVectorClient` wrapper, and `EnVectorSDKAdapter`):

  * The real SDK-version difference is narrow. It shows up in exactly two
    SDK calls: `ev.init(...)` and `ev.Index.insert(...)`.
  * score / remind / create_index / drop_index / metadata AES encryption /
    reconnect are version-agnostic — already implemented in
    `EnVectorSDKAdapter` and identical across SDK versions.
  * So a concrete adapter REUSES an `EnVectorSDKAdapter` instance as-is
    (no fork, no production-code edit) and only overrides the calls whose
    signature changed between SDK versions.

Version-agnostic calls are implemented here in the base class. Version-
specific ones (`connect`, `insert`, `measure_insert_to_searchable`,
`searchable_phase_names`) are abstract and implemented by V122Adapter /
V143Adapter.
"""

from __future__ import annotations

import sys
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional


# ── debug logging ──────────────────────────────────────────────────────────
#
# The 1.4.3 envector cluster has been observed to die mid-benchmark. The
# runner already suspects create_index — latency_bench.py _reset_bench_index /
# run_sweep note that "the cluster kills the *second* create_index in a
# process". `_create_index_calls` is a process-wide ordinal so a crash can be
# pinned to an exact create call; create_index / drop_index both log through
# `_dbg`. Output goes to stderr to stay clear of the runner's stdout progress
# and report text — silence with `2>/dev/null` when not debugging.

_create_index_calls = 0


def _dbg(tag: str, msg: str) -> None:
    print(f"[dbg {time.strftime('%H:%M:%S')}] {tag}: {msg}", file=sys.stderr, flush=True)


@dataclass
class SearchableCtx:
    """Inputs the `measure_insert_to_searchable` call needs.

    The runner builds one of these per measurement iteration. It covers only
    the *insert -> searchable* segment; the embed / score / vault phases are
    measured by the runner itself since they do not depend on the SDK version.

    `vault` is used only by the 1.2.2 client-polling path. The 1.4.x
    lifecycle-wait path ignores it.
    """

    index_name: str
    vec: list
    metadata: list          # list[dict], one entry per vector
    vault: Any
    insert_mode: str = "single"


class SdkAdapter(ABC):
    """Base adapter. Subclasses declare the four class attributes below and
    implement the abstract methods."""

    # Set by each concrete subclass as class attributes.
    sdk_version: str = ""       # e.g. "1.2.2"
    eval_mode: str = ""         # "rmp" | "mm32"
    index_type: str = ""        # "flat" | "ivf_vct" — display / report label
    # Exact dict passed to ev.create_index(). IVF (1.4.x) MUST carry
    # nlist / default_nprobe — an IVF index built from index_type alone is
    # malformed, and the cluster faults on the first insert into it. Each
    # subclass supplies its own; see V143Adapter / V122Adapter.
    index_params: dict = {}

    def __init__(self) -> None:
        # The underlying EnVectorSDKAdapter — built by connect().
        self._sdk: Any = None

    # ── connection ─────────────────────────────────────────────────────────

    @abstractmethod
    def connect(
        self,
        *,
        address: str,
        key_id: str,
        key_path: str,
        access_token: Optional[str],
        agent_id: Optional[str],
        agent_dek: Optional[bytes],
        secure: bool = True,
    ) -> None:
        """Build the underlying EnVectorSDKAdapter wired for this SDK version.

        This is the first of the two version-divergent call sites: the
        `eval_mode` / `secure` / `index_type` kwargs of `ev.init(...)` differ
        between 1.2.x and 1.4.x.

        `secure` is the Vault bundle's `envector_secure` flag — a 1.4.x TLS
        toggle. Defaults to True (secure by default): TLS stays on unless the
        bundle explicitly disables it. v1.2.2 has no such parameter (TLS is
        implied when an access_token is supplied) and ignores it.
        """

    @property
    def sdk(self) -> Any:
        """The underlying EnVectorSDKAdapter. Raises if connect() was skipped."""
        if self._sdk is None:
            raise RuntimeError(f"{type(self).__name__}.connect() not called yet")
        return self._sdk

    # ── index lifecycle (version-agnostic) ────────────────────────────────

    def list_index_names(self) -> list[str]:
        import pyenvector as ev

        existing = self.sdk._with_reconnect(ev.get_index_list)
        if hasattr(existing, "indexes"):
            return [idx.index_name for idx in existing.indexes]
        if isinstance(existing, (list, tuple)):
            return [str(idx) for idx in existing]
        return []

    def drop_index(self, index_name: str) -> None:
        import pyenvector as ev

        _dbg("drop_index", f"START index={index_name!r}")
        t0 = time.perf_counter()
        try:
            self.sdk._with_reconnect(lambda: ev.drop_index(index_name))
        except Exception as e:
            _dbg(
                "drop_index",
                f"FAILED after {(time.perf_counter() - t0) * 1000:.0f}ms "
                f"index={index_name!r} {type(e).__name__}: {e}",
            )
            raise
        _dbg(
            "drop_index",
            f"OK after {(time.perf_counter() - t0) * 1000:.0f}ms "
            f"index={index_name!r}",
        )

    def create_index(self, index_name: str, dim: int) -> None:
        """Create a bench index.

        `index_params` is the version-specific knob, supplied per subclass.
        For IVF (1.4.x) it MUST include nlist / default_nprobe: an IVF index
        created from `index_type` alone is malformed, and the cluster faults
        on the first insert into it (`get index info ... INTERNAL`).
        """
        import pyenvector as ev

        # Process-wide ordinal — if the cluster dies on create #2 (the
        # documented failure mode), this number is the first thing to check.
        global _create_index_calls
        _create_index_calls += 1
        ordinal = _create_index_calls
        _dbg(
            "create_index",
            f"#{ordinal} START index={index_name!r} dim={dim} "
            f"index_params={self.index_params} sdk={self.sdk_version}",
        )

        def _do() -> None:
            ev.create_index(
                index_name=index_name,
                dim=dim,
                index_params=self.index_params,
                query_encryption="plain",
                metadata_encryption=False,
                metadata_key=b"",
            )

        t0 = time.perf_counter()
        try:
            self.sdk._with_reconnect(_do)
        except Exception as e:
            _dbg(
                "create_index",
                f"#{ordinal} FAILED after {(time.perf_counter() - t0) * 1000:.0f}ms "
                f"index={index_name!r} {type(e).__name__}: {e}",
            )
            raise
        _dbg(
            "create_index",
            f"#{ordinal} OK after {(time.perf_counter() - t0) * 1000:.0f}ms "
            f"index={index_name!r}",
        )

    def load_index(self, index_name: str) -> None:
        import pyenvector as ev

        self.sdk._with_reconnect(lambda: ev.Index(index_name).load())

    # ── data plane (version-agnostic) ─────────────────────────────────────

    def score(self, index_name: str, vector: list) -> dict:
        """FHE novelty score. Returns {"ok": bool, "encrypted_blobs": [...]}."""
        return self.sdk.call_score(index_name, vector)

    def remind(
        self,
        index_name: str,
        indices: list,
        output_fields: Optional[list[str]] = None,
    ) -> dict:
        """Fetch metadata for the rows Vault returned after decryption."""
        return self.sdk.call_remind(index_name, indices, output_fields)

    # ── data plane (version-specific) ─────────────────────────────────────

    @abstractmethod
    def insert(
        self,
        index_name: str,
        vectors: list,
        metadata: list,
        *,
        row_insert: bool = False,
    ) -> None:
        """Insert vectors with their metadata dicts.

        The second version-divergent call site: 1.4.x exposes a single-row
        insert path (`use_row_insert`) that 1.2.2 does not. `row_insert` is
        honoured on 1.4.x and ignored on 1.2.2.
        """

    # ── searchable measurement (mechanism differs per SDK) ─────────────────

    @abstractmethod
    async def measure_insert_to_searchable(self, ctx: SearchableCtx) -> dict:
        """Measure the insert -> searchable segment.

        Returns {phase_name: elapsed_ms}. The phase names differ per SDK:
          * 1.2.2 — single phase, client-side score polling.
          * 1.4.x — three phases, server lifecycle (insert_rpc/load/wait).
        Because the *mechanism* differs, the phase names are intentionally
        not unified; the comparison report places them side by side with a
        "not directly comparable" caveat.
        """

    @abstractmethod
    def searchable_phase_names(self) -> list[str]:
        """The phase names `measure_insert_to_searchable` produces, in order."""
