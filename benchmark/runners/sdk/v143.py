"""pyenvector 1.4.3 adapter — eval_mode=mm32, index_type=ivf_vct.

Implemented and verified against pyenvector 1.4.3. The v1.4.x `ev.init()` /
`ev.Index.insert()` call sites are exercised through EnVectorSDKAdapter
(mcp/adapter/envector_sdk.py); the searchable measurement drives the
ev.Index insert / load / wait_for_insert_stage lifecycle directly.

Implementation reference — the v1.4.3-native runner is preserved on the
`benchmark/envector-latency-v1.4.3` branch:

    git show benchmark/envector-latency-v1.4.3:benchmark/runners/latency_bench_v1.4.3.py

The index operation lifecycle (SPLITTING -> SPLIT_COMPLETED -> MERGING ->
MERGED_SAVED -> SEARCHABLE) is documented at:

    https://docs.envector.io/sdk-user-guide/advanced-user-guide/index-operation-lifecycle

That doc describes the 1.4.x lifecycle — confirmed (by auditing the
installed package) NOT to apply to 1.2.2.

Two version-divergent SDK call sites distinguish this from V122Adapter:
  1. ev.init(...)        — v1.4.x EnVectorSDKAdapter.__init__ additionally
                           takes `secure` and `index_type`; `query_encryption`
                           is a string ("plain"/"cipher"), not a bool.
  2. ev.Index.insert(...) — v1.4.x accepts `await_completion`, `execute_until`,
                           `use_row_insert`, `request_ids`.
Everything else (score, remind, index lifecycle, metadata encryption,
reconnect) is version-agnostic and inherited from SdkAdapter unchanged.
"""

from __future__ import annotations

import json
import time
from typing import Optional

from .base import SdkAdapter, SearchableCtx


class V143Adapter(SdkAdapter):
    sdk_version = "1.4.3"
    eval_mode = "mm32"
    index_type = "ivf_vct"
    # IVF index params, fixed by the sweep plan
    # (benchmark/plans/latency_sweep_plan.md, "Index config"). ivf_vct is NOT
    # in production — there is no live index to mirror; nlist / default_nprobe
    # are evaluation choices. Both are mandatory at create time: omitting
    # nlist yields a malformed IVF index the cluster crashes on at the first
    # insert (the bug create_index was fixed for). default_nprobe sets the
    # crossover N (≈ default_nprobe × 4096) where ivf overtakes a flat scan.
    # nlist=8192 is generous headroom but UNVERIFIED — probe it with
    # create_index_probe.py before a long run (only nlist=256 is known-good).
    index_params = {"index_type": "IVF_VCT", "nlist": 8192, "default_nprobe": 6}

    # ── connection ─────────────────────────────────────────────────────────

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
        """Build an EnVectorSDKAdapter wired for pyenvector 1.4.3.

        Reference — V122Adapter.connect, plus the v1.4.x-only kwargs the
        native runner's `EnVectorClient(...)` call passes through. Construct:

            EnVectorSDKAdapter(
                address=address, key_id=key_id, key_path=key_path,
                eval_mode=self.eval_mode,        # "mm32"
                query_encryption="plain",        # 1.4.x: string, not bool
                access_token=access_token,
                secure=secure,                   # Vault bundle envector_secure
                auto_key_setup=False,
                agent_id=agent_id, agent_dek=agent_dek,
                index_type=self.index_type,      # "ivf_vct"
            )

        Wrap construction in the same 5x retry loop as V122Adapter.connect —
        the first handshake after a fresh process can flake. Assign the
        result to self._sdk.
        """
        from adapter.envector_sdk import EnVectorSDKAdapter

        # EnVectorSDKAdapter calls ev.init() in __init__; the first handshake
        # after a fresh process can flake, so retry the construction.
        last_err: Optional[Exception] = None
        for _attempt in range(5):
            try:
                self._sdk = EnVectorSDKAdapter(
                    address=address,
                    key_id=key_id,
                    key_path=key_path,
                    eval_mode=self.eval_mode,        # "mm32"
                    query_encryption="plain",        # 1.4.x: string, not bool
                    access_token=access_token,
                    secure=secure,                   # Vault bundle envector_secure
                    auto_key_setup=False,
                    agent_id=agent_id,
                    agent_dek=agent_dek,
                    index_type=self.index_type,      # "ivf_vct"
                )
                return
            except Exception as e:  # noqa: BLE001
                last_err = e
                time.sleep(2.0)
        assert last_err is not None
        raise last_err

    # ── data plane ─────────────────────────────────────────────────────────

    def insert(
        self,
        index_name: str,
        vectors: list,
        metadata: list,
        *,
        row_insert: bool = False,
    ) -> None:
        """Insert vectors with metadata. v1.4.x honours `row_insert`.

        Reference — v1.4.3-native `_single_capture_phases` /
        `_multi_capture_phases`, which call insert with `use_row_insert`.
        JSON-serialise the metadata dicts first (as V122Adapter.insert does),
        then call through EnVectorSDKAdapter — v1.4.x `call_insert` accepts
        `use_row_insert`, so pass `row_insert` through. Raise RuntimeError if
        the result dict reports not-ok.

        `await_completion=False, load=False` are passed explicitly: per the
        `measure_insert_to_searchable` docstring, this is the ONLY working
        insert combination on the 1.4.3 SDK today (the `await_completion=True`
        path is bug-pending and the SDK default `load=True` triggers a server
        `ForwardLoadRawShard` RPC that the v1.4.3 cluster build does not
        implement — see benchmark/reports/raw/create_probe_n256_rowinsert_*).
        """
        # JSON-serialise metadata dicts (as V122Adapter.insert does), then
        # call through EnVectorSDKAdapter — v1.4.x call_insert honours
        # use_row_insert.
        meta_strs = [
            json.dumps(m) if isinstance(m, dict) else str(m) for m in metadata
        ]
        res = self.sdk.call_insert(
            index_name=index_name,
            vectors=vectors,
            metadata=meta_strs,
            use_row_insert=row_insert,
            await_completion=False,
            load=False,
        )
        if isinstance(res, dict) and not res.get("ok", True):
            raise RuntimeError(f"insert failed: {res.get('error')}")

    # ── priming (out-of-band batch insert) ────────────────────────────────

    def prime_insert(
        self,
        index_name: str,
        vectors: list,
        metadata: list,
    ) -> None:
        """Batch-insert priming rows with `await_completion=True, load=True`.

        Priming runs outside any measured window, so it waits for the cluster
        to absorb each batch before the next one is submitted — the only
        combination that keeps the 1.4.3 cluster stable across many
        sequential batches. Force `use_row_insert=False`: even when the
        scenario itself runs in single-insert mode, priming uses the batch
        path because that is what the stability test confirmed.
        """
        meta_strs = [
            json.dumps(m) if isinstance(m, dict) else str(m) for m in metadata
        ]
        res = self.sdk.call_insert(
            index_name=index_name,
            vectors=vectors,
            metadata=meta_strs,
            use_row_insert=False,
            await_completion=True,
            load=True,
        )
        if isinstance(res, dict) and not res.get("ok", True):
            raise RuntimeError(f"prime_insert failed: {res.get('error')}")

    # ── searchable measurement ────────────────────────────────────────────

    def searchable_phase_names(self) -> list[str]:
        return ["insert_rpc", "merge_wait", "publish_wait"]

    async def measure_insert_to_searchable(self, ctx: SearchableCtx) -> dict:
        """Measure insert -> SEARCHABLE (done=true), three phases following
        the cluster lifecycle raw -> MERGED_SAVED(6) -> SEARCHABLE(7).

          insert_rpc    index.insert(execute_until="segmentation",
                        await_completion=False, load=False, ...) — split
                        submission; execute_until makes the server auto-queue
                        the async merge so it is in flight by Phase B.
          merge_wait    poll wait_for_index_operations_state(MERGED_SAVED).
          publish_wait  index.load() (safe now — merge done) +
                        poll wait_for_index_operations_state(SEARCHABLE).

        load() ordering: calling load() between insert(load=False) and
        merge_wait triggers a server ForwardLoadRawShard RPC the v1.4.3
        cluster does not implement — reproduced 2026-05-20 in
        benchmark/reports/raw/smoke_sweep_N10_stdout.log. The pre-load
        BEFORE the timed window keeps the index loaded across iterations;
        the in-window load() must come AFTER merge_wait.

        The Index handle is built once outside the timed window —
        ev.Index(name) issues server round-trips that would otherwise leak
        into Phase A latency.

        Mirrors the v1.4.3-native reference runner
        (latency_bench_v1.4.3.py:_searchable_capture_phases). ctx.vault is
        unused on this path (1.4.x uses the server lifecycle, not client
        polling).
        """
        import pyenvector as ev
        from pyenvector.proto_gen.v2.common.index_operation_message_pb2 import (
            IndexOperationState,
        )

        merged_state = IndexOperationState.Value("MERGED_SAVED")    # = 6
        searchable_state = IndexOperationState.Value("SEARCHABLE")  # = 7

        sdk = self.sdk  # EnVectorSDKAdapter — for _with_reconnect

        # Build the Index handle ONCE, before the timed window.
        index = sdk._with_reconnect(lambda: ev.Index(ctx.index_name))

        # Pre-load OUTSIDE the timed window. Idempotent on an index with no
        # raw shards; required so the post-merge load() in Phase C lands on
        # an already-initialized index.
        sdk._with_reconnect(index.load)

        meta_strs = [
            json.dumps(m) if isinstance(m, dict) else str(m)
            for m in ctx.metadata
        ]
        # request_ids is an out-list: the SDK clears it and appends one
        # server-generated request_id per async split RPC.
        request_ids: list[str] = []

        # Phase A — insert_rpc
        t0 = time.perf_counter()
        sdk._with_reconnect(
            lambda: index.insert(
                data=[ctx.vec],
                metadata=meta_strs,
                request_ids=request_ids,
                await_completion=False,
                load=False,
                use_row_insert=(ctx.insert_mode == "single"),
                execute_until="segmentation",
            )
        )
        insert_rpc_ms = (time.perf_counter() - t0) * 1000.0

        # Phase B — merge_wait: poll until each request_id reaches MERGED_SAVED.
        t0 = time.perf_counter()
        sdk._with_reconnect(
            lambda: index.indexer.wait_for_index_operations_state(
                ctx.index_name,
                request_ids,
                target_state=merged_state,
                timeout_s=120.0,
                poll_interval_s=0.5,
            )
        )
        merge_wait_ms = (time.perf_counter() - t0) * 1000.0

        # Phase C — publish_wait: load (safe now — merge done) + SEARCHABLE.
        def _publish() -> None:
            index.load()
            index.indexer.wait_for_index_operations_state(
                ctx.index_name,
                request_ids,
                target_state=searchable_state,
                timeout_s=120.0,
                poll_interval_s=0.5,
            )
        t0 = time.perf_counter()
        sdk._with_reconnect(_publish)
        publish_wait_ms = (time.perf_counter() - t0) * 1000.0

        return {
            "insert_rpc": insert_rpc_ms,
            "merge_wait": merge_wait_ms,
            "publish_wait": publish_wait_ms,
        }
