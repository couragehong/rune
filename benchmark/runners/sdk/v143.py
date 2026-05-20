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

    # ── searchable measurement ────────────────────────────────────────────

    def searchable_phase_names(self) -> list[str]:
        # Fixed at three phases — see measure_insert_to_searchable. This is
        # not a stylistic choice: the current SDK only accepts the
        # await_completion=False + load=False insert combination (bug fix
        # pending), which forces the insert_rpc / load_index / wait_searchable
        # decomposition.
        return ["insert_rpc", "load_index", "wait_searchable"]

    async def measure_insert_to_searchable(self, ctx: SearchableCtx) -> dict:
        """Measure insert -> SEARCHABLE latency, decomposed into three phases.

        SDK lifecycle:
            SPLITTING -> SPLIT_COMPLETED -> MERGING -> MERGED_SAVED -> SEARCHABLE

        CURRENT SDK CONSTRAINT (bug fix pending): the lifecycle docs describe
        an insert(await_completion=True) path that blocks to MERGED_SAVED, but
        that path is currently broken. The ONLY working insert combination
        today is await_completion=False + load=False. The three-phase
        decomposition below is therefore not a stylistic choice — it is the
        only combination that runs, and it is exactly what the v1.4.3-native
        runner already does (which is *why* the native runner diverges from
        the docs).

        Three phases — each wrapped in EnVectorSDKAdapter._with_reconnect:

          insert_rpc:   index.insert(data=[ctx.vec], metadata=[...],
                            await_completion=False, load=False,
                            use_row_insert=(ctx.insert_mode == "single"),
                            request_ids=request_ids)   # out-list, SDK-filled
          load_index:   index.load()
          wait_searchable: index.wait_for_inserts_searchable(request_ids)

        `index` is ONE Index handle built BEFORE the timed window:
            index = ev.Index(ctx.index_name)   # build once, outside timing
        Do NOT call ev.Index(name) inside a phase's timed block. Its
        constructor issues server round-trips (get_index_list /
        get_index_info), and that construction cost would leak into the
        measured phase latency. The v1.4.3-native runner rebuilds ev.Index()
        inside every phase — that is a measurement-accuracy flaw; reuse one
        handle here instead of copying it.

        Still to confirm on the 1.4.3 environment:
          - wait method name + receiver: the native runner used
            index.wait_for_inserts_searchable(request_ids); the docs say
            index.indexer.wait_for_insert_searchable(index_name, request_ids).
          - execute_until is MOOT while await_completion=False is forced (it
            only takes effect with await_completion=True). Revisit only if the
            bug is fixed and the documented 2-phase path becomes usable.

        IMPORTANT: this measurement depends on the SDK bug state. Record the
        pyenvector build / bug-fix status in the report env, so a later
        re-measurement (post-fix, possibly via the documented await_completion
        =True path) is not silently compared against these numbers.

        Note: v122 measures this as a single phase (client polling), so the
        v122-vs-v143 comparison is on TOTAL insert->searchable time, not
        per-phase.

        ctx.vault is unused on this path (1.4.x uses the server lifecycle, not
        client polling). Return {phase_name: ms}; keys must match
        searchable_phase_names().
        """
        import pyenvector as ev

        sdk = self.sdk  # EnVectorSDKAdapter — for _with_reconnect

        # Build the Index handle ONCE, before the timed window. ev.Index(name)
        # issues server round-trips (get_index_list / get_index_info); building
        # it inside a phase would leak that cost into the measured latency.
        index = sdk._with_reconnect(lambda: ev.Index(ctx.index_name))

        meta_strs = [
            json.dumps(m) if isinstance(m, dict) else str(m)
            for m in ctx.metadata
        ]
        # request_ids is an out-list: the SDK clears it and appends one
        # server-generated split request_id per async split RPC.
        request_ids: list[str] = []

        # phase 1 — insert_rpc: async submit (await_completion=False, load=False)
        t0 = time.perf_counter()
        sdk._with_reconnect(
            lambda: index.insert(
                data=[ctx.vec],
                metadata=meta_strs,
                request_ids=request_ids,
                await_completion=False,
                load=False,
                use_row_insert=(ctx.insert_mode == "single"),
            )
        )
        insert_rpc_ms = (time.perf_counter() - t0) * 1000.0

        # phase 2 — load_index: index.load() (no-op when already loaded)
        t0 = time.perf_counter()
        sdk._with_reconnect(index.load)
        load_index_ms = (time.perf_counter() - t0) * 1000.0

        # phase 3 — wait_searchable: block until the captured request_ids reach
        # the segmentation stage (MERGED_SAVED) — i.e. become searchable.
        t0 = time.perf_counter()
        sdk._with_reconnect(
            lambda: index.wait_for_insert_stage(
                request_ids=request_ids,
                target_stage="segmentation",
                timeout_s=60.0,
                poll_interval_s=0.5,
            )
        )
        wait_searchable_ms = (time.perf_counter() - t0) * 1000.0

        return {
            "insert_rpc": insert_rpc_ms,
            "load_index": load_index_ms,
            "wait_searchable": wait_searchable_ms,
        }
