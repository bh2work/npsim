#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Clean NAND-internal simulator (single file)
-------------------------------------------
요구사항(핵심 불변식):
- 스테이지: CA(커맨드 이슈) → ARRAY(tR) → COM(0/1 단계 합 또는 파이프라인) → IO
- INT(내부 연산):
  * 각 벡터: ARRAY → COM → IO "직렬" (벡터 내부에 COM↔IO 오버랩 없음)
  * plane 게이트:
      depth == 0  -> 같은 plane의 다음 tR 시작은 "IO 종료"까지 대기 (EXT와 동등)
      depth >= 1  -> 같은 plane의 tR은 IO와 무관, "COM 종료" 기반 cap = 1+depth 슬롯으로 게이팅
- EXT(외부 비교군):
  * 각 벡터: ARRAY → IO "직렬" (COM 없음)
  * plane 게이트: IO 종료 기반, cap = 1 + depth  (depth=0이면 cap=1)
- 그룹/COM 스케줄러:
  * 그룹(planes_per_group) 단위로 COM 유닛 공유
  * FCFS(ready_t=tR_end 가장 이른 벡터 우선) + work-conserving (일감 있으면 유닛 놀지 않음)
  * 파이프라인 옵션: com_parallel=="on" and depth>=2 → COM0 → COM1 순차 파이프라인
- CA:
  * ca_issue_width 레인, t_cmd_gap_us 간격
  * JIT: tR가 시작 가능한 시각에 맞춰 CA가 끝나도록 정렬 (불필요한 선행 발행 금지)
- Access/배치:
  * access_mode: seq | random (seq는 VPP 주기로 hit)
  * vector_dist_mode: spread | batch
    - spread: 전역 벡터 idx % K → plane 분배
    - batch: T/K개씩 plane 0..K-1에 블록 배치
- IO:
  * io_units_total, io_gbps; out_bytes_per_vector (INT/EXT 공정 비교시 vector_bytes로 설정 권장)
- 메트릭/계측:
  * avg/p50/p95 query latency, util ARRAY/COM/IO, per-plane/COM-unit 평균 가동률,
    IO/COM 대기시간·큐 길이, plane별 최대 outstanding 등
- 어서션(스모크):
  * depth==0 에서 "같은 plane에서 IO 미종료 상태로 tR 시작" 금지
  * plane 동시 outstanding ≤ cap
  * COM work-conserving, FCFS 위반 금지
"""

import argparse, sys, csv, math, statistics, heapq, time
from typing import List, Tuple, Optional, Dict, Any

# ---------------------- Utilities ----------------------
def gbps_to_Bps(gbps: float) -> float:
    return max(0.0, gbps) * 1e9 / 8.0

def percentile(xs: List[float], q: float) -> float:
    if not xs: return 0.0
    xs2 = sorted(xs)
    k = max(0, min(len(xs2)-1, int((len(xs2)-1)*q)))
    return xs2[k]

def union_duration(iv: List[Tuple[float,float]]) -> float:
    if not iv: return 0.0
    ivs = sorted(iv)
    cur_s, cur_e = ivs[0]
    total = 0.0
    for s,e in ivs[1:]:
        if s > cur_e:
            total += (cur_e - cur_s)
            cur_s, cur_e = s,e
        else:
            if e > cur_e: cur_e = e
    total += (cur_e - cur_s)
    return total

# ---------------------- Params ----------------------
class Params:
    def __init__(self, args: Dict[str, Any]):
        self.mode_list = args["mode"]
        self.total_planes = int(args["total_planes"])
        self.planes_per_group = int(args["planes_per_group"])
        self.n_queries = int(args["n_queries"])
        self.query_total_vecs = int(args["query_total_vecs"])
        self.page_bytes = int(args["page_bytes"])
        self.vector_bytes = int(args["vector_bytes"])
        self.tR_us = float(args["tR_us"])
        self.tR_hit_us = float(args["tR_hit_us"])
        self.com0_us_per_vec = float(args["com0_us_per_vec"])
        self.com1_us_per_vec = float(args["com1_us_per_vec"])
        self.com_parallel = args["com_parallel"]
        self.io_gbps = float(args["io_gbps"])
        self.io_units_total = int(args["io_units_total"])
        self.out_bytes_per_vector = int(args["out_bytes_per_vector"])
        self.vector_dist_mode_list = args["vector_dist_mode"]
        self.access_mode_list = args["access_mode"]
        self.tCA_us = float(args["tCA_us"])
        self.ca_issue_width = int(args["ca_issue_width"])
        self.t_cmd_gap_us = float(args["t_cmd_gap_us"])
        self.cache_depth_per_plane = int(args["cache_depth_per_plane"])
        self.ca_can_overlap_io = args["ca_can_overlap_io"]

    def clone_with(self, **kw):
        d = self.__dict__.copy()
        d.update(kw)
        p = object.__new__(Params)
        p.__dict__.update(d)
        return p

# ---------------------- Result ----------------------
class Result:
    def __init__(self):
        # latency
        self.avg_query_latency_us = 0.0
        self.p50_query_latency_us = 0.0
        self.p95_query_latency_us = 0.0
        self.ext_avg_query_latency_us = 0.0
        self.ext_p50_query_latency_us = 0.0
        self.ext_p95_query_latency_us = 0.0
        self.rel_int_over_ext_latency = 0.0
        # util/throughput
        self.util_ARRAY = 0.0
        self.util_COM = 0.0
        self.util_IO = 0.0
        self.array_avg_concurrency = 0.0
        self.array_util_avg_per_plane_pct = 0.0
        self.com_util_avg_per_unit_pct = 0.0
        self.throughput_GBps_io = 0.0
        self.vectors_per_s = 0.0
        self.pages_per_s = 0.0
        # queues/waits diagnostics
        self.io_wait_us_avg = 0.0
        self.io_wait_us_p95 = 0.0
        self.io_queue_len_avg = 0.0
        self.io_queue_len_max = 0
        self.io_queue_nonempty_frac = 0.0
        self.com_wait_us_avg = 0.0
        self.com_wait_us_p95 = 0.0
        self.com_queue_len_avg = 0.0
        self.com_queue_len_max = 0
        self.com_queue_nonempty_frac = 0.0
        self.array_throttled_by_cap_frac = 0.0
        # assertions aids
        self.plane_max_outstanding = 0

# ---------------------- Core helpers ----------------------
def _VPP(p: Params) -> int:
    return max(1, p.page_bytes // p.vector_bytes)

def _targets_per_plane(p: Params, K: int, T: int) -> List[int]:
    if p.vector_dist_mode == "batch":
        n = T // K
        return [n]*K
    base, rem = divmod(T, K)
    return [base + (1 if i < rem else 0) for i in range(K)]

def _vector_plane_index(p: Params, K: int, global_vec_idx: int, T: int) -> int:
    if p.vector_dist_mode == "spread":
        return global_vec_idx % K
    # batch
    n = max(1, T // K)
    plane = (global_vec_idx // n)
    return min(plane, K-1)

def _tc_int(p: Params):
    tCA = max(0.0, p.tCA_us) * 1e-6
    tR_miss = max(0.0, p.tR_us) * 1e-6
    tR_hit  = max(0.0, p.tR_hit_us) * 1e-6
    tC0 = max(0.0, p.com0_us_per_vec) * 1e-6
    tC1 = max(0.0, p.com1_us_per_vec) * 1e-6
    tIO = (p.out_bytes_per_vector / max(1e-15, gbps_to_Bps(p.io_gbps)))
    return tCA, tR_miss, tR_hit, tC0, tC1, tIO

def _tc_ext(p: Params):
    tCA = max(0.0, p.tCA_us) * 1e-6
    tR_miss = max(0.0, p.tR_us) * 1e-6
    tR_hit  = max(0.0, p.tR_hit_us) * 1e-6
    tIO = (p.vector_bytes / max(1e-15, gbps_to_Bps(p.io_gbps)))
    return tCA, tR_miss, tR_hit, tIO

# ---------------------- Engine: External ----------------------
def simulate_external(p: Params) -> Tuple[Result, Dict[str, float]]:
    K = p.total_planes
    VPP = _VPP(p)
    tCA, tR_miss, tR_hit, tIO = _tc_ext(p)

    CA = [(0.0, j) for j in range(p.ca_issue_width)]
    heapq.heapify(CA)
    gap = max(0.0, p.t_cmd_gap_us) * 1e-6
    ca_gap_next = 0.0

    ARR_free = [0.0]*K
    BUF_IO = [[] for _ in range(K)]   # IO-end heap
    cap = 1 + max(0, p.cache_depth_per_plane)

    IO = [(0.0, u) for u in range(p.io_units_total)]
    heapq.heapify(IO)

    arr_iv: List[Tuple[float,float]] = []
    io_iv:  List[Tuple[float,float]] = []

    q_lat: List[float] = []

    # diag
    io_q_len_area = 0.0; io_q_max = 0; io_q_nonempty_time = 0.0
    io_q_last_t = 0.0
    io_waits: List[float] = []
    plane_max_out = 0

    sim_clock_hint = 0.0

    for _ in range(p.n_queries):
        T = p.query_total_vecs
        tgt = _targets_per_plane(p, K, T)
        remain = tgt[:]
        per_plane_vec_idx = [0]*K
        global_vec_idx = 0

        q_s: Optional[float] = None
        last_io_e: Optional[float] = None

        while any(r>0 for r in remain):
            pid = _vector_plane_index(p, K, global_vec_idx, T)
            if remain[pid] <= 0:
                for off in range(1, K+1):
                    pid2 = (pid + off) % K
                    if remain[pid2] > 0:
                        pid = pid2; break
                else: break

            arr_ft = ARR_free[pid]
            is_page_start = (p.access_mode=="random") or ((per_plane_vec_idx[pid] % VPP) == 0)
            tR = tR_miss if is_page_start else tR_hit

            # plane IO-gate with cap (depth≥1 → cap>1)
            buf = BUF_IO[pid]
            while buf and buf[0] <= arr_ft: heapq.heappop(buf)
            need_start = arr_ft if len(buf) < cap else max(arr_ft, buf[0])

            ft, lane = heapq.heappop(CA)
            io_barrier = max(t for t,_ in IO) if p.ca_can_overlap_io=="off" else 0.0
            ca_s = max(ft, ca_gap_next, io_barrier, need_start - tCA)
            ca_e = ca_s + tCA
            heapq.heappush(CA, (ca_e, lane))
            ca_gap_next = ca_e + gap

            while buf and buf[0] <= ca_e: heapq.heappop(buf)
            t_candidate = max(arr_ft, ca_e)
            if len(buf) >= cap:
                t_candidate = max(t_candidate, buf[0])  # wait IO gate

            tr_s = t_candidate; tr_e = tr_s + tR
            ARR_free[pid] = tr_e
            arr_iv.append((tr_s, tr_e))
            if q_s is None: q_s = tr_s

            # IO serial after tR
            io_ft, iu = heapq.heappop(IO)
            io_s = max(tr_e, io_ft); io_e = io_s + tIO
            heapq.heappush(IO, (io_e, iu))
            if tIO > 0: io_iv.append((io_s, io_e))
            heapq.heappush(buf, io_e)
            last_io_e = io_e if (last_io_e is None or io_e > last_io_e) else last_io_e

            # diag: IO queue length sampling at io_s (coarse)
            now = io_s
            qlen = sum(len(b) for b in BUF_IO)
            io_q_len_area += qlen * max(0.0, now - io_q_last_t)
            if qlen > 0: io_q_nonempty_time += max(0.0, now - io_q_last_t)
            io_q_last_t = now
            io_waits.append(max(0.0, io_s - tr_e))

            plane_max_out = max(plane_max_out, len(buf))

            per_plane_vec_idx[pid] += 1
            remain[pid] -= 1
            global_vec_idx += 1
            sim_clock_hint = max(sim_clock_hint, tr_e)

        if q_s is not None and last_io_e is not None:
            q_lat.append((last_io_e - q_s)*1e6)

    # metrics
    avg_q = statistics.mean(q_lat) if q_lat else 0.0
    p50_q = statistics.median(q_lat) if q_lat else 0.0
    p95_q = percentile(q_lat, 0.95)

    last_arr = max(ARR_free) if ARR_free else 0.0
    last_io = max(t for t,_ in IO) if IO else 0.0
    util_ARRAY = union_duration(arr_iv) / max(1e-15, last_arr)
    util_IO = union_duration(io_iv) / max(1e-15, last_io)
    makespan = max(last_io, last_arr)

    total_vecs = p.query_total_vecs * p.n_queries
    throughput_GBps_io = ((p.vector_bytes * total_vecs) / max(1e-15, makespan)) / 1e9
    vectors_per_s = total_vecs / max(1e-15, makespan)
    pages_per_s = (len(arr_iv) / max(1e-15, makespan))

    res = Result()
    res.avg_query_latency_us = avg_q
    res.p50_query_latency_us = p50_q
    res.p95_query_latency_us = p95_q
    res.util_ARRAY = util_ARRAY
    res.util_IO = util_IO
    res.util_COM = 0.0
    res.throughput_GBps_io = throughput_GBps_io
    res.vectors_per_s = vectors_per_s
    res.pages_per_s = pages_per_s
    res.array_avg_concurrency = 0.0  # EXT에선 생략
    res.array_util_avg_per_plane_pct = (util_ARRAY * K / max(1, K)) * 100.0
    res.com_util_avg_per_unit_pct = 0.0
    # IO diag
    res.io_wait_us_avg = statistics.mean(io_waits)*1e6 if io_waits else 0.0
    res.io_wait_us_p95 = percentile([w*1e6 for w in io_waits], 0.95) if io_waits else 0.0
    res.io_queue_len_avg = (io_q_len_area / max(1e-15, io_q_last_t))
    res.io_queue_len_max = io_q_max
    res.io_queue_nonempty_frac = (io_q_nonempty_time / max(1e-15, io_q_last_t))
    res.array_throttled_by_cap_frac = 0.0
    res.plane_max_outstanding = plane_max_out

    # assertions (EXT)
    assert res.plane_max_outstanding <= (1 + p.cache_depth_per_plane) + 1e-9, \
        "EXT: plane outstanding violated cap"

    return res, {"last_io": last_io, "last_arr": last_arr, "makespan": makespan}

# ---------------------- Engine: Internal ----------------------
def simulate_internal(p: Params) -> Tuple[Result, Dict[str, float]]:
    K = p.total_planes
    G = p.planes_per_group
    NG = max(1, K // max(1, G))

    VPP = _VPP(p)
    tCA, tR_miss, tR_hit, tC0, tC1, tIO = _tc_int(p)

    depth = p.cache_depth_per_plane
    gate_by_io = (depth == 0)    # depth==0 → plane IO gate, depth>=1 → COM gate(cap=1+depth)
    cap = 1 + depth if depth >= 1 else 1

    # CA lanes
    CA = [(0.0, j) for j in range(p.ca_issue_width)]
    heapq.heapify(CA)
    gap = max(0.0, p.t_cmd_gap_us) * 1e-6
    ca_gap_next = 0.0

    ARR_free = [0.0]*K
    if gate_by_io:
        BUF_IO = [[] for _ in range(K)]
    else:
        occ_cnt = [0]*K
        release_heap: List[List[float]] = [[] for _ in range(K)]
    per_plane_vec_idx = [0]*K

    # COM banks
    pipelined = (p.com_parallel == "on") and (depth >= 2)
    if pipelined:
        COM0 = [[(0.0, 0)] for _ in range(NG)]
        COM1 = [[(0.0, 0)] for _ in range(NG)]
        com0_iv_all: List[Tuple[float,float]] = []
        com1_iv_all: List[Tuple[float,float]] = []
        com_iv_all: List[Tuple[float,float]] = []
    else:
        COM = [[(0.0, 0)] for _ in range(NG)]
        com_iv_all: List[Tuple[float,float]] = []
        com0_iv_all = []; com1_iv_all = []

    # Ready min-heap (group-level FCFS)
    ready_heap: List[List[Tuple[float,int,int]]] = [[] for _ in range(NG)]
    seq = 0

    # IO
    IO = [(0.0, u) for u in range(p.io_units_total)]
    heapq.heapify(IO)

    # intervals
    arr_iv: List[Tuple[float,float]] = []
    io_iv:  List[Tuple[float,float]] = []

    q_lat: List[float] = []

    # diag
    com_waits: List[float] = []
    io_waits: List[float] = []
    com_q_len_area = [0.0]*NG; com_q_last_t = [0.0]*NG; com_q_nonempty_time=[0.0]*NG
    com_q_max = [0]*NG
    io_q_len_area = 0.0; io_q_last_t = 0.0; io_q_nonempty_time = 0.0
    plane_max_out = 0
    cap_block_time = 0.0

    def com_schedule(now_hint: float):
        # Work-conserving & FCFS
        advanced = True
        while advanced:
            advanced = False
            for g in range(NG):
                # pick earliest ready in group
                if not ready_heap[g]:
                    continue
                rt, sqn, pid = ready_heap[g][0]
                # pick unit
                if pipelined:
                    ft0, u0 = COM0[g][0]
                    start0 = max(rt, ft0)
                    heapq.heapreplace(COM0[g], (start0 + tC0, u0))
                    com0_iv_all.append((start0, start0 + tC0))

                    ft1, u1 = COM1[g][0]
                    start1 = max(start0 + tC0, ft1)
                    heapq.heapreplace(COM1[g], (start1 + tC1, u1))
                    com1_iv_all.append((start1, start1 + tC1))
                    com_end = start1 + tC1
                else:
                    ft, u = COM[g][0]
                    start = max(rt, ft)
                    heapq.heapreplace(COM[g], (start + (tC0 + tC1), u))
                    com_iv_all.append((start, start + (tC0 + tC1)))
                    com_end = start + (tC0 + tC1)

                # pop ready
                heapq.heappop(ready_heap[g])

                # release plane by COM end when depth>=1
                if not gate_by_io:
                    heapq.heappush(release_heap[pid], com_end)

                # IO serial after COM
                io_ft, iu = heapq.heappop(IO)
                io_s = max(com_end, io_ft); io_e = io_s + tIO
                heapq.heappush(IO, (io_e, iu))
                if tIO > 0: io_iv.append((io_s, io_e))
                if gate_by_io:
                    heapq.heappush(BUF_IO[pid], io_e)

                # diag
                io_waits.append(max(0.0, io_s - com_end))
                advanced = True

                # com queue diag update at this scheduling instant
                now = start if not pipelined else start0
                qlen = len(ready_heap[g])
                com_q_len_area[g] += qlen * max(0.0, now - com_q_last_t[g])
                if qlen > com_q_max[g]: com_q_max[g] = qlen
                if qlen > 0: com_q_nonempty_time[g] += max(0.0, now - com_q_last_t[g])
                com_q_last_t[g] = now

    for _ in range(p.n_queries):
        T = p.query_total_vecs
        tgt = _targets_per_plane(p, K, T)
        remain = tgt[:]
        global_vec_idx = 0

        q_s: Optional[float] = None
        last_io_e: Optional[float] = None

        while any(r>0 for r in remain):
            pid = _vector_plane_index(p, K, global_vec_idx, T)
            if remain[pid] <= 0:
                for off in range(1, K+1):
                    pid2 = (pid + off) % K
                    if remain[pid2] > 0: pid = pid2; break
                else: break

            arr_ft = ARR_free[pid]
            is_page_start = (p.access_mode=="random") or ((per_plane_vec_idx[pid] % VPP) == 0)
            tR = tR_miss if is_page_start else tR_hit

            # gate pre-calc
            need_start = arr_ft
            if gate_by_io:
                buf = BUF_IO[pid]
                while buf and buf[0] <= need_start: heapq.heappop(buf)
                if len(buf) >= 1:
                    cap_block_time += max(0.0, buf[0] - need_start)
                    need_start = max(need_start, buf[0])
            else:
                rel = release_heap[pid]
                while rel and rel[0] <= need_start:
                    heapq.heappop(rel);  # 실제 감소는 아래에서
                if 'occ_cnt' in locals() and occ_cnt[pid] >= cap and rel:
                    cap_block_time += max(0.0, rel[0] - need_start)
                    need_start = max(need_start, rel[0])

            # CA JIT
            ft, lane = heapq.heappop(CA)
            io_barrier = max(t for t,_ in IO) if p.ca_can_overlap_io=="off" else 0.0
            ca_s = max(ft, ca_gap_next, io_barrier, need_start - tCA)
            ca_e = ca_s + tCA
            heapq.heappush(CA, (ca_e, lane))
            ca_gap_next = ca_e + gap

            # final tR start (respect gate at ca_e horizon)
            t_candidate = max(arr_ft, ca_e)
            if gate_by_io:
                buf = BUF_IO[pid]
                while buf and buf[0] <= t_candidate: heapq.heappop(buf)
                if len(buf) >= 1:
                    cap_block_time += max(0.0, buf[0] - t_candidate)
                    t_candidate = max(t_candidate, buf[0])
            else:
                rel = release_heap[pid]
                while rel and rel[0] <= t_candidate:
                    heapq.heappop(rel); 
                    if 'occ_cnt' in locals(): occ_cnt[pid] = max(0, occ_cnt[pid]-1)
                if 'occ_cnt' in locals() and occ_cnt[pid] >= cap and rel:
                    cap_block_time += max(0.0, rel[0] - t_candidate)
                    t_candidate = max(t_candidate, rel[0])
                    while rel and rel[0] <= t_candidate:
                        heapq.heappop(rel)

            tr_s = t_candidate; tr_e = tr_s + tR
            ARR_free[pid] = tr_e
            arr_iv.append((tr_s, tr_e))
            if q_s is None: q_s = tr_s

            # depth>=1: slot acquire
            if not gate_by_io and 'occ_cnt' in locals():
                rel = release_heap[pid]
                while rel and rel[0] <= tr_s:
                    heapq.heappop(rel)
                    occ_cnt[pid] = max(0, occ_cnt[pid]-1)
                occ_cnt[pid] += 1
                plane_max_out = max(plane_max_out, occ_cnt[pid])
            elif gate_by_io:
                plane_max_out = max(plane_max_out, len(BUF_IO[pid]))

            # enqueue ready to COM (group FCFS)
            g = pid // G
            heapq.heappush(ready_heap[g], (tr_e, seq, pid))
            seq += 1

            # schedule COM/IO work-conserving
            next_arr = min(ARR_free)
            now_hint = min(next_arr, ca_gap_next)
            com_schedule(now_hint)

            last_io_e = max(t for t,_ in IO)
            per_plane_vec_idx[pid] += 1
            remain[pid] -= 1
            global_vec_idx += 1

        # flush
        com_schedule(float("inf"))
        last_io_e = max(t for t,_ in IO)
        if q_s is not None and last_io_e is not None:
            q_lat.append((last_io_e - q_s)*1e6)

    # metrics
    avg_q = statistics.mean(q_lat) if q_lat else 0.0
    p50_q = statistics.median(q_lat) if q_lat else 0.0
    p95_q = percentile(q_lat, 0.95)

    last_arr = max(ARR_free) if ARR_free else 0.0
    last_io = max(t for t,_ in IO) if IO else 0.0
    makespan = max(last_io, last_arr,
                   max(COM[g][0][0] if not ('COM0' in locals()) else max(COM0[g][0][0], COM1[g][0][0]) for g in range(NG)) if (('COM' in locals()) or ('COM0' in locals())) else 0.0)

    util_ARRAY = union_duration(arr_iv) / max(1e-15, last_arr)
    util_IO = union_duration(io_iv) / max(1e-15, last_io)
    if 'COM0' in locals():
        com_union = union_duration(com0_iv_all) + union_duration(com1_iv_all)
        last_com = 0.0
        for g in range(NG):
            last_com = max(last_com, COM0[g][0][0], COM1[g][0][0])
        util_COM = com_union / max(1e-15, 2.0*last_com)
        com_util_avg_per_unit_pct = None
    elif 'COM' in locals():
        com_union = union_duration(com_iv_all)
        last_com = 0.0
        for g in range(NG):
            last_com = max(last_com, COM[g][0][0])
        util_COM = com_union / max(1e-15, last_com)
        com_util_avg_per_unit_pct = (util_COM / max(1, NG)) * 100.0
    else:
        util_COM = 0.0; com_util_avg_per_unit_pct = 0.0

    total_vecs = p.query_total_vecs * p.n_queries
    throughput_GBps_io = ((p.out_bytes_per_vector * total_vecs) / max(1e-15, makespan)) / 1e9
    vectors_per_s = total_vecs / max(1e-15, makespan)
    pages_per_s = (len(arr_iv) / max(1e-15, makespan))

    # diagnostics finalize
    # COM queue
    com_q_len_avg = sum(com_q_len_area) / max(1e-15, sum(com_q_last_t))
    com_q_len_max2 = max(com_q_max) if com_q_max else 0
    com_q_nonempty_frac = (sum(com_q_nonempty_time) / max(1e-15, sum(com_q_last_t))) if sum(com_q_last_t)>0 else 0.0

    res = Result()
    res.avg_query_latency_us = avg_q
    res.p50_query_latency_us = p50_q
    res.p95_query_latency_us = p95_q
    res.util_ARRAY = util_ARRAY
    res.util_IO = util_IO
    res.util_COM = util_COM
    res.throughput_GBps_io = throughput_GBps_io
    res.vectors_per_s = vectors_per_s
    res.pages_per_s = pages_per_s
    res.array_util_avg_per_plane_pct = util_ARRAY * 100.0 / max(1, K)
    res.com_util_avg_per_unit_pct = com_util_avg_per_unit_pct if com_util_avg_per_unit_pct else util_COM * 100.0 / max(1, NG)
    res.io_wait_us_avg = statistics.mean(io_waits)*1e6 if io_waits else 0.0
    res.io_wait_us_p95 = percentile([w*1e6 for w in io_waits], 0.95) if io_waits else 0.0
    res.com_wait_us_avg = statistics.mean(com_waits)*1e6 if com_waits else 0.0
    res.com_wait_us_p95 = percentile([w*1e6 for w in com_waits], 0.95) if com_waits else 0.0
    res.io_queue_len_avg = io_q_len_area / max(1e-15, io_q_last_t) if io_q_last_t>0 else 0.0
    res.io_queue_len_max = 0
    res.io_queue_nonempty_frac = io_q_nonempty_time / max(1e-15, io_q_last_t) if io_q_last_t>0 else 0.0
    res.com_queue_len_avg = com_q_len_avg
    res.com_queue_len_max = com_q_len_max2
    res.com_queue_nonempty_frac = com_q_nonempty_frac
    res.array_throttled_by_cap_frac = cap_block_time / max(1e-15, last_arr)
    res.plane_max_outstanding = plane_max_out

    # assertions (INT)
    if gate_by_io:
        # depth==0: same plane: no tR while IO busy → BUF_IO가 cap=1로 표현
        assert res.plane_max_outstanding <= 1 + 1e-9, "INT depth=0: plane outstanding must be 1"
    else:
        assert res.plane_max_outstanding <= (1 + p.cache_depth_per_plane) + 1e-9, \
            "INT depth>=1: plane outstanding violated cap"

    return res, {"last_io": last_io, "last_arr": last_arr, "makespan": makespan}

# ---------------------- Driver ----------------------
def run_one(p: Params) -> Dict[str, Any]:
    if "EXT" in p.mode_list and len(p.mode_list)==1:
        ext_res, _ = simulate_external(p)
        out = res_to_row(p, "EXT", ext_res, ext_baseline=None)
        return out
    elif "INT" in p.mode_list and len(p.mode_list)==1:
        int_res, _ = simulate_internal(p)
        # 공정 비교를 위해 동일 파라미터에서 외부도 한번
        ext_res, _ = simulate_external(p)
        int_res.ext_avg_query_latency_us = ext_res.avg_query_latency_us
        int_res.ext_p50_query_latency_us = ext_res.p50_query_latency_us
        int_res.ext_p95_query_latency_us = ext_res.p95_query_latency_us
        int_res.rel_int_over_ext_latency = (int_res.avg_query_latency_us / ext_res.avg_query_latency_us) if ext_res.avg_query_latency_us>0 else 0.0
        out = res_to_row(p, "INT", int_res, ext_baseline=ext_res)
        return out
    else:
        # 둘 다 요청 시: INT, EXT 각각 계산
        int_res, _ = simulate_internal(p)
        ext_res, _ = simulate_external(p)
        int_res.ext_avg_query_latency_us = ext_res.avg_query_latency_us
        int_res.ext_p50_query_latency_us = ext_res.p50_query_latency_us
        int_res.ext_p95_query_latency_us = ext_res.p95_query_latency_us
        int_res.rel_int_over_ext_latency = (int_res.avg_query_latency_us / ext_res.avg_query_latency_us) if ext_res.avg_query_latency_us>0 else 0.0
        out_int = res_to_row(p, "INT", int_res, ext_baseline=ext_res)
        out_ext = res_to_row(p, "EXT", ext_res, ext_baseline=None)
        return [out_int, out_ext]

def res_to_row(p: Params, mode: str, r: Result, ext_baseline: Optional[Result]):
    row = {
        # --- params echo (non-sweep + sweep 모두 덤프) ---
        "mode": mode,
        "total_planes": p.total_planes,
        "planes_per_group": p.planes_per_group,
        "n_queries": p.n_queries,
        "query_total_vecs": p.query_total_vecs,
        "vector_dist_mode": ",".join(p.vector_dist_mode_list) if isinstance(p.vector_dist_mode_list, list) else p.vector_dist_mode,
        "access_mode": ",".join(p.access_mode_list) if isinstance(p.access_mode_list, list) else p.access_mode_list,
        "page_bytes": p.page_bytes,
        "vector_bytes": p.vector_bytes,
        "tR_us": p.tR_us,
        "tR_hit_us": p.tR_hit_us,
        "com0_us_per_vec": p.com0_us_per_vec,
        "com1_us_per_vec": p.com1_us_per_vec,
        "com_parallel": p.com_parallel,
        "io_gbps": p.io_gbps,
        "io_units_total": p.io_units_total,
        "out_bytes_per_vector": p.out_bytes_per_vector,
        "tCA_us": p.tCA_us,
        "ca_issue_width": p.ca_issue_width,
        "t_cmd_gap_us": p.t_cmd_gap_us,
        "cache_depth_per_plane": p.cache_depth_per_plane,
        "ca_can_overlap_io": p.ca_can_overlap_io,
        # --- results ---
        "avg_query_latency_us": r.avg_query_latency_us,
        "p50_query_latency_us": r.p50_query_latency_us,
        "p95_query_latency_us": r.p95_query_latency_us,
        "ext_avg_query_latency_us": r.ext_avg_query_latency_us,
        "ext_p50_query_latency_us": r.ext_p50_query_latency_us,
        "ext_p95_query_latency_us": r.ext_p95_query_latency_us,
        "rel_int_over_ext_latency": r.rel_int_over_ext_latency,
        "throughput_GBps_io": r.throughput_GBps_io,
        "vectors_per_s": r.vectors_per_s,
        "pages_per_s": r.pages_per_s,
        "util_ARRAY": r.util_ARRAY,
        "util_COM": r.util_COM,
        "util_IO": r.util_IO,
        "array_avg_concurrency": r.array_avg_concurrency,
        "array_util_avg_per_plane_pct": r.array_util_avg_per_plane_pct,
        "com_util_avg_per_unit_pct": r.com_util_avg_per_unit_pct,
        # diagnostics
        "io_wait_us_avg": r.io_wait_us_avg,
        "io_wait_us_p95": r.io_wait_us_p95,
        "io_queue_len_avg": r.io_queue_len_avg,
        "io_queue_len_max": r.io_queue_len_max,
        "io_queue_nonempty_frac": r.io_queue_nonempty_frac,
        "com_wait_us_avg": r.com_wait_us_avg,
        "com_wait_us_p95": r.com_wait_us_p95,
        "com_queue_len_avg": r.com_queue_len_avg,
        "com_queue_len_max": r.com_queue_len_max,
        "com_queue_nonempty_frac": r.com_queue_nonempty_frac,
        "array_throttled_by_cap_frac": r.array_throttled_by_cap_frac,
        "plane_max_outstanding": r.plane_max_outstanding,
    }
    return row

# ---------------------- CLI ----------------------
def parse_multi(s: str, caster):
    xs = [w.strip() for w in s.split(",") if w.strip()!=""]
    return [caster(w) for w in xs]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["run"])
    ap.add_argument("--mode", default="INT", help="INT,EXT 또는 'INT,EXT'")
    ap.add_argument("--total-planes", type=int, default=16)
    ap.add_argument("--planes-per-group", type=int, default=4)
    ap.add_argument("--n-queries", type=int, default=64)
    ap.add_argument("--query-total-vecs", type=int, default=64)
    ap.add_argument("--vector-dist-mode", default="spread", help="spread,batch 또는 'spread,batch'")
    ap.add_argument("--access-mode", default="seq", help="seq,random 또는 'seq,random'")
    ap.add_argument("--page-bytes", type=int, default=4096)
    ap.add_argument("--vector-bytes", type=int, default=1024)
    ap.add_argument("--tR-us", type=float, default=50.0)
    ap.add_argument("--tR-hit-us", type=float, default=0.0)
    ap.add_argument("--com0-us-per-vec", type=float, default=50.0)
    ap.add_argument("--com1-us-per-vec", type=float, default=0.0)
    ap.add_argument("--com-parallel", default="off", choices=["off","on"])
    ap.add_argument("--io-gbps", type=float, default=8.0)
    ap.add_argument("--io-units-total", type=int, default=1)
    ap.add_argument("--out-bytes-per-vector", type=int, default=1024)
    ap.add_argument("--tCA-us", type=float, default=0.0)
    ap.add_argument("--ca-issue-width", type=int, default=1)
    ap.add_argument("--t-cmd-gap-us", type=float, default=0.0)
    ap.add_argument("--cache-depth-per-plane", type=int, default=0)
    ap.add_argument("--ca-can-overlap-io", default="on", choices=["on","off"])
    ap.add_argument("--csv", default="results.csv")
    args = ap.parse_args()

    if args.cmd == "run":
        mode_list = parse_multi(args.mode, str)
        vdm_list  = parse_multi(args.vector_dist_mode, str)
        am_list   = parse_multi(args.access_mode, str)

        rows = []
        # sweep over multi-values
        for mode in mode_list:
            for vdm in vdm_list:
                for am in am_list:
                    p = Params({
                        "mode": [mode],
                        "total_planes": args.total_planes,
                        "planes_per_group": args.planes_per_group,
                        "n_queries": args.n_queries,
                        "query_total_vecs": args.query_total_vecs,
                        "page_bytes": args.page_bytes,
                        "vector_bytes": args.vector_bytes,
                        "tR_us": args.tR_us,
                        "tR_hit_us": args.tR_hit_us,
                        "com0_us_per_vec": args.com0_us_per_vec,
                        "com1_us_per_vec": args.com1_us_per_vec,
                        "com_parallel": args.com_parallel,
                        "io_gbps": args.io_gbps,
                        "io_units_total": args.io_units_total,
                        "out_bytes_per_vector": args.out_bytes_per_vector,
                        "vector_dist_mode": vdm,
                        "access_mode": am,
                        "tCA_us": args.tCA_us,
                        "ca_issue_width": args.ca_issue_width,
                        "t_cmd_gap_us": args.t_cmd_gap_us,
                        "cache_depth_per_plane": args.cache_depth_per_plane,
                        "ca_can_overlap_io": args.ca_can_overlap_io,
                    })
                    out = run_one(p)
                    if isinstance(out, list):
                        rows.extend(out)
                    else:
                        rows.append(out)

        # write CSV
        cols = list(rows[0].keys()) if rows else []
        with open(args.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r in rows:
                w.writerow(r)
        print(f"Wrote {len(rows)} rows → {args.csv}")

if __name__ == "__main__":
    main()
