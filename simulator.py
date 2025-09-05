#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base + Patch 001 (baked)
Summary:
  - Sweep 기능 내장
  - planes_per_group, cache_depth_per_plane: 리스트 스윕 지원
  - com0_us_per_vec, com1_us_per_vec: linspace 스윕 지원 (--com0-lin, --com1-lin)
  - INT/EXT 비교군 모두 지원 (INT 실행 시 EXT도 자동 비교)

Notes:
  - 이후 패치는 이 파일을 기준으로 넘버링된 diff로 발행합니다.
  - 외부 의존성 없음 (Python 3.9+ 권장)
"""

import argparse, sys, csv, math, statistics, heapq
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

def linspace_inclusive(a: float, b: float, n: int) -> List[float]:
    """Generate n points from a to b inclusive (n>=1)."""
    if n <= 1:
        return [float(a)]
    step = (float(b) - float(a)) / (n - 1)
    return [float(a) + i*step for i in range(n)]

# ---------------------- Params ----------------------
class Params:
    def __init__(self, args: Dict[str, Any]):
        self.mode_list = args["mode"]                # ['INT'] or ['EXT'] or ['INT','EXT']
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
        self.com_parallel = args["com_parallel"]     # 'off' or 'on'
        self.io_gbps = float(args["io_gbps"])
        self.io_units_total = int(args["io_units_total"])
        self.out_bytes_per_vector = int(args["out_bytes_per_vector"])
        self.vector_dist_mode = args["vector_dist_mode"]  # 'spread' | 'batch'
        self.access_mode = args["access_mode"]            # 'seq' | 'random'
        self.tCA_us = float(args["tCA_us"])
        self.ca_issue_width = int(args["ca_issue_width"])
        self.t_cmd_gap_us = float(args["t_cmd_gap_us"])
        self.cache_depth_per_plane = int(args["cache_depth_per_plane"])
        self.ca_can_overlap_io = args["ca_can_overlap_io"]  # 'on'|'off'

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
        # diagnostics (subset)
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
        self.plane_max_outstanding = 0

# ---------------------- Helpers ----------------------
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
    io_waits: List[float] = []
    plane_max_out = 0

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

            io_ft, iu = heapq.heappop(IO)
            io_s = max(tr_e, io_ft); io_e = io_s + tIO
            heapq.heappush(IO, (io_e, iu))
            if tIO > 0: io_iv.append((io_s, io_e))
            heapq.heappush(buf, io_e)
            last_io_e = io_e if (last_io_e is None or io_e > last_io_e) else last_io_e

            io_waits.append(max(0.0, io_s - tr_e))
            plane_max_out = max(plane_max_out, len(buf))

            per_plane_vec_idx[pid] += 1
            remain[pid] -= 1
            global_vec_idx += 1

        if q_s is not None and last_io_e is not None:
            q_lat.append((last_io_e - q_s)*1e6)

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
    res.array_avg_concurrency = 0.0
    res.array_util_avg_per_plane_pct = (util_ARRAY * 100.0) / max(1, K)
    res.com_util_avg_per_unit_pct = 0.0
    res.io_wait_us_avg = statistics.mean(io_waits)*1e6 if io_waits else 0.0
    res.io_wait_us_p95 = percentile([w*1e6 for w in io_waits], 0.95) if io_waits else 0.0
    res.io_queue_len_avg = 0.0
    res.io_queue_len_max = 0
    res.io_queue_nonempty_frac = 0.0
    res.array_throttled_by_cap_frac = 0.0
    res.plane_max_outstanding = plane_max_out

    assert res.plane_max_outstanding <= (1 + p.cache_depth_per_plane) + 1e-9, \
        "EXT: plane outstanding violated cap"

    return res, {"last_io": last_io, "last_arr": last_arr, "makespan": makespan}

# ---------------------- Engine: Internal (baseline; later patches can refine) ----------------------
def simulate_internal(p: Params) -> Tuple[Result, Dict[str, float]]:
    # 간단 모델: 외부 파이프라인에 벡터당 COM 비용을 직렬로 더한 근사값
    ext_res, stats = simulate_external(p)
    extra = (p.com0_us_per_vec + p.com1_us_per_vec) * 1e-6 * p.query_total_vecs
    r = Result()
    # 복사
    for k,v in ext_res.__dict__.items():
        setattr(r, k, v)
    r.avg_query_latency_us = ext_res.avg_query_latency_us + extra*1e6
    r.util_COM = 0.0
    return r, stats

# ---------------------- Driver ----------------------
def run_one(p: Params) -> Dict[str, Any]:
    if "EXT" in p.mode_list and len(p.mode_list)==1:
        ext_res, _ = simulate_external(p)
        out = res_to_row(p, "EXT", ext_res, ext_baseline=None)
        return out
    elif "INT" in p.mode_list and len(p.mode_list)==1:
        int_res, _ = simulate_internal(p)
        ext_res, _ = simulate_external(p)
        int_res.ext_avg_query_latency_us = ext_res.avg_query_latency_us
        int_res.ext_p50_query_latency_us = ext_res.p50_query_latency_us
        int_res.ext_p95_query_latency_us = ext_res.p95_query_latency_us
        int_res.rel_int_over_ext_latency = (int_res.avg_query_latency_us / ext_res.avg_query_latency_us) if ext_res.avg_query_latency_us>0 else 0.0
        out = res_to_row(p, "INT", int_res, ext_baseline=ext_res)
        return out
    else:
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
        # params echo
        "mode": mode,
        "total_planes": p.total_planes,
        "planes_per_group": p.planes_per_group,
        "n_queries": p.n_queries,
        "query_total_vecs": p.query_total_vecs,
        "vector_dist_mode": p.vector_dist_mode,
        "access_mode": p.access_mode,
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
        # results
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
        # diags
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
    # sweep: list 지원
    ap.add_argument("--planes-per-group", default="4",
                    help="예: '1,2,4,8' 또는 단일 '4'")
    ap.add_argument("--n-queries", type=int, default=64)
    ap.add_argument("--query-total-vecs", type=int, default=64)
    ap.add_argument("--vector-dist-mode", default="spread",
                    help="spread,batch 또는 'spread,batch'")
    ap.add_argument("--access-mode", default="seq",
                    help="seq,random 또는 'seq,random'")
    ap.add_argument("--page-bytes", type=int, default=4096)
    ap.add_argument("--vector-bytes", type=int, default=1024)
    ap.add_argument("--tR-us", type=float, default=50.0)
    ap.add_argument("--tR-hit-us", type=float, default=0.0)
    # com0/com1: 단일 값 또는 linspace 스윕 지원
    ap.add_argument("--com0-us-per-vec", default="50.0",
                    help="단일 값 또는 linspace 미사용 시")
    ap.add_argument("--com1-us-per-vec", default="0.0",
                    help="단일 값 또는 linspace 미사용 시")
    ap.add_argument("--com0-lin", default=None,
                    help="linspace: 'start:end:num' 또는 'start,end,num'")
    ap.add_argument("--com1-lin", default=None,
                    help="linspace: 'start:end:num' 또는 'start,end,num'")
    ap.add_argument("--com-parallel", default="off", choices=["off","on"])
    ap.add_argument("--io-gbps", type=float, default=8.0)
    ap.add_argument("--io-units-total", type=int, default=1)
    ap.add_argument("--out-bytes-per-vector", type=int, default=1024)
    ap.add_argument("--tCA-us", type=float, default=0.0)
    ap.add_argument("--ca-issue-width", type=int, default=1)
    ap.add_argument("--t-cmd-gap-us", type=float, default=0.0)
    # cache depth: list 지원
    ap.add_argument("--cache-depth-per-plane", default="0",
                    help="예: '0,1' 또는 단일 '0'")
    ap.add_argument("--ca-can-overlap-io", default="on", choices=["on","off"])
    ap.add_argument("--csv", default="results.csv")
    args = ap.parse_args()

    if args.cmd == "run":
        mode_list = parse_multi(args.mode, str)
        vdm_list  = parse_multi(args.vector_dist_mode, str)
        am_list   = parse_multi(args.access_mode, str)

        # --- sweep lists (001) ---
        pp_list = parse_multi(args.planes_per_group, int)
        cd_list = parse_multi(args.cache_depth_per_plane, int)
        if args.com0_lin:
            s = args.com0_lin.replace(",", ":")
            a,b,n = s.split(":")
            com0_list = [float(x) for x in linspace_inclusive(float(a), float(b), int(n))]
        else:
            com0_list = parse_multi(args.com0_us_per_vec, float)
        if args.com1_lin:
            s = args.com1_lin.replace(",", ":")
            a,b,n = s.split(":")
            com1_list = [float(x) for x in linspace_inclusive(float(a), float(b), int(n))]
        else:
            com1_list = parse_multi(args.com1_us_per_vec, float)

        rows = []
        for mode in mode_list:
            for vdm in vdm_list:
                for am in am_list:
                    for pp in pp_list:
                        for cd in cd_list:
                            for c0 in com0_list:
                                for c1 in com1_list:
                                    p = Params({
                                        "mode": [mode],
                                        "total_planes": args.total_planes,
                                        "planes_per_group": pp,
                                        "n_queries": args.n_queries,
                                        "query_total_vecs": args.query_total_vecs,
                                        "page_bytes": args.page_bytes,
                                        "vector_bytes": args.vector_bytes,
                                        "tR_us": args.tR_us,
                                        "tR_hit_us": args.tR_hit_us,
                                        "com0_us_per_vec": c0,
                                        "com1_us_per_vec": c1,
                                        "com_parallel": args.com_parallel,
                                        "io_gbps": args.io_gbps,
                                        "io_units_total": args.io_units_total,
                                        "out_bytes_per_vector": args.out_bytes_per_vector,
                                        "vector_dist_mode": vdm,
                                        "access_mode": am,
                                        "tCA_us": args.tCA_us,
                                        "ca_issue_width": args.ca_issue_width,
                                        "t_cmd_gap_us": args.t_cmd_gap_us,
                                        "cache_depth_per_plane": cd,
                                        "ca_can_overlap_io": args.ca_can_overlap_io,
                                    })
                                    out = run_one(p)
                                    if isinstance(out, list):
                                        rows.extend(out)
                                    else:
                                        rows.append(out)

        cols = list(rows[0].keys()) if rows else []
        with open(args.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r in rows:
                w.writerow(r)
        print(f"Wrote {len(rows)} rows → {args.csv}")

if __name__ == "__main__":
    main()
