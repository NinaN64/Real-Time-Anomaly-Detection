import argparse
import csv
import json
import math
import os
import time
from datetime import datetime
from confluent_kafka import Consumer, KafkaException
import config

SUPPORTED_DETECTORS = {"mmd", "padd"}

IDLE_TIMEOUT_SECS = 30


def nearest_interval_distance(seq, event):
    start = event["start_seq"]
    end = event.get("end_seq", start)
    if start <= seq <= end:
        return 0.0
    return float(min(abs(seq - start), abs(seq - end)))


def fmt(v):
    if v is None:
        return "nan"
    if isinstance(v, float) and math.isnan(v):
        return "nan"
    return f"{v:.4f}"


def csv_val(v, decimals=4):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return float("nan")

    try:
        return round(float(v), decimals)
    except (ValueError, TypeError):
        return v


def compute_r_matchaware(tp, fp, fn):
    return (fp + fn) / max(1, tp + fn)




def log(msg: str) -> None:
    try:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)
    except UnicodeEncodeError:
        clean_msg = msg.replace("✓", "[OK]").replace("✗", "[FAIL]")
        try:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {clean_msg}", flush=True)
        except UnicodeEncodeError:
            ascii_msg = msg.encode("ascii", errors="replace").decode("ascii")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {ascii_msg}", flush=True)


def print_separator(char: str = "─", width: int = 60) -> None:
    try:
        print(char * width, flush=True)
    except UnicodeEncodeError:
        fallback = "=" if char == "═" else "-"
        print(fallback * width, flush=True)


def compute_d1_d2_r(drift_events, alert_seqs, fp_count):
    """
    Compute D1, D2, R drift detection error measures.
    Reference: Komorniczak et al. (2022), Knowledge-Based Systems.
    All three return float("nan") when the required inputs are absent.
    """
    n_drifts = len(drift_events)
    n_detections = len(alert_seqs)

    # D1: Mean detection → nearest drift interval distance
    if n_detections == 0 or n_drifts == 0:
        d1 = float("nan")
    else:
        d1 = sum(
            min(nearest_interval_distance(a, e) for e in drift_events)
            for a in alert_seqs
        ) / n_detections

    # D2: Mean drift interval → nearest detection distance
    if n_drifts == 0 or n_detections == 0:
        d2 = float("nan")
    else:
        d2 = sum(
            min(nearest_interval_distance(a, e) for a in alert_seqs)
            for e in drift_events
        ) / n_drifts

    # R: scaled ratio — optimum at 0
    if n_detections == 0 or n_drifts == 0:
        r = float("nan")
        r_matchaware = float("nan")
    else:
        r = abs(n_drifts / n_detections - 1)
        
        tp = sum(1 for e in drift_events if e.get("matched"))
        fn = n_drifts - tp
        fp = fp_count
        r_matchaware = compute_r_matchaware(tp, fp, fn)

    return d1, d2, r, r_matchaware


def build_drift_events(unique_docs, args, logged_drift_starts, logged_drift_ends):
    sorted_seqs = sorted(unique_docs.keys())

    drift_events = []
    active_drift = None
    last_drift_start_seq = -1
    in_cooldown = False
    cooldown_remaining = 0
    total_baseline_docs = 0
    seq_states = {}

    for seq in sorted_seqs:
        d = unique_docs[seq]
        is_drift    = d["is_drift"]
        drift_start = d["drift_start"]
        d_type      = d["d_type"]
        ts          = d["ts"]

        if is_drift:
            in_cooldown = False
            cooldown_remaining = 0
            if drift_start != last_drift_start_seq:
                active_drift = {
                    "start_seq":      drift_start,
                    "end_seq":        seq,
                    "type":           d_type,
                    "start_ts":       ts,
                    "matched":        False,
                    "latency_docs":   None,
                    "latency_ms":     None,
                    "match_deadline": drift_start + args.match_window,
                }
                drift_events.append(active_drift)
                last_drift_start_seq = drift_start

                if drift_start not in logged_drift_starts:
                    log(f"  [DRIFT START] type={d_type}  start_seq={drift_start}  "
                        f"deadline≤seq {active_drift['match_deadline']}")
                    logged_drift_starts.add(drift_start)
            else:
                if active_drift:
                    active_drift["end_seq"] = seq
                elif drift_events and drift_events[-1]["start_seq"] == drift_start:
                    drift_events[-1]["end_seq"] = seq
                    active_drift = drift_events[-1]
        else:
            if active_drift:
                prev_drift_start = active_drift["start_seq"]
                if prev_drift_start not in logged_drift_ends:
                    log(f"  [DRIFT END]   cooldown={args.cooldown} docs")
                    logged_drift_ends.add(prev_drift_start)
                active_drift = None
                in_cooldown = True
                cooldown_remaining = args.cooldown

            if in_cooldown:
                cooldown_remaining -= 1
                if cooldown_remaining <= 0:
                    in_cooldown = False
            else:
                total_baseline_docs += 1

        if active_drift:
            seq_states[seq] = "DRIFT"
        elif in_cooldown:
            seq_states[seq] = "COOLDOWN"
        else:
            seq_states[seq] = "BASELINE"

    return drift_events, seq_states, total_baseline_docs, sorted_seqs


def match_alerts(drift_events, unique_alerts, ts_to_seq, seq_states, sorted_seqs,
                 args, logged_alerts, logged_fps, confirmed_fps, confirmed_tp_windows):
    """
    Match alerts to drift events.
    """
    sorted_alerts = sorted(unique_alerts.values(), key=lambda a: a.get("windowEnd", 0))
    alert_seqs_set = set()

    # Build a fast lookup: drift start_seq -> event dict
    event_by_start = {e["start_seq"]: e for e in drift_events}

    for alert in sorted_alerts:
        prev_match = alert.get("_matched_event_start")
        if prev_match is not None and prev_match in event_by_start:
            event = event_by_start[prev_match]
            w_end_ts  = alert.get("windowEnd")
            doc_count = alert.get("docCount")
            alert_ts  = alert.get("detected_at")

            window_end_seq = ts_to_seq.get((w_end_ts, doc_count))
            if window_end_seq is None:
                window_end_seq = ts_to_seq.get(w_end_ts)

            if window_end_seq is not None and event["start_seq"] <= window_end_seq <= event["match_deadline"]:
                event["matched"] = True
                event["latency_docs"] = window_end_seq - event["start_seq"]
                event["latency_ms"]   = alert_ts - event["start_ts"]
                alert["_latency_docs"] = event["latency_docs"]
                alert["_latency_ms"]   = event["latency_ms"]
                
                confirmed_tp_windows.add(w_end_ts)
                confirmed_fps.discard(w_end_ts)
            else:
                alert["_matched_event_start"] = None
                alert["_latency_docs"] = None
                alert["_latency_ms"] = None

    # Second pass: attempt to match unmatched alerts to unmatched events
    for alert in sorted_alerts:
        w_end_ts  = alert.get("windowEnd")
        doc_count = alert.get("docCount")
        alert_ts  = alert.get("detected_at")

        window_end_seq = ts_to_seq.get((w_end_ts, doc_count))
        if window_end_seq is None:
            window_end_seq = ts_to_seq.get(w_end_ts)

        if alert.get("_matched_event_start") is not None:
            if window_end_seq is not None:
                alert_seqs_set.add(window_end_seq)
            if w_end_ts in confirmed_fps:
                confirmed_fps.discard(w_end_ts)
            confirmed_tp_windows.add(w_end_ts)
            continue

        if window_end_seq is None:
            continue

        candidates = []
        for event in drift_events:
            if not event["matched"] and event["start_seq"] <= window_end_seq <= event["match_deadline"]:
                candidates.append(event)

        matched_any   = False
        matched_event = None

        if candidates:
            matched_event = min(
                candidates,
                key=lambda e: nearest_interval_distance(window_end_seq, e)
            )
            matched_event["matched"]      = True
            matched_event["latency_docs"] = window_end_seq - matched_event["start_seq"]
            matched_event["latency_ms"]   = alert_ts - matched_event["start_ts"]
            matched_any   = True
            alert["_matched_event_start"] = matched_event["start_seq"]
            alert["_latency_docs"]        = matched_event["latency_docs"]
            alert["_latency_ms"]          = matched_event["latency_ms"]

        if matched_any:
            alert_seqs_set.add(window_end_seq)
            confirmed_tp_windows.add(w_end_ts)
            confirmed_fps.discard(w_end_ts)
            if w_end_ts not in logged_alerts:
                log(f"  [ALERT ✓] detector={args.detector.upper()}  "
                    f"drift_type={matched_event['type']}  "
                    f"latency={matched_event['latency_docs']} docs / "
                    f"{matched_event['latency_ms']:.0f} ms")
                logged_alerts.add(w_end_ts)
        else:
            if w_end_ts not in confirmed_tp_windows:
                state = "BASELINE"
                applicable_seqs = [s for s in sorted_seqs if s <= window_end_seq]
                if applicable_seqs:
                    state = seq_states.get(applicable_seqs[-1], "BASELINE")

                if state == "BASELINE":
                    confirmed_fps.add(w_end_ts)
                    if w_end_ts not in logged_fps:
                        log(f"  [ALERT ✗] FALSE POSITIVE #{len(confirmed_fps)}  "
                            f"detector={args.detector.upper()}  "
                            f"window_end_seq={window_end_seq}")
                        logged_fps.add(w_end_ts)

    return list(alert_seqs_set)


def run_evaluation(unique_docs, unique_alerts, ts_to_seq, args,
                   logged_drift_starts, logged_drift_ends, logged_alerts, logged_fps,
                   confirmed_fps, confirmed_tp_windows):
    drift_events, seq_states, total_baseline_docs, sorted_seqs = build_drift_events(
        unique_docs, args, logged_drift_starts, logged_drift_ends
    )
    alert_seqs = match_alerts(
        drift_events, unique_alerts, ts_to_seq, seq_states, sorted_seqs,
        args, logged_alerts, logged_fps, confirmed_fps, confirmed_tp_windows
    )
    return drift_events, total_baseline_docs, alert_seqs


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--detector",      required=True,
                        choices=list(SUPPORTED_DETECTORS),
                        help="Detector to evaluate: mmd or padd")
    parser.add_argument("--trigger-n",     type=int, required=True)
    parser.add_argument("--window-type",   required=True)
    parser.add_argument("--source-topic",  required=True)
    parser.add_argument("--match-window",  type=int, default=500)
    parser.add_argument("--cooldown",      type=int, default=50)
    parser.add_argument("--output",        default="evaluation_results.csv")
    parser.add_argument("--idle-timeout",  type=int, default=IDLE_TIMEOUT_SECS,
                        help="Seconds of Kafka silence before experiment is declared finished")
    parser.add_argument("--max-batches",   type=int, default=None,
                        help="Optional maximum number of batches to process before finishing")
    parser.add_argument("--dataset", default="unknown",
                        help="Dataset name (newsgroups, arxiv, yahoo, agnews)")
    parser.add_argument("--far-ignore-cooldown", action="store_true",
                        help="Ignore cooldown documents in the FAR denominator")
    args = parser.parse_args()

    if args.detector not in SUPPORTED_DETECTORS:
        print(f"[ERROR] Unsupported detector '{args.detector}'. "
              f"Choose from: {SUPPORTED_DETECTORS}")
        return

    c = Consumer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "group.id": f"evaluator-group-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    c.subscribe([args.source_topic, config.ALERT_TOPIC])

    unique_docs   = {}
    unique_alerts = {}
    ts_to_seq     = {}

    logged_drift_starts = set()
    logged_drift_ends   = set()
    logged_alerts       = set()
    logged_fps          = set()

    confirmed_fps = set()
    confirmed_tp_windows = set()

    batches_seen        = 0
    total_docs_seen     = 0
    total_baseline_docs = 0
    drift_events        = []
    total_alerts_seen   = 0
    alert_seqs          = []

    print_separator("═")
    log(f"Evaluator started")
    log(f"  Detector    : {args.detector.upper()}")
    log(f"  Window type : {args.window_type}")
    log(f"  Trigger-N   : {args.trigger_n}")
    log(f"  Source topic: {args.source_topic}")
    log(f"  Alert topic : {config.ALERT_TOPIC}")
    log(f"  Idle timeout: {args.idle_timeout}s  (auto-finish when no messages arrive)")
    log(f"  Output file : {args.output}")
    print_separator("═")
    print(flush=True)

    last_msg_time = time.time()

    try:
        while True:
            elapsed_idle = time.time() - last_msg_time
            if elapsed_idle >= args.idle_timeout:
                print_separator()
                log(f"No messages for {args.idle_timeout}s — experiment considered finished.")
                print_separator()
                break

            msg = c.poll(1.0)
            if msg is None:
                remaining = args.idle_timeout - int(time.time() - last_msg_time)
                if remaining <= 10 and remaining > 0:
                    log(f"  [idle] No messages — finishing in {remaining}s "
                        f"unless more arrive...")
                continue
            if msg.error():
                continue

            last_msg_time = time.time()

            val   = json.loads(msg.value().decode("utf-8"))
            topic = msg.topic()

            if topic == args.source_topic:
                docs = val.get("documents", [])
                if not docs:
                    continue

                batches_seen += 1

                for d in docs:
                    seq = d.get("sequenceNumber")
                    if seq is not None:
                        unique_docs[seq] = {
                            "is_drift":    d.get("driftLabel", False),
                            "drift_start": d.get("driftStartTs"),
                            "d_type":      d.get("driftType"),
                            "ts":          d.get("timestamp"),
                        }

                window_end = val.get("windowEnd")
                if window_end:
                    max_seq = max(d.get("sequenceNumber", 0) for d in docs)
                    ts_to_seq[(window_end, len(docs))] = max_seq
                    if window_end not in ts_to_seq:
                        ts_to_seq[window_end] = max_seq

                drift_events, total_baseline_docs, alert_seqs = run_evaluation(
                    unique_docs, unique_alerts, ts_to_seq, args,
                    logged_drift_starts, logged_drift_ends, logged_alerts, logged_fps,
                    confirmed_fps, confirmed_tp_windows
                )
                total_docs_seen   = len(unique_docs)
                total_alerts_seen = len(unique_alerts)

                if batches_seen % 50 == 0:
                    matched_so_far = sum(1 for e in drift_events if e["matched"])
                    log(f"  [progress] batches={batches_seen}  docs={total_docs_seen}  "
                        f"drift_events={len(drift_events)}  matched={matched_so_far}  "
                        f"alerts={total_alerts_seen}  FP={len(confirmed_fps)}")

                if args.max_batches is not None and batches_seen >= args.max_batches:
                    print_separator()
                    log(f"Processed max batches ({args.max_batches}) — finishing.")
                    print_separator()
                    break

            elif topic == config.ALERT_TOPIC:
                if val.get("detector") != args.detector:
                    continue

                w_end_ts = val.get("windowEnd")
                if w_end_ts is not None:
                    unique_alerts[w_end_ts] = val

                drift_events, total_baseline_docs, alert_seqs = run_evaluation(
                    unique_docs, unique_alerts, ts_to_seq, args,
                    logged_drift_starts, logged_drift_ends, logged_alerts, logged_fps,
                    confirmed_fps, confirmed_tp_windows
                )
                total_docs_seen   = len(unique_docs)
                total_alerts_seen = len(unique_alerts)

    except KeyboardInterrupt:
        log("Stopped manually (Ctrl+C).")
    finally:
        c.close()

        fp_count = len(confirmed_fps)

        print(flush=True)
        print_separator("═")
        log("EXPERIMENT SUMMARY")
        print_separator("═")
        log(f"  Detector        : {args.detector.upper()}")
        log(f"  Batches seen    : {batches_seen}")
        log(f"  Docs seen       : {total_docs_seen}")
        log(f"  Baseline docs   : {total_baseline_docs}")
        log(f"  Drift events    : {len(drift_events)}")
        log(f"  Alerts received : {total_alerts_seen}")
        log(f"  False positives : {fp_count}")
        print_separator()

        results = {}
        for event in drift_events:
            t = event["type"]
            if t not in results:
                results[t] = {"tp": 0, "fn": 0, "lat_docs": [], "lat_ms": []}
            if event["matched"]:
                results[t]["tp"] += 1
                results[t]["lat_docs"].append(event["latency_docs"])
                results[t]["lat_ms"].append(event["latency_ms"])
            else:
                results[t]["fn"] += 1

        if getattr(args, "far_ignore_cooldown", False):
            far_denominator = total_baseline_docs
        else:
            far_denominator = sum(1 for d in unique_docs.values() if not d.get("is_drift"))

        far = fp_count / max(1, far_denominator)

        alert_seqs_unique = list(set(alert_seqs))
        d1_global, d2_global, r_global, r_matchaware_global = compute_d1_d2_r(
            drift_events, alert_seqs_unique, fp_count
        )

        log(f"  D1 (mean detection→drift dist) : {fmt(d1_global)}")
        log(f"  D2 (mean drift→detection dist) : {fmt(d2_global)}")
        log(f"  R  (drift/detection ratio err) : {fmt(r_global)}")
        log(f"  R_matchaware (diagnostic err)  : {fmt(r_matchaware_global)}")
        print_separator()

        if not results:
            log("  No drift events recorded — no metrics to write.")
        else:
            for t, m in results.items():
                tp = m["tp"]
                fn = m["fn"]
                fp = fp_count
                
                precision    = tp / (tp + fp) if (tp + fp) > 0 else 0.0
                recall       = tp / (tp + fn) if (tp + fn) > 0 else 0.0
                f1           = (2 * precision * recall / (precision + recall)
                                if (precision + recall) > 0 else 0.0)
                
                avg_lat_docs = (sum(m["lat_docs"]) / len(m["lat_docs"])
                                if m["lat_docs"] else float("nan"))
                avg_lat_ms   = (sum(m["lat_ms"]) / len(m["lat_ms"])
                                if m["lat_ms"] else float("nan"))

                log(f"  [{t.upper()}]  P≈{precision:.4f}  R={recall:.4f}  "
                    f"F1={f1:.4f}  LatDocs={avg_lat_docs:.1f}  LatMs={avg_lat_ms:.1f}  "
                    f"FAR={far:.6f}  D1={fmt(d1_global)}  D2={fmt(d2_global)}  "
                    f"R_ratio={fmt(r_global)}")

            global_tp = sum(m["tp"] for m in results.values())
            global_fn = sum(m["fn"] for m in results.values())
            global_fp = fp_count

            global_precision = global_tp / (global_tp + global_fp) if (global_tp + global_fp) > 0 else 0.0
            global_recall    = global_tp / (global_tp + global_fn) if (global_tp + global_fn) > 0 else 0.0
            global_f1        = (2 * global_precision * global_recall / (global_precision + global_recall)
                                if (global_precision + global_recall) > 0 else 0.0)
            global_far       = global_fp / max(1, far_denominator)

            all_lat_docs = []
            all_lat_ms = []
            for m in results.values():
                all_lat_docs.extend(m["lat_docs"])
                all_lat_ms.extend(m["lat_ms"])
            avg_global_lat_docs = sum(all_lat_docs) / len(all_lat_docs) if all_lat_docs else float("nan")
            avg_global_lat_ms   = sum(all_lat_ms) / len(all_lat_ms) if all_lat_ms else float("nan")

            log(f"  [__GLOBAL__]  P={global_precision:.4f}  R={global_recall:.4f}  "
                f"F1={global_f1:.4f}  LatDocs={avg_global_lat_docs:.1f}  LatMs={avg_global_lat_ms:.1f}  "
                f"FAR={global_far:.6f}  D1={fmt(d1_global)}  D2={fmt(d2_global)}  "
                f"R_ratio={fmt(r_global)}")

        print_separator("═")

        file_exists = os.path.isfile(args.output)
        with open(args.output, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow([
                    "Dataset", "Detector", "WindowType", "TriggerN", "DriftType",
                    "Precision", "Recall", "F1", "LatencyDocs", "LatencyMs", "FAR",
                    "D1", "D2", "R", "R_matchaware",
                ])
            for t, m in results.items():
                tp = m["tp"]
                fn = m["fn"]
                fp = fp_count

                precision    = tp / (tp + fp) if (tp + fp) > 0 else 0.0
                recall       = tp / (tp + fn) if (tp + fn) > 0 else 0.0
                f1           = (2 * precision * recall / (precision + recall)
                                if (precision + recall) > 0 else 0.0)
                
                avg_lat_docs = (sum(m["lat_docs"]) / len(m["lat_docs"])
                                if m["lat_docs"] else float("nan"))
                avg_lat_ms   = (sum(m["lat_ms"]) / len(m["lat_ms"])
                                if m["lat_ms"] else float("nan"))
                
                r_matchaware = compute_r_matchaware(tp, fp, fn)

                writer.writerow([
                    args.dataset, args.detector, args.window_type, args.trigger_n, t,
                    csv_val(precision), csv_val(recall), csv_val(f1),
                    csv_val(avg_lat_docs, 2), csv_val(avg_lat_ms, 2), csv_val(far, 6),
                    csv_val(d1_global), csv_val(d2_global), csv_val(r_global), csv_val(r_matchaware),
                ])

            if results:
                # Write the '__GLOBAL__' summary row
                writer.writerow([
                    args.dataset, args.detector, args.window_type, args.trigger_n, "__GLOBAL__",
                    csv_val(global_precision), csv_val(global_recall), csv_val(global_f1),
                    csv_val(avg_global_lat_docs, 2), csv_val(avg_global_lat_ms, 2), csv_val(global_far, 6),
                    csv_val(d1_global), csv_val(d2_global), csv_val(r_global), csv_val(r_matchaware_global),
                ])

        log(f"Results appended to {args.output}")
        print_separator("═")


if __name__ == "__main__":
    main()