"""Simulation for ticket 23: peak gateway load per rack size.

Reuses the murmur2 port and the capped rendezvous rule from 22-tracking-sim.py.
Per rack, 512 equal user partitions are routed two ways:
- today: murmur2("<topicId>-<partitionIndex>") % b over the rack's sorted brokers;
- gateways: the user partition's coordinator partition, murmur2(20-byte log key) % N,
  then the highest hash(logKey, brokerId) among that coordinator partition's g gateways.
Reports the busiest broker's load against today's busiest and against the rack mean.
"""
import importlib.util
import os
import random
import struct
import uuid

_spec = importlib.util.spec_from_file_location(
    "sim22", os.path.join(os.path.dirname(__file__), "22-tracking-sim.py"))
sim22 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sim22)
murmur2, to_positive, assign_rack = sim22.murmur2, sim22.to_positive, sim22.assign_rack


def user_partitions(rng, topics=8, per_topic=64):
    out = []
    for _ in range(topics):
        tid = uuid.UUID(int=rng.getrandbits(128))
        for p in range(per_topic):
            out.append((tid.bytes + struct.pack(">i", p), f"{tid}-{p}".encode()))
    return out


def rack_loads(brokers, ups, n):
    b = len(brokers)
    srt = sorted(brokers)
    today = {br: 0 for br in brokers}
    gw = {br: 0 for br in brokers}
    held = assign_rack(brokers, n)
    gws_of = {cp: [br for br in brokers if cp in held[br]] for cp in range(n)}
    for log_key, legacy in ups:
        today[srt[to_positive(murmur2(legacy)) % b]] += 1
        cp = to_positive(murmur2(log_key)) % n
        pick = max(gws_of[cp], key=lambda br: (to_positive(murmur2(log_key + struct.pack(">i", br))), -br))
        gw[pick] += 1
    return today, gw, held


def run(per_rack, trials, rng, n=8):
    s_today = s_gw = s_ratio = 0.0
    worst_ratio = 0.0
    s_peak_mean = 0.0
    for _ in range(trials):
        brokers = rng.sample(range(0, 1000), per_rack)
        ups = user_partitions(rng)
        today, gw, _ = rack_loads(brokers, ups, n)
        mt, mg = max(today.values()), max(gw.values())
        s_today += mt
        s_gw += mg
        s_ratio += mg / mt
        worst_ratio = max(worst_ratio, mg / mt)
        s_peak_mean += mg / (len(ups) / per_rack)
    return {
        "today_peak": s_today / trials,
        "gw_peak": s_gw / trials,
        "ratio_of_means": s_gw / s_today,
        "mean_ratio": s_ratio / trials,
        "worst_ratio": worst_ratio,
        "gw_peak_over_rack_mean": s_peak_mean / trials,
    }


def mix_parts_per_file(per_rack, frac, rng, n=8, ppb=64, batches=256, trials=40):
    """Parts per file when a share of user partitions keeps replica routing.

    Equal traffic per partition, one replica per rack, and a file of `batches`
    batches drawn from the partitions that route to the broker in its rack.
    """
    b = 3 * per_rack
    p_total = ppb * b
    tot = files = 0
    for _ in range(trials):
        ids = rng.sample(range(1000), b)
        local = set(rng.sample(range(p_total), round(frac * p_total)))
        cp = {p: rng.randrange(n) for p in range(p_total)}
        for rb in (ids[r::3] for r in range(3)):
            held = assign_rack(rb, n)
            gws = {c: [br for br in rb if c in held[br]] for c in range(n)}
            routed = {br: [] for br in rb}
            for p in range(p_total):
                routed[rng.choice(rb) if p in local else rng.choice(gws[cp[p]])].append(p)
            for br in rb:
                ps = routed[br]
                if not ps:
                    continue
                touched = {cp[rng.choice(ps)] for _ in range(round(batches * len(ps) * per_rack / p_total))}
                if held[br]:
                    touched.add(min(held[br]))
                tot += len(touched)
                files += 1
    return tot / files


if __name__ == "__main__":
    rng = random.Random(23)
    print("local-log share, parts per file at 24 and 12 brokers")
    for frac in (0.0, 0.01, 0.02, 0.05, 0.10, 1.0):
        print(frac, round(mix_parts_per_file(8, frac, rng), 2), round(mix_parts_per_file(4, frac, rng), 2))
    print()
    print("per_rack B  today_peak gw_peak ratio(means) mean_ratio worst gw_peak/rack_mean")
    for per_rack in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 16, 20, 24):
        r = run(per_rack, 300, rng)
        print(per_rack, 3 * per_rack, f"{r['today_peak']:.1f}", f"{r['gw_peak']:.1f}",
              f"{r['ratio_of_means']:.2f}", f"{r['mean_ratio']:.2f}", f"{r['worst_ratio']:.2f}",
              f"{r['gw_peak_over_rack_mean']:.2f}")
