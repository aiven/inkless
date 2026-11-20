"""Simulation for ticket 22: the tracking partition under gateways.

Ports Kafka's Utils.murmur2 and the capped rendezvous rule from ticket 21, then
compares tracking-partition choices by commit parts per file and by how often a
broker has to switch its tracking partition when the assignment changes.
"""
import math
import random
import struct
import sys
from collections import Counter


def murmur2(data: bytes) -> int:
    length = len(data)
    seed = 0x9747B28C
    m = 0x5BD1E995
    r = 24
    mask = 0xFFFFFFFF
    h = (seed ^ length) & mask
    length4 = length >> 2
    for i in range(length4):
        k = struct.unpack_from("<I", data, i * 4)[0]
        k = (k * m) & mask
        k ^= k >> r
        k = (k * m) & mask
        h = (h * m) & mask
        h ^= k
    index = length4 << 2
    rem = length - index
    if rem == 3:
        h ^= data[index + 2] << 16
    if rem >= 2:
        h ^= data[index + 1] << 8
    if rem >= 1:
        h ^= data[index]
        h = (h * m) & mask
    h ^= h >> 13
    h = (h * m) & mask
    h ^= h >> 15
    return h  # unsigned 32-bit view


def to_positive(x: int) -> int:
    return x & 0x7FFFFFFF


def be32(x: int) -> bytes:
    return struct.pack(">i", x)


def score(cp: int, broker: int) -> int:
    return to_positive(murmur2(be32(cp) + be32(broker)))


def v2_tracking(broker: int, n: int) -> int:
    return to_positive(murmur2(be32(broker))) % n


def assign_rack(brokers, n):
    """Capped rendezvous walk for one rack. Returns {broker: set(cp)}."""
    b = len(brokers)
    g = max(1, b // n)
    cap = math.ceil(n * g / b)
    pairs = sorted(((score(cp, br), cp, br) for cp in range(n) for br in brokers),
                   key=lambda t: (-t[0], t[2]))
    need = {cp: g for cp in range(n)}
    held = {br: set() for br in brokers}
    for _, cp, br in pairs:
        if need[cp] > 0 and len(held[br]) < cap:
            held[br].add(cp)
            need[cp] -= 1
    return held


def best_gateway_cp(broker, gw):
    return max(gw, key=lambda cp: (score(cp, broker), -cp))


def standin_cp(broker, brokers, n):
    """The cp for which a spare ranks highest among the rack's spares."""
    return max(range(n), key=lambda cp: (score(cp, broker), -cp))


def tracking(broker, gw, prev, policy, n, brokers):
    if policy == "v2":
        return v2_tracking(broker, n)
    if gw:
        if policy == "sticky" and prev is not None and prev in gw:
            return prev
        return best_gateway_cp(broker, gw)
    # spare
    return standin_cp(broker, brokers, n)


def parts_per_file(gw, t):
    return len(gw) + (0 if t in gw else 1)


def example(n=8, racks=3, per_rack=8, show=True):
    ids = list(range(1, racks * per_rack + 1))
    rack_of = {i: (i - 1) % racks for i in ids}
    out = []
    for r in range(racks):
        rb = [i for i in ids if rack_of[i] == r]
        held = assign_rack(rb, n)
        for br in rb:
            gw = held[br]
            t2 = v2_tracking(br, n)
            tb = tracking(br, gw, None, "hi", n, rb)
            out.append((r, br, sorted(gw), t2, parts_per_file(gw, t2) if gw else None,
                        tb, parts_per_file(gw, tb) if gw else None))
    if show:
        print(f"Example: N={n}, {racks} racks, {per_rack} per rack, IDs 1..{len(ids)}")
        print("rack broker gateway-cps v2-t parts  B-t parts")
        for row in out:
            print(*row)
    return out


def averages(n, racks, per_rack, trials, rng):
    tot = Counter()
    for _ in range(trials):
        ids = rng.sample(range(0, 1000), racks * per_rack)
        for r in range(racks):
            rb = ids[r * per_rack:(r + 1) * per_rack]
            held = assign_rack(rb, n)
            for br in rb:
                gw = held[br]
                if not gw:
                    tot["spares"] += 1
                    continue
                tot["gws"] += 1
                tot["v2_parts"] += parts_per_file(gw, v2_tracking(br, n))
                tot["b_parts"] += parts_per_file(gw, best_gateway_cp(br, gw))
                tot["k"] += len(gw)
    g = tot["gws"]
    return {
        "k": tot["k"] / g,
        "v2": tot["v2_parts"] / g,
        "b": tot["b_parts"] / g,
        "spares_per_rack": tot["spares"] / (trials * racks),
    }


def switches(n, per_rack, trials, rng, policy):
    """Brokers other than the event broker whose tracking partition changes,
    per event, in one rack. Events: fence of a random broker, its return, a join."""
    res = Counter()
    for _ in range(trials):
        rb = rng.sample(range(0, 1000), per_rack)
        held = assign_rack(rb, n)
        t = {br: tracking(br, held[br], None, policy, n, rb) for br in rb}
        # fence and return of one broker
        x = rng.choice(rb)
        rest = [b for b in rb if b != x]
        held2 = assign_rack(rest, n)
        t2 = {br: tracking(br, held2[br], t[br], policy, n, rest) for br in rest}
        res["fence"] += sum(1 for br in rest if t2[br] != t[br])
        held3 = assign_rack(rb, n)
        t3 = {br: tracking(br, held3[br], t2.get(br), policy, n, rb) for br in rest}
        res["return"] += sum(1 for br in rest if t3[br] != t2[br])
        # join of a new broker
        newb = rng.choice([i for i in range(1000, 2000)])
        joined = rb + [newb]
        held4 = assign_rack(joined, n)
        t4 = {br: tracking(br, held4[br], t[br], policy, n, joined) for br in rb}
        res["join"] += sum(1 for br in rb if t4[br] != t[br])
    return {k: v / trials for k, v in res.items()}


def tracking_load(n, racks, per_rack, trials, rng, policy):
    worst = Counter()
    for _ in range(trials):
        ids = rng.sample(range(0, 1000), racks * per_rack)
        load = Counter()
        for r in range(racks):
            rb = ids[r * per_rack:(r + 1) * per_rack]
            held = assign_rack(rb, n)
            for br in rb:
                load[tracking(br, held[br], None, policy, n, rb)] += 1
        worst[max(load.values()) if load else 0] += 1
    mean_max = sum(k * v for k, v in worst.items()) / trials
    return mean_max


if __name__ == "__main__":
    rng = random.Random(22)
    n = 8
    example(n, 3, 8)
    print()
    example(n, 3, 1)
    print()
    example(n, 3, 4)
    print()
    print("per_rack B  k(cps/gw)  parts_v2  parts_B  spares/rack")
    for per_rack in (1, 4, 8, 10, 15, 16):
        a = averages(n, 3, per_rack, 300, rng)
        print(per_rack, 3 * per_rack, f"{a['k']:.2f}", f"{a['v2']:.3f}", f"{a['b']:.3f}",
              f"{a['spares_per_rack']:.1f}")
    print()
    print("Switches of other brokers per event in one rack (N=8):")
    for per_rack in (2, 3, 4, 7, 8, 9, 10, 15):
        for pol in ("hi", "sticky"):
            s = switches(n, per_rack, 2000, rng, pol)
            print(per_rack, pol, {k: round(v, 2) for k, v in s.items()})
    print()
    print("Mean busiest tracking partition (brokers tracked there), 3 racks:")
    for per_rack in (1, 4, 8, 15):
        for pol in ("v2", "hi"):
            print(per_rack, pol, round(tracking_load(n, 3, per_rack, 500, rng, pol), 2))
