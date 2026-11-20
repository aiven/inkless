"""Relabels the reused demos: v2 and v3 labels become the obvious rule and the design."""
import pathlib
import re

P = pathlib.Path(__file__).parent / "parts"

EDITS = [
    # Chapter references.
    ("demoGuard.js", "as chapter 8 shows.", "as the durability chapter shows."),
    ("demoApply.js", "Chapter 9 shows how snapshots keep this replay short.", "The snapshot chapter shows how snapshots keep this replay short."),
    ("demoScan.js", "as chapter 15 shows.", "as the rejected-alternatives chapter shows."),
    ("demoSeq.js", "Chapter 23 shows why", "The chapter on reads shows why"),

    # Broker row: retiredThrough.
    ("demo-row.html", "<td>The row is retired, or <code>e</code> is older than the row's epoch</td>",
     "<td><code>e</code> is at or below the row's <code>retiredThrough</code>, or older than the epoch of the last commit</td>"),
    ("demoRow.js", ": `<li>broker epoch retired: rows move to epoch ${o.e}</li>`",
     ": `<li>broker epoch retired: <code>retiredThrough</code> ${o.e - 1}</li>`"),
    ("demoRow.js",
     "Once cp1's leader sees that in its metadata, it appends a broker-epoch-retired record. When it applies, the row moves to epoch ${st.curE}, and every replica rejects epoch ${st.curE - 1} from the same log position. Sequences start over in the new epoch.",
     "Once cp1's leader sees the new registration in its metadata image, it appends a broker-epoch-retired record with <code>retiredThrough</code> ${st.curE - 1}. When it applies, every replica rejects epoch ${st.curE - 1} and older from the same log position. Sequences start over in the new epoch."),

    # Finish reports demo.
    ("demo-v3-empty.html", "Compare v2 with the two v3 outcomes: cp1 recovers within the deadline, or cp1 stays unavailable past it.",
     "Compare the obvious rule, which fences participants it hasn't heard from, with the design's two outcomes: cp1 recovers within the deadline, or cp1 stays unavailable past it."),
    ("demo-v3-empty.html", ">V2 behavior<", ">Obvious rule: fence the silent<"),
    ("demo-v3-empty.html", ">V3: cp1 fails over, then commits<", ">Finish reports: cp1 fails over, then commits<"),
    ("demo-v3-empty.html", ">V3: cp1 misses the deadline<", ">Finish reports: cp1 misses the deadline<"),
    ("demoV3Empty.js", "That release no longer triggers fences.", "That release triggers no fence."),
    ("demoV3Empty.js", "V2 now sees cp0 and cp1 as silent participants and sends a fence to both.",
     "Cp2 hasn't heard from cp0 or cp1, and every partition it has heard from is released, so the obvious rule fences both."),
    ("demoV3Empty.js", "'V2 path: the delayed cp1 part gets stale-sequence, so the produce fails.' : 'V2 path: step through the empty tracking part and the failover.'",
     "'Obvious rule: the delayed cp1 part gets stale-sequence, so the produce fails although nothing was wrong with it.' : 'Obvious rule: step through the empty tracking part and the failover.'"),
    ("demoV3Empty.js", "'V3 path: the produce succeeded, and cp2 sent no fence. Ticket 01\\'s named test checks exactly this.' : 'V3 path: the tracking partition waits for a finish report before it decides anything.'",
     "'Finish reports: the produce succeeded, and cp2 sent no fence. A named test checks exactly this.' : 'Finish reports: the tracking partition waits for a finish report before it decides anything.'"),
    ("demoV3Empty.js", "'V3 path: the only fence went to the participant that the broker reported as an exception, after the file finished. It answered released.'",
     "'Finish reports: the only fence went to the participant that the broker reported as an exception, after the file finished. It answered released.'"),
    ("demoV3Empty.js", "'V3 path: cp1\\'s batches failed at the deadline, as any unconfirmed part does. A fence comes only after the finish report.'",
     "'Finish reports: cp1\\'s batches failed at the deadline, as any unconfirmed part does. A fence comes only after the finish report.'"),
    ("demoV3Empty.js", ": 'V3 path: the tracking partition waits for a finish report before it decides anything.';",
     ": 'Finish reports: the tracking partition waits for a finish report before it decides anything.';"),
    ("demoV3Empty.js", "chip('info', 'v2 behavior') : chip('good', 'v3 decision')", "chip('bad', 'obvious rule') : chip('good', 'the design')"),

    # Release entry demo.
    ("demo-v3-entry.html", "Both models receive the same events. V2 has no finish report, so those events do nothing on the left.",
     "Both trackers receive the same events. The naive tracker on the left stores an entry only after the tracking part, and lets each answer overwrite the last. It ignores finish reports."),
    ("demoV3Entry.js", "'V2 has no finish report, so this event does nothing.'", "'The naive tracker ignores finish reports, so this event does nothing.'"),
    ("demoV3Entry.js", "'No entry exists, so v2 drops the report. Cp1 appends release-accepted and never resends.'",
     "'No entry exists, so the naive tracker drops the report. Cp1 appends release-accepted and never resends.'"),
    ("demoV3Entry.js", "<h6>V2 behavior ${", "<h6>Naive tracker ${"),
    ("demoV3Entry.js", "<h6>V3 decision ${", "<h6>The design ${"),
    ("demoV3Entry.js", "'In v3, cp1 is pending", "'Cp1 is pending"),
    ("demoV3Entry.js", "'In v3, a fence is out", "'A fence is out"),
    ("demoV3Entry.js", "'In v3, cp1 holds", "'Cp1 holds"),
    ("demoV3Entry.js", "'In v3, cp1 is released", "'Cp1 is released"),
    ("demoV3Entry.js", "V2 leaks this object:", "The naive tracker leaks this object:"),

    # Fence storm demo.
    ("demo-v3-storm.html", "under the v3 fence rules before batching", "under the three fence rules before batching"),
    ("demo-v3-storm.html", " The formulas come from ticket 06.", ""),

    # Per-request completion demo.
    ("demo-v3-slowest.html", "compare v2's whole-file completion with v3's per-request completion.",
     "compare completing every request with the whole file against completing each request with its own parts."),
    ("demo-v3-slowest.html", ">V2: whole file<", ">Complete with the whole file<"),
    ("demo-v3-slowest.html", ">V3: per request<", ">Complete per request (the design)<"),
    ("demoV3Slowest.js", "`V2: every request completes", "`Whole-file completion: every request completes"),
    ("demoV3Slowest.js", "`V3: request A completes", "`Per-request completion: request A completes"),

    # Group apply demo.
    ("demo-v3-apply.html", "then compare v2's transaction per record with v3's group.", "then compare one transaction per record with the design's groups."),
    ("demo-v3-apply.html", ">V2: transaction per record<", ">One transaction per record<"),
    ("demo-v3-apply.html", ">V3: groups with savepoints<", ">Groups with savepoints (the design)<"),
    ("demoV3Apply.js", "? `V3 applies ${end}", "? `Group apply covers ${end}"),
    ("demoV3Apply.js", ": `V2 applies ${end}", ": `Per-record apply covers ${end}"),

    # Blast radius demo.
    ("demoV3Blast.js", "`<h6>V3 ${chip('warn', 'accepted risk · ticket 11')}</h6><p class=\"small\" style=\"margin:0\"><b>${fmt(affected)}</b> partitions go offline until an empty restart, whose rules are open in ticket 19. They restart empty from offset 0. Objects the lost partition tracked leak, and no other partition's data is deleted.</p>`",
     "`<h6>Coordinator ${chip('warn', 'accepted risk')}</h6><p class=\"small\" style=\"margin:0\"><b>${fmt(affected)}</b> partitions can't be written or read. The coordinator partition stays leaderless, and the design has no procedure that brings it back. Fences to it go unanswered and releases from it never arrive, so the objects it touched leak instead of being deleted. No other partition's data is touched.</p>`"),
    ("demoV3Blast.js", "`<h6>V3 ${chip('bad', 'outside the bar')}</h6>", "`<h6>Coordinator ${chip('bad', 'outside the bar')}</h6>"),
    ("demoV3Blast.js", "chip('info', 'future work · ticket 09')", "chip('info', 'future work')"),
    ("demoV3Blast.js", "with no acknowledged record lost. V3 writes no footer.", "with no acknowledged record lost. This design writes no footer."),

    # Image demo.
    ("demoV3Image.js", "'The image shows no registration for broker 4, so v2 appends the unregistered form of broker-epoch-retired. The marker has no epoch, and broker 4 is rejected for good, although it is live at epoch 120.'",
     "'The image shows no registration for broker 4, so the naive rule retires broker 4 with no epoch attached. Broker 4 is rejected for good, although it is live at epoch 120.'"),
    ("demoV3Image.js", "'The image shows no registration, so v2 appends the retired marker. When broker 4 returns at epoch 480, the marker rejects it as stale epoch, and it stays rejected.'",
     "'The image shows no registration, so the naive rule retires broker 4 with no epoch attached. When broker 4 returns at epoch 480, it is rejected as a stale epoch, and it stays rejected.'"),
    ("demoV3Image.js", "'The image shows epoch 480, so v2 moves the row to the new epoch, and commits at 480 pass. V2 fails only when it acted on an image from the unregistered window.'",
     "'The image shows epoch 480, so the naive rule moves the row to the new epoch, and commits at 480 pass. It fails only when it acted on an image from the unregistered window.'"),
    ("demoV3Image.js", "'The image has no registration for broker 7, so v2 appends the unregistered form of broker-epoch-retired. Broker 7 is live and has already committed here, but it is rejected for good.'",
     "'The image has no registration for broker 7, so the naive rule retires it. Broker 7 is live and has already committed here, but it is rejected for good.'"),
    ("demoV3Image.js", "'The image doesn\\'t contain the topic, so v2 appends topic-deleted.", "'The image doesn\\'t contain the topic, so the naive rule appends topic-deleted."),
    ("demoV3Image.js", "'The image is past the deletion, and v2 appends topic-deleted, which is correct here.'", "'The image is past the deletion, and the naive rule appends topic-deleted, which is correct here.'"),
    ("demoV3Image.js", "panel('V2 behavior', r.v2)", "panel('Naive rule: absent means gone', r.v2)"),
    ("demoV3Image.js", "panel('V3 decision', r.v3)", "panel('The design: compare offsets', r.v3)"),
    ("demoV3Image.js", "'V2 reads absence from a lagging image as retirement or deletion. V3 compares the image offset with the offset the row carries, so it waits instead.'",
     "'The naive rule reads absence from a lagging image as retirement or deletion. The design compares the image offset with the offset the row carries, so it waits instead.'"),
    ("demoV3Image.js", "both versions agree.", "both rules agree."),

    # Clock skew demo, used in rejected alternatives.
    ("demoSkew.js", "`<b>Owner approach.</b> ${v1}`", "`<b>Intent with tombstones (rejected).</b> ${v1}`"),
]

for name, old, new in EDITS:
    f = P / name
    text = f.read_text()
    assert old in text, f"{name}: missing {old[:70]!r}"
    f.write_text(text.replace(old, new))

for f in P.glob("demo*.js"):
    text = f.read_text()
    f.write_text(re.sub(r"^(/\* -+ )(?:chapter \d+|part 7): ", r"\1", text, count=1, flags=re.M))
print("patched", len(EDITS))
