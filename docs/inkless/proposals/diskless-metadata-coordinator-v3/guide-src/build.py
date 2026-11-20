"""Assembles guide.html from the head, the part files, the reused demos, and the scripts."""
import pathlib
import re

SRC = pathlib.Path(__file__).parent
PARTS = SRC / "parts"
OUT = SRC.parent / "guide.html"

BODY = ["10-part1.html", "20-part2.html", "30-part3.html", "40-part4.html", "50-part5.html", "60-part6.html"]
REUSED_JS = ["demoXzone", "demoWal", "demoSeq", "demoApply", "demoOverlay", "demoFlush", "demoSnap", "demoHash",
             "demoFanout", "demoV3Slowest", "demoV3Apply", "demoCrash", "demoScan", "demoRow", "demoKey", "demoRace",
             "demoV3Empty", "demoV3Storm", "demoV3Entry", "demoBarrier", "demoGuard", "demoV3Rack", "demoElect",
             "demoSkip", "demoV3Image", "demoV3Blast", "demoSkew"]


def demo(m):
    return (PARTS / f"demo-{m.group(1)}.html").read_text().rstrip()


head = (SRC / "00-head.html").read_text()
head = head.replace("{{css}}", (PARTS / "style.css").read_text() + (SRC / "extra.css").read_text())
body = "\n".join(re.sub(r"\{\{demo:([\w-]+)\}\}", demo, (SRC / f).read_text()) for f in BODY)
missing = re.findall(r"\{\{[^}]+\}\}", body)
assert not missing, missing

js = [(PARTS / "helpers.js").read_text()]
js += [(PARTS / f"{name}.js").read_text() for name in REUSED_JS]
js.append((SRC / "new.js").read_text())
starts = ["buildToc"] + REUSED_JS + re.findall(r"^function (demo\w+|buildGlossary)\(", js[-1], flags=re.M)
js.append("/* ---------------------------------------------------------------- start */\n"
          f"[{', '.join(starts)}].forEach(fn => {{\n"
          "  try { fn(); } catch (err) { console.error(fn.name, err); }\n});\n")

OUT.write_text(head + body + (SRC / "99-tail.html").read_text().replace("{{js}}", "\n".join(js)))
print(OUT, len(OUT.read_text().splitlines()), "lines")
