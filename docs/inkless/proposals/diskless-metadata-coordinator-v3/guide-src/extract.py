"""One-time extraction of reusable CSS, demo markup, and demo functions from the old guide."""
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[3]
OLD = (ROOT / "docs/inkless/proposals/diskless-metadata-coordinator-v3/intro-guide.html").read_text()
OUT = pathlib.Path(__file__).parent / "parts"
OUT.mkdir(exist_ok=True)

css = OLD[OLD.index("<style>") + len("<style>"):OLD.index("</style>")]
(OUT / "style.css").write_text(css.strip() + "\n")


def block(start_pat):
    i = OLD.index(start_pat)
    depth, j = 0, i
    for m in re.finditer(r"<div\b|</div>", OLD[i:]):
        depth += 1 if m.group(0).startswith("<div") else -1
        if depth == 0:
            j = i + m.end()
            break
    return OLD[i:j]


for demo in ["xzone", "wal", "seq", "apply", "overlay", "flush", "snap", "hash", "fanout", "crash", "scan",
             "skew", "row", "key", "lease", "race", "barrier", "guard", "elect", "skip",
             "v3-empty", "v3-image", "v3-entry", "v3-storm", "v3-slowest", "v3-apply", "v3-rack", "v3-blast"]:
    (OUT / f"demo-{demo}.html").write_text(block(f'<div class="demo" id="demo-{demo}">') + "\n")

script = OLD[OLD.index("<script>") + len("<script>"):OLD.rindex("</script>")]
helpers = script[:script.index("/* ---------------------------------------------------------------- chapter 1")]
(OUT / "helpers.js").write_text(helpers.strip() + "\n")

funcs = re.split(r"(?=^/\* -{10,})", script, flags=re.M)
for chunk in funcs:
    m = re.search(r"^function (\w+)\(", chunk, flags=re.M)
    if m:
        (OUT / f"{m.group(1)}.js").write_text(chunk.strip() + "\n")
