/* ---------------------------------------------------------------- clock skew */
function demoSkew() {
  const arrive = $('#sk-arrive');
  if (!arrive) return;
  const skew = $('#sk-skew');
  const AGE = 5, DEL_REC = 0.5, TOMB_END = 10.5, OBJ_DEL = 6, MAX = 20;
  const x = t => 30 + (t / MAX) * 560;
  const render = () => {
    const a = +arrive.value, sk = +skew.value;
    $('#sk-arrive-l').textContent = 'minute ' + a;
    $('#sk-skew-l').textContent = '+' + sk + ' min';
    const ageOk = a - sk <= AGE;
    const ageEnd = Math.min(MAX, AGE + sk);
    let ticks = '';
    for (let t = 0; t <= MAX; t += 2) ticks += `<line x1="${x(t)}" y1="128" x2="${x(t)}" y2="133" style="stroke:var(--faint)"/><text x="${x(t)}" y="146" text-anchor="middle" style="fill:var(--faint);font-size:10px">${t}</text>`;
    $('#sk-svg').innerHTML = `<svg viewBox="0 0 620 156" role="img" aria-label="Timeline of a late commit part">
      <line x1="30" y1="128" x2="590" y2="128" style="stroke:var(--line);stroke-width:2"/>${ticks}
      <rect x="${x(0)}" y="18" width="${x(ageEnd) - x(0)}" height="18" rx="4" style="fill:color-mix(in srgb, var(--warn) 35%, transparent)"/>
      <text x="${x(0) + 6}" y="31" style="fill:var(--ink);font-size:10.5px">leader's age check passes (until minute ${ageEnd})</text>
      <rect x="${x(DEL_REC)}" y="44" width="${x(TOMB_END) - x(DEL_REC)}" height="18" rx="4" style="fill:color-mix(in srgb, var(--good) 35%, transparent)"/>
      <text x="${x(DEL_REC) + 6}" y="57" style="fill:var(--ink);font-size:10.5px">tombstone for the key (minute 0.5 to 10.5)</text>
      <rect x="${x(OBJ_DEL)}" y="70" width="${x(MAX) - x(OBJ_DEL)}" height="18" rx="4" style="fill:color-mix(in srgb, var(--bad) 25%, transparent)"/>
      <text x="${x(OBJ_DEL) + 6}" y="83" style="fill:var(--ink);font-size:10.5px">object deleted from storage (minute 6)</text>
      <circle cx="${x(0)}" cy="112" r="5" style="fill:var(--accent)"/><text x="${x(0) + 8}" y="116" style="fill:var(--muted);font-size:10px">commit</text>
      <line x1="${x(a)}" y1="94" x2="${x(a)}" y2="128" style="stroke:var(--ink);stroke-width:2;stroke-dasharray:3 3"/>
      <circle cx="${x(a)}" cy="112" r="6" style="fill:var(--ink)"/>
      <text x="${Math.min(x(a) + 9, 520)}" y="108" style="fill:var(--ink);font-size:10.5px;font-weight:700">late copy</text>
    </svg>`;
    let v1cls, v1;
    if (a < DEL_REC) { v1cls = 'good'; v1 = 'It arrives before <code>DeleteRecords</code>, while the commit still stands. The leader treats it as a retry of a known key. Harmless.'; }
    else if (a < TOMB_END) { v1cls = 'good'; v1 = `Rejected: the tombstone for the key is still there${ageOk ? '' : ', and the part also fails the age check'}.`; }
    else if (ageOk) { v1cls = 'bad'; v1 = `<b class="bad">Accepted, and data is lost.</b> The tombstone expired at minute 10.5. The age check passes because the uploader's clock is ${sk} minutes ahead: the leader computes an age of ${(a - sk).toFixed(1)} minutes. The batches now point into an object deleted at minute 6.`; }
    else { v1cls = 'good'; v1 = `Rejected by the age check: the leader computes an age of ${(a - sk).toFixed(1)} minutes, more than 5. This holds only while the skew stays below ${(a - AGE).toFixed(1)} minutes.`; }
    show($('#sk-v1'), v1cls, `<b>Intent with tombstones (rejected).</b> ${v1}`);
    show($('#sk-v2'), 'good', '<b>This design.</b> The late copy carries the pair <code>(7, 41)</code>. If broker 3 committed nothing newer there, the pair equals the last commit, so the leader replies with the stored result and adds no batches. Otherwise it\'s below the floor and rejected as stale. The skew plays no part.');
  };
  arrive.addEventListener('input', render);
  skew.addEventListener('input', render);
  render();
}
