/* ---------------------------------------------------------------- hash mapping */
function demoHash() {
  const tb = $('#hs-table');
  if (!tb) return;
  const topicEl = $('#hs-topic'), partsEl = $('#hs-parts'), nEl = $('#hs-n');
  const keyBytes = (topic, p) => {
    const name = new TextEncoder().encode(topic);
    const out = new Uint8Array(name.length + 4);
    out.set(name, 0);
    new DataView(out.buffer).setInt32(name.length, p, false);
    return out;
  };
  const render = () => {
    const topic = topicEl.value || 'orders';
    const count = Math.max(1, Math.min(64, Math.floor(+partsEl.value) || 1));
    const n = +nEl.value;
    tb.innerHTML = '';
    let moved = 0;
    const load = new Array(n).fill(0);
    for (let p = 0; p < count; p++) {
      const hv = toPositive(murmur2(keyBytes(topic, p)));
      const slot = hv % 4096, cp = slot % n, cp2 = slot % (2 * n);
      load[cp]++;
      const moves = cp2 !== cp;
      if (moves) moved++;
      tb.append(h('tr', { style: moves ? { background: 'var(--warn-soft)' } : null },
        h('td', null, `${topic}-${p}`), h('td', { class: 'mono' }, fmt(hv)), h('td', { class: 'mono' }, slot),
        h('td', { html: cpChip(cp) }), h('td', { html: cpChip(cp2) + (moves ? ` <span class="small warn-text">moves from cp${cp} to cp${cp2}</span>` : ' <span class="small muted">stays</span>') })));
    }
    $('#hs-double').disabled = n >= 16;
    show($('#hs-readout'), '',
      `At N = ${n}, the ${count} partitions spread as ${load.map((c, i) => `cp${i}: ${c}`).join(', ')}. ` +
      `If N doubled to ${2 * n}, <b>${moved} of ${count}</b> would move, each from cp<i>p</i> to cp<i>p</i>+${n}, and the rest would stay. ` +
      `No partition moves anywhere else, so one coordinator partition hands half of its work to exactly one new partition.`);
  };
  [topicEl, partsEl].forEach(el => el.addEventListener('input', render));
  nEl.addEventListener('change', render);
  $('#hs-double').addEventListener('click', () => {
    const next = String(+nEl.value * 2);
    if ([...nEl.options].some(o => o.value === next || o.textContent === next)) { nEl.value = next; render(); }
  });
  render();
}
