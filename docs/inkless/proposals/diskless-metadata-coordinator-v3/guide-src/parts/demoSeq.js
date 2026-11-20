/* ---------------------------------------------------------------- the sequencer */
function demoSeq() {
  const btnHost = $('#seq-buttons');
  if (!btnHost) return;
  const pending = [
    { b: 1, key: '7c1e5a90', n: 10, bytes: [4096, 8191] },
    { b: 2, key: '19ab04d2', n: 5, bytes: [0, 2047] },
    { b: 3, key: 'e4d09f17', n: 8, bytes: [12288, 15359] },
  ];
  let committed = [];
  let next = 0;
  const buttons = pending.map(p => {
    const btn = h('button', { class: 'primary' }, `Commit broker ${p.b}'s batch (${p.n} records)`);
    btn.addEventListener('click', () => {
      committed.push({ ...p, from: next, to: next + p.n - 1 });
      next += p.n;
      btn.disabled = true;
      render();
      show($('#seq-readout'), '', `The coordinator gave broker ${p.b}'s batch offsets ${next - p.n} to ${next - 1}, because it was commit number ${committed.length}. The next free offset is ${next}.`);
    });
    btnHost.append(btn);
    return btn;
  });
  const render = () => {
    const tb = $('#seq-table');
    tb.innerHTML = '';
    if (!committed.length) {
      tb.innerHTML = '<tr><td colspan="4" class="muted">No batches committed yet. The partition is empty.</td></tr>';
      return;
    }
    for (const c of committed) {
      tb.append(h('tr', { class: 'fadein' },
        h('td', { class: 'mono' }, `${c.from}–${c.to}`), h('td', { class: 'mono' }, c.key),
        h('td', { class: 'mono' }, `${fmt(c.bytes[0])}–${fmt(c.bytes[1])}`), h('td', null, 'Broker ' + c.b)));
    }
  };
  $('#seq-find').addEventListener('click', () => {
    const off = Math.floor(+$('#seq-offset').value);
    const out = $('#seq-readout');
    if (!(off >= 0)) { show(out, 'warn', 'Offsets start at 0.'); return; }
    const hit = committed.find(c => off >= c.from && off <= c.to);
    if (hit) {
      show(out, 'good', `Offset ${off} is in the batch with offsets ${hit.from}–${hit.to}, in object <code>${hit.key}</code> at bytes ${fmt(hit.bytes[0])}–${fmt(hit.bytes[1])}. ` +
        `The broker reads that byte range from storage and sets offsets ${hit.from} to ${hit.to} on the records before it sends them to the consumer. The object itself never contained them.`);
    } else if (off === next) {
      show(out, '', `Offset ${off} is the next offset to be written. The consumer's fetch waits until a new batch commits.`);
    } else {
      show(out, 'warn', `<code>OFFSET_OUT_OF_RANGE</code>: offset ${off} doesn't exist yet. The partition ends at offset ${next}. The chapter on reads shows why a coordinator must be careful before it gives this answer.`);
    }
  });
  $('#seq-reset').addEventListener('click', () => {
    committed = []; next = 0; buttons.forEach(b => { b.disabled = false; }); render();
    show($('#seq-readout'), '', 'Commit a batch, then look up an offset.');
  });
  render();
}
