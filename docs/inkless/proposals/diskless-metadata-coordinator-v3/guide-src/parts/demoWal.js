/* ---------------------------------------------------------------- WAL objects */
const USER_PARTS = ['orders-0', 'orders-1', 'clicks-0', 'clicks-1', 'logs-0', 'logs-1'];
const PART_COLORS = ['#2563eb', '#db2777', '#0d9488', '#ea580c', '#7c3aed', '#ca8a04'];

function demoWal() {
  const host = $('#wal-brokers');
  if (!host) return;
  const brokers = ['Broker 1 · zone A', 'Broker 2 · zone B', 'Broker 3 · zone C'];
  let buffers, objects, rounds, lastUpload;
  const rnd = n => Math.floor(Math.random() * n);
  const hex = () => Array.from({ length: 8 }, () => '0123456789abcdef'[rnd(16)]).join('');

  const legend = h('div', { class: 'small muted', style: { margin: '0 0 0.6rem', display: 'flex', flexWrap: 'wrap', gap: '0.7rem' } },
    USER_PARTS.map((p, i) => h('span', { style: { display: 'inline-flex', gap: '0.3rem', alignItems: 'center' } },
      h('span', { class: 'dot', style: { background: PART_COLORS[i] } }), p)));
  host.before(legend);

  const reset = () => { buffers = [[], [], []]; objects = []; rounds = 0; lastUpload = null; render(); show($('#wal-readout'), '', 'Nothing uploaded yet.'); };

  const render = () => {
    host.innerHTML = '';
    buffers.forEach((buf, b) => {
      const cells = buf.map(x => h('span', { class: 'cell', style: { background: PART_COLORS[x.p], padding: '0 0.3rem' }, title: `${USER_PARTS[x.p]}, ${x.kb} KB` }, x.kb + 'K'));
      host.append(h('div', { class: 'panel' },
        h('h6', null, brokers[b], h('span', { class: 'chip' }, buf.length + ' batches')),
        buf.length ? h('div', { class: 'log' }, cells) : h('div', { class: 'small muted' }, 'Buffer empty')));
    });
    const st = $('#wal-storage');
    st.innerHTML = '';
    if (!objects.length) { st.append(h('span', { class: 'muted small' }, 'Empty')); return; }
    for (const o of objects) {
      const total = o.batches.reduce((a, x) => a + x.kb, 0);
      const parts = new Set(o.batches.map(x => x.p));
      st.append(h('div', { class: 'panel fadein', style: { padding: '0.45rem 0.6rem' } },
        h('div', { class: 'mono' }, o.key),
        h('div', { class: 'objbox', style: { width: Math.max(80, total * 1.6) + 'px', margin: '0.3rem 0' } },
          o.batches.map(x => h('span', { style: { width: (x.kb / total) * 100 + '%', background: PART_COLORS[x.p] }, title: `${USER_PARTS[x.p]}, ${x.kb} KB` }))),
        h('div', { class: 'small muted' }, `from broker ${o.broker + 1} · ${parts.size} partitions · ${total} KB`)));
    }
  };

  $('#wal-produce').addEventListener('click', () => {
    rounds++;
    buffers.forEach(buf => {
      const n = 2 + rnd(2);
      for (let i = 0; i < n; i++) buf.push({ p: rnd(USER_PARTS.length), kb: 4 + rnd(40) });
    });
    render();
    show($('#wal-readout'), '', `Round ${rounds}: every broker buffered batches for whatever partitions its producers sent. Nothing is in object storage yet, so no producer has an answer.`);
  });

  $('#wal-upload').addEventListener('click', () => {
    const fresh = [];
    buffers.forEach((buf, b) => {
      if (!buf.length) return;
      const batches = buf.slice().sort((x, y) => x.p - y.p);
      const o = { key: hex() + '-' + hex(), broker: b, batches };
      objects.push(o);
      fresh.push(o);
    });
    buffers = [[], [], []];
    render();
    if (!fresh.length) { show($('#wal-readout'), 'warn', 'All buffers are empty. Produce a round first.'); return; }
    const seen = new Map();
    for (const o of fresh) for (const x of new Set(o.batches.map(y => y.p))) seen.set(x, (seen.get(x) || 0) + 1);
    const shared = [...seen.entries()].filter(([, c]) => c > 1).map(([p]) => USER_PARTS[p]);
    const avg = fresh.reduce((a, o) => a + new Set(o.batches.map(x => x.p)).size, 0) / fresh.length;
    show($('#wal-readout'), '',
      `${fresh.length} objects uploaded, with ${avg.toFixed(1)} partitions per object on average. ` +
      `Each batch knows its partition and its byte range in the object, but <b>no batch has an offset</b>. ` +
      (shared.length
        ? `Batches for <b>${shared.join(', ')}</b> landed in more than one object at the same moment. Nothing in the objects says which comes first.`
        : `Produce a few more rounds: soon two brokers write the same partition at once, and nothing in the objects says which batch comes first.`));
  });
  $('#wal-reset').addEventListener('click', reset);
  reset();
}
