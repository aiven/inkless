/* ---------------------------------------------------------------- flush policy */
function demoFlush() {
  const host = $('#fl-replicas');
  if (!host) return;
  const btnOne = $('#fl-one'), btnDef = $('#fl-default');
  const names = ['Leader', 'Follower 1', 'Follower 2'];
  let st;
  const reset = msg => {
    st = { one: btnOne.classList.contains('on'), log: [], flushed: 0, lost: [], afterLoss: false, n: 0 };
    render();
    show($('#fl-readout'), '', msg || 'Commit a few files.');
  };
  const range = e => `${e.from}–${e.from + 99}`;
  const render = () => {
    host.innerHTML = '';
    for (let r = 0; r < 3; r++) {
      const cells = st.log.map((e, k) => h('span', {
        class: 'cell' + (k >= st.flushed ? ' unflushed' : '') + (e.reuse ? ' reuse' : ''),
        style: { background: e.reuse ? 'var(--warn)' : 'var(--cp0)', padding: '0 0.35rem' },
        title: `commit ${e.n}: orders-0 offsets ${range(e)}, ${k >= st.flushed ? 'in memory only' : 'on disk'}`,
      }, `#${e.n} · ${range(e)}`));
      const lost = st.lost.map(e => h('span', { class: 'cell lost', style: { padding: '0 0.35rem' }, title: 'lost in the power loss' }, `#${e.n} · ${range(e)}`));
      const mem = st.log.length - st.flushed;
      host.append(h('div', { class: 'bar-row', style: { gridTemplateColumns: '6.5rem 1fr auto' } },
        h('span', { class: 'small', style: { fontWeight: 650 } }, names[r]),
        h('div', { class: 'log' }, cells.length || lost.length ? [...cells, ...lost] : h('span', { class: 'small muted' }, 'Log empty')),
        h('span', { class: 'small ' + (mem ? 'bad-text' : 'muted') }, `${st.flushed} on disk, ${mem} in memory only`)));
    }
    $('#fl-osflush').disabled = st.one;
  };
  $('#fl-commit').addEventListener('click', () => {
    const from = st.log.length * 100;
    const victim = st.lost.find(e => e.from === from);
    const e = { n: ++st.n, from, reuse: !!victim };
    st.log.push(e);
    if (st.one) st.flushed = st.log.length;
    render();
    if (victim) {
      show($('#fl-readout'), 'bad',
        `Commit #${e.n} got offsets ${range(e)}, the same offsets that lost commit #${victim.n} had. Brokers that cached #${victim.n}'s coordinates serve its records for offset ${from}. ` +
        `Brokers that ask the coordinator now get #${e.n}'s records. Two different records now sit at one offset, and consumers can't tell which one is real.`);
    } else {
      show($('#fl-readout'), st.one ? 'good' : 'warn', st.one
        ? `Commit #${e.n} is on every replica's disk before the coordinator replies. Its batch for orders-0 got offsets ${range(e)}.`
        : `Commit #${e.n} is replicated and acknowledged, but every replica holds it only in memory until the OS writes it out. Its batch for orders-0 got offsets ${range(e)}.`);
    }
  });
  $('#fl-osflush').addEventListener('click', () => {
    st.flushed = st.log.length;
    render();
    show($('#fl-readout'), '', 'The OS wrote every replica\'s memory to disk. In real life this happens on its own schedule, and the three brokers rarely do it at the same moment.');
  });
  $('#fl-power').addEventListener('click', () => {
    const lostNow = st.log.slice(st.flushed);
    st.lost = st.lost.concat(lostNow);
    st.log = st.log.slice(0, st.flushed);
    render();
    if (!lostNow.length) {
      show($('#fl-readout'), 'good', 'All three replicas restart with every acknowledged commit on disk. Nothing is lost.');
    } else {
      show($('#fl-readout'), 'bad',
        `All three replicas restart, and the log ends at commit #${st.log.length ? st.log[st.log.length - 1].n : '–'}. ` +
        `${lostNow.length} acknowledged commit${lostNow.length === 1 ? ' is' : 's are'} gone, but the objects ${lostNow.length === 1 ? 'it' : 'they'} pointed to are still in storage, and producers were told the data is safe. ` +
        `Press <b>Commit a file</b> to see what the next commit does.`);
    }
  });
  btnOne.addEventListener('click', () => { setOn([btnOne, btnDef], btnOne); reset('<code>flush.messages=1</code>: every replica syncs each batch before it counts as appended.'); });
  btnDef.addEventListener('click', () => { setOn([btnOne, btnDef], btnDef); reset('Classic default: appends go to memory, and the OS writes them to disk later.'); });
  $('#fl-reset').addEventListener('click', () => reset());
  reset();
}
