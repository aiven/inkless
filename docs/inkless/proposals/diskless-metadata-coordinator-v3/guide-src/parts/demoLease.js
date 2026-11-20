/* ---------------------------------------------------------------- the lease meter */
function demoLease() {
  const view = $('#ls-view');
  if (!view) return;
  const K = 16, IDLE = 6;
  const playBtn = $('#ls-play'), downBtn = $('#ls-down'), idleBtn = $('#ls-idle');
  let st, timer = null;
  const log = msg => { st.events.unshift(`tick ${st.tick}: ${msg}`); st.events = st.events.slice(0, 6); };
  const reset = () => {
    st = { tick: 0, leases: [{ from: 0, to: K - 1, at: 2, state: 'active' }], used: [], failed: 0, next: 0, pending: null, down: downBtn.classList.contains('on'), idle: idleBtn.classList.contains('on'), idleFor: 0, events: [], queued: [] };
    log('cp2 leased [0, 15] to broker 3 at epoch 7.');
    render('Broker 3 holds the lease [0, 15]. Each tick closes one file.');
  };
  const active = () => st.leases.filter(l => l.state === 'active');
  const remaining = () => active().reduce((a, l) => a + Math.max(0, l.to - Math.max(l.from, st.next) + 1), 0);
  const maxTo = () => Math.max(...st.leases.map(l => l.to));
  const tracking = () => (st.down ? 3 : 2);
  const requestLease = why => {
    const from = st.leases.some(l => l.state === 'active') ? maxTo() + 1 : Math.max(st.next, maxTo() + 1);
    st.pending = { from, to: from + K - 1, at: tracking(), since: st.tick, why };
    log(`asked cp${tracking()} for [${from}, ${from + K - 1}]${st.down ? ' (cp2 is down, so the broker falls back to cp3)' : ''}.`);
  };
  const tick = () => {
    st.tick++;
    let msg = '';
    if (st.pending && st.pending.since < st.tick) {
      const p = st.pending;
      st.leases.push({ from: p.from, to: p.to, at: p.at, state: 'active' });
      if (!active().some(l => l.from <= st.next && st.next <= l.to)) st.next = p.from;
      log(`cp${p.at} appended sequences-leased [${p.from}, ${p.to}].`);
      st.pending = null;
    }
    if (st.idle) {
      st.idleFor++;
      msg = `The broker closed no file (idle for ${st.idleFor} tick${st.idleFor === 1 ? '' : 's'}).`;
      if (st.idleFor === IDLE && active().length) {
        const left = [];
        for (const l of active()) {
          const a = Math.max(l.from, st.next);
          if (a <= l.to) left.push([a, l.to, l.at]);
          l.state = 'ended';
        }
        for (const [a, b] of left) for (let s = a; s <= b; s++) st.queued.push(s);
        st.next = maxTo() + 1;
        log(`idle timeout: the lease ended. Leftovers ${left.map(([a, b]) => `[${a}, ${b}]`).join(' and ')} were range-fenced at every partition and queued for deletion.`);
        msg = `<b>Idle timeout.</b> The tracking partition heard nothing for ${IDLE} ticks, so it ended the lease. It fenced the unused sequences at every coordinator partition and put their keys in the deletion queue. Deleting keys that were never written is cheap.`;
      }
    } else {
      st.idleFor = 0;
      const lease = active().find(l => l.from <= st.next && st.next <= l.to);
      if (lease) {
        st.used.push(st.next);
        msg = `The broker closed a file and took sequence ${st.next} from the lease. No request was needed.`;
        st.next++;
        if (!active().some(l => l.from <= st.next && st.next <= l.to)) {
          for (const l of active()) if (l.to < st.next) l.state = 'used';
          const later = active().filter(l => l.from >= st.next).sort((x, y) => x.from - y.from)[0];
          if (later) st.next = later.from;
        }
      } else {
        st.failed++;
        msg = '<b>No leased sequence.</b> The file fails at once with a retriable error and is never uploaded, so nothing unrecorded reaches storage. The broker asks for a new lease.';
        log('a file failed without upload: no leased sequence.');
      }
      if (!st.pending && remaining() < K / 2) requestLease('renew');
    }
    render(msg);
  };
  const render = msg => {
    view.innerHTML = '';
    const rows = st.leases.slice(-3).map(l => {
      const cells = [];
      for (let s = l.from; s <= l.to; s++) {
        let bg = 'var(--soft)', ink = 'var(--muted)', cls = 'cell', title = 'leased, unused';
        if (st.used.includes(s)) { bg = 'var(--accent)'; ink = '#fff'; title = 'file uploaded'; }
        else if (st.queued.includes(s)) { bg = 'var(--warn)'; ink = '#fff'; title = 'leftover: fenced and queued'; }
        else if (s === st.next && l.state === 'active') { bg = 'color-mix(in srgb, var(--accent) 25%, transparent)'; ink = 'var(--ink)'; title = 'next sequence'; }
        cells.push(h('span', { class: cls, style: { background: bg, color: ink, minWidth: '1.6rem' }, title }, s));
      }
      return h('div', { class: 'bar-row', style: { gridTemplateColumns: '10rem 1fr' } },
        h('span', { class: 'small', html: `[${l.from}, ${l.to}] from ${cpChip(l.at)} <span class="muted">${l.state}</span>` }),
        h('div', { class: 'log' }, cells));
    });
    view.append(...rows);
    if (st.pending) view.append(h('div', { class: 'small muted' }, `Waiting for cp${st.pending.at} to grant [${st.pending.from}, ${st.pending.to}].`));
    view.append(h('div', { class: 'small', style: { marginTop: '0.5rem' } },
      h('b', null, 'Recent events'), h('ol', { class: 'small mono', style: { margin: '0.2rem 0 0', paddingLeft: '1.1rem' } }, st.events.map(e => h('li', null, e)))));
    show($('#ls-readout'), /No leased|Idle timeout/.test(msg || '') ? 'warn' : '',
      `${msg || ''} <span class="muted">Remaining in active leases: ${remaining()}. Files uploaded: ${st.used.length}. Files failed without upload: ${st.failed}. Keys queued for deletion: ${st.queued.length}.</span>`);
  };
  $('#ls-tick').addEventListener('click', tick);
  playBtn.addEventListener('click', () => {
    if (timer) { clearInterval(timer); timer = null; playBtn.classList.remove('on'); return; }
    playBtn.classList.add('on');
    timer = setInterval(tick, 800);
  });
  downBtn.addEventListener('click', () => {
    st.down = !st.down;
    downBtn.classList.toggle('on', st.down);
    render(st.down
      ? 'cp2 is down. The broker keeps using its current lease. Its next request goes to cp3, the next partition, so produce keeps working.'
      : 'cp2 is back. New lease requests go to cp2 again.');
  });
  idleBtn.addEventListener('click', () => {
    st.idle = !st.idle;
    idleBtn.classList.toggle('on', st.idle);
    render(st.idle ? `The broker stops closing files. Keep ticking: after ${IDLE} ticks, the tracking partition's idle timeout ends the lease.` : 'The broker is busy again. Tick to close a file.');
  });
  $('#ls-reset').addEventListener('click', () => {
    if (timer) { clearInterval(timer); timer = null; playBtn.classList.remove('on'); }
    reset();
  });
  reset();

  const chart = $('#lat-chart');
  if (chart) {
    const rowsData = [
      ['PostgreSQL', [[250, 'buffer', 'var(--faint)'], [100, 'upload', 'var(--accent)'], [10, 'commit', 'var(--good)']]],
      ['Owner approach', [[250, 'buffer', 'var(--faint)'], [10, 'intent', 'var(--bad)'], [100, 'upload', 'var(--accent)'], [10, 'commit', 'var(--good)']]],
      ['This design', [[250, 'buffer', 'var(--faint)'], [100, 'upload', 'var(--accent)'], [10, 'commit', 'var(--good)']]],
    ];
    const total = 380;
    chart.innerHTML = '';
    for (const [name, segs] of rowsData) {
      let at = 0;
      const track = h('div', { class: 'bar-track' });
      for (const [ms, label, color] of segs) {
        track.append(h('div', { class: 'bar-seg', title: `${label}: ${ms} ms`, style: { left: (at / total) * 100 + '%', width: (ms / total) * 100 + '%', background: color } }, ms >= 50 ? `${label} ${ms} ms` : ''));
        at += ms;
      }
      chart.append(h('div', { class: 'bar-row', style: { gridTemplateColumns: '10rem 1fr' } }, h('span', null, h('b', null, name), h('span', { class: 'muted' }, ` ${at} ms`)), track));
    }
    chart.append(h('figcaption', null, 'Median time from a produce request to its answer. The red slice is the owner approach\'s intent: one replicated append with a disk sync, before the upload can start. This design has the same steps as PostgreSQL.'));
  }
}
