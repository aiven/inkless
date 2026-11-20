/* ---------------------------------------------------------------- shared gateway rule */
const be32 = n => [(n >>> 24) & 255, (n >>> 16) & 255, (n >>> 8) & 255, n & 255];
const gwScore = (cp, b) => toPositive(murmur2(be32(cp).concat(be32(b))));
const strBytes = s => Array.from(new TextEncoder().encode(s));
const logKeyPick = (logKey, ids) => ids.slice().sort((x, y) =>
  toPositive(murmur2(strBytes(logKey).concat(be32(y)))) - toPositive(murmur2(strBytes(logKey).concat(be32(x)))) || x - y)[0];

/* Capped rendezvous for one rack. Returns gateways per coordinator partition and slots per broker. */
function assignRack(ids, N, plain) {
  const br = ids.length;
  const gw = Array.from({ length: N }, () => []);
  const load = new Map(ids.map(b => [b, 0]));
  if (!br) return { gw, load, g: 0, cap: 0 };
  const g = Math.max(1, Math.floor(br / N));
  const cap = plain ? Infinity : Math.ceil((N * g) / br);
  const pairs = [];
  for (let cp = 0; cp < N; cp++) for (const b of ids) pairs.push([gwScore(cp, b), cp, b]);
  pairs.sort((x, y) => y[0] - x[0] || x[2] - y[2] || x[1] - y[1]);
  for (const [, cp, b] of pairs) {
    if (gw[cp].length < g && load.get(b) < cap) { gw[cp].push(b); load.set(b, load.get(b) + 1); }
  }
  return { gw, load, g, cap };
}
const gatewaySet = (ids, N, b) => assignRack(ids, N).gw.map((list, cp) => (list.includes(b) ? cp : -1)).filter(cp => cp >= 0);
const byScore = (cps, b) => cps.slice().sort((x, y) => gwScore(y, b) - gwScore(x, b) || x - y);

/* ---------------------------------------------------------------- where should the catalog live */
function demoWhere() {
  const tabs = $('#wh-tabs');
  if (!tabs) return;
  const needs = ['Kafka stays self-sufficient', 'Cluster metadata stays fast', 'Grows with the cluster', 'Any broker can serve any partition'];
  const options = [
    { name: 'An external database', desc: 'Keep a database such as PostgreSQL next to Kafka, as Inkless does today. Brokers call it through <code>ControlPlane</code>.',
      score: [false, true, false, true], cls: 'bad', verdict: 'Rejected by KIP-1164: "To keep Kafka self-sufficient", Apache Kafka can\'t require another system. One primary also carries every commit in the cluster.' },
    { name: 'The cluster\'s KRaft quorum', desc: 'Write batch metadata into <code>__cluster_metadata</code>, next to topics, partitions, and brokers.',
      score: [true, false, false, true], cls: 'bad', verdict: 'Rejected by KIP-1164: it mixes low-throughput cluster metadata with high-throughput batch metadata, which could slow down leadership changes for the whole cluster. Every broker would also replay every commit.' },
    { name: 'A separate KRaft quorum', desc: 'Run a second Raft quorum only for diskless metadata.',
      score: [true, true, false, true], cls: 'bad', verdict: 'Rejected by KIP-1164: it has "no benefit" over a normal Kafka topic. A topic gives the same replicated log, and it can have many partitions, each with its own leader.' },
    { name: 'No coordinator', desc: 'Let each partition\'s leader order its own diskless writes, as KIP-1176 does, or use remote storage itself as the replication medium.',
      score: [true, true, true, false], cls: 'bad', verdict: 'Rejected by KIP-1163: diskless topics separate stateful coordinators from stateless replicas, so any broker can take writes and reads for any partition. Leader-ordered designs tie each partition to its leader again.' },
    { name: 'An internal Kafka topic', desc: 'Store the metadata in a new internal topic, <code>__diskless_metadata</code>, whose partition leaders act as coordinators on <code>CoordinatorRuntime</code>, as the group coordinator does with <code>__consumer_offsets</code>.',
      score: [true, true, true, true], cls: 'good', verdict: 'Chosen by KIP-1164. It reuses machinery Kafka already runs, keeps batch metadata out of the cluster metadata log, and spreads the work over several partition leaders.' },
  ];
  const btns = options.map((o, i) => {
    const b = h('button', { onclick: () => pick(i) }, o.name);
    tabs.append(b);
    return b;
  });
  const pick = i => {
    const o = options[i];
    setOn(btns, btns[i]);
    $('#wh-desc').innerHTML = `<h6>${o.name}</h6><p class="small" style="margin:0">${o.desc}</p>`;
    $('#wh-score').innerHTML = '<h6>What the KIPs ask</h6>' + needs.map((n, k) =>
      `<div class="small">${o.score[k] ? chip('good', 'yes') : chip('bad', 'no')} ${n}</div>`).join('');
    show($('#wh-readout'), o.cls, o.verdict);
  };
  pick(0);
}

/* ---------------------------------------------------------------- the data-loss bar */
function demoBar() {
  const sel = $('#bar-fail');
  if (!sel) return;
  const cases = [
    { name: 'One broker crashes', classic: ['good', 'Nothing lost', 'With replication factor 3 and <code>acks=all</code>, a follower in the ISR takes over with every acknowledged record.'],
      coord: ['good', 'Must survive', 'The coordinator log is an ordinary replicated topic, appended with <code>acks=-1</code>, so a follower takes over the same way.'],
      cls: 'good', text: '<b>Clause 1.</b> A classic topic survives it, so the coordinator must too. The durability chapter shows how.' },
    { name: 'Every replica loses power at once', classic: ['warn', 'Loses one partition\'s tail', 'Kafka doesn\'t fsync every batch by default, so each partition can lose its unflushed tail. The loss stays in that partition.'],
      coord: ['bad', 'A tail loss would spread', 'A lost tail on one coordinator partition can make another one delete an object it still needs. So the coordinator fsyncs every batch on every replica.'],
      cls: 'warn', text: '<b>Clause 3.</b> A loss that spreads to other partitions can\'t be accepted. The design pays an fsync per batch to rule it out, as the durability chapter explains.' },
    { name: 'A broker returns with a wiped disk', classic: ['warn', 'Can lose acknowledged records', 'If every replica restarted uncleanly, Kafka can elect the last known leader even though its log is now empty.'],
      coord: ['good', 'Refused as leader', 'The controller checks the log directory ID at registration and won\'t let a replica whose storage changed lead.'],
      cls: 'good', text: '<b>Clauses 1 and 3.</b> On a coordinator partition the same election would wipe 1/N of every diskless topic\'s index, so the design closes it. See the election chapter.' },
    { name: 'Two brokers\' clocks disagree by 10 minutes', classic: ['good', 'Nothing lost', 'Replication and leader election don\'t depend on wall clocks.'],
      coord: ['good', 'Must survive', 'No clock decides whether an object is safe to delete. Clocks only decide when.'],
      cls: 'good', text: '<b>Clause 1.</b> This one ruled out the first version of the design, which used clocks for safety. See the clocks chapter.' },
    { name: 'A commit times out, then arrives late', classic: ['good', 'Nothing lost', 'The producer retries. An idempotent producer never sees a duplicate.'],
      coord: ['good', 'Must survive', 'The late commit can\'t point into a deleted object: a floor or a fence rejects it first.'],
      cls: 'good', text: '<b>Clause 1.</b> Floors and fences handle it without a clock. See the fences chapter.' },
    { name: 'Every replica of one coordinator partition is lost', classic: ['bad', 'That partition is gone', 'Losing every replica of a classic partition loses that partition\'s data.'],
      coord: ['warn', 'About 1/N of every diskless topic', 'The coordinator partition stays leaderless, so its user partitions can\'t be read or written. Its objects leak, but nothing else is deleted.'],
      cls: 'warn', text: '<b>Clause 2\'s named exception.</b> Accepted because the controller enforces 3 replicas spread across racks. See the risks chapter.' },
    { name: 'A bug in the code that applies records', classic: ['warn', 'Depends on the bug', 'Kafka\'s own coordinators share this limit: a deterministic bug corrupts every replica alike.'],
      coord: ['warn', 'Accepted, with tests', 'A wrong release or fence answer can delete an object that another partition still needs.'],
      cls: 'warn', text: '<b>Clause 3\'s exception.</b> Shared tests, golden-record tests, and stopping on unknown records reduce the chance. See the risks chapter.' },
    { name: 'Someone deletes objects from the bucket', classic: ['bad', 'Data lost', 'Deleting a classic topic\'s segment files loses data too.'],
      coord: ['bad', 'Data lost', 'Inkless can\'t stop a delete from outside Kafka.'],
      cls: 'warn', text: '<b>Clause 3\'s exception.</b> The deployment docs forbid expiration rules and manual deletes on the WAL prefix.' },
  ];
  cases.forEach((c, i) => sel.append(h('option', { value: i }, c.name)));
  const panel = (title, [cls, verdict, text]) => `<h6>${title} ${chip(cls, verdict)}</h6><p class="small" style="margin:0">${text}</p>`;
  const pick = () => {
    const c = cases[sel.value];
    $('#bar-classic').innerHTML = panel('Classic topic', c.classic);
    $('#bar-coord').innerHTML = panel('Coordinator', c.coord);
    show($('#bar-readout'), c.cls, c.text);
  };
  sel.addEventListener('change', pick);
  pick();
}

/* ---------------------------------------------------------------- the lease meter */
function demoLease() {
  const view = $('#lz-view');
  if (!view) return;
  const K = 16, IDLE = 6, KA = IDLE / 2, PREF = 2, ORDER = [2, 5, 0, 1, 3, 4, 6, 7];
  const playBtn = $('#lz-play'), downBtn = $('#lz-down'), quietBtn = $('#lz-quiet'), cutBtn = $('#lz-cut');
  let st, timer = null;
  const log = m => { st.events.unshift(`tick ${st.tick}: ${m}`); st.events = st.events.slice(0, 7); };
  const isUp = cp => !(cp === PREF && st.down);
  const reset = () => {
    st = { tick: 0, ranges: [{ from: 0, to: K - 1, at: PREF, state: 'active' }], maxReq: K - 1, next: 0, inUse: PREF, pending: null,
      used: new Set(), trackFailed: new Set(), queued: new Set(), missed: 0, releases: [], sinceAnswer: 0, silence: 0, endedIdle: false,
      reasons: { threshold: 0, keepalive: 0, 'failed tracking part': 0, assignment: 0 }, keepalives: 0, failed: 0, stale: 0, events: [],
      down: downBtn.classList.contains('on'), quiet: quietBtn.classList.contains('on'), cut: cutBtn.classList.contains('on') };
    log('cp2 leased [0, 15] to broker 3 at epoch 7.');
    render('Broker 3 holds the lease [0, 15] at cp2, its preferred tracking partition. Each tick, a busy broker closes one file.');
  };
  const active = () => st.ranges.filter(r => r.state === 'active' && r.at === st.inUse);
  const remaining = () => active().reduce((a, r) => a + Math.max(0, r.to - Math.max(r.from, st.next) + 1), 0);
  const renew = reason => {
    if (st.pending) return;
    st.pending = { from: st.maxReq + 1, to: st.maxReq + K, reason, since: st.tick };
    st.maxReq += K;
    if (st.reasons[reason] !== undefined) st.reasons[reason]++;
    log(`renewal (${reason}): asks for [${st.pending.from}, ${st.pending.to}], walking the list from cp${ORDER[0]}.`);
  };
  const leftovers = r => { const out = []; for (let s = Math.max(r.from, st.next); s <= r.to; s++) if (!st.used.has(s)) out.push(s); return out; };
  const tick = () => {
    st.tick++;
    let msg = '';
    if (st.pending && st.pending.since < st.tick && !st.cut) {
      const p = st.pending;
      const target = ORDER.find(isUp);
      const skipped = ORDER.slice(0, ORDER.indexOf(target));
      st.pending = null;
      st.ranges.push({ from: p.from, to: p.to, at: target, state: 'active' });
      st.sinceAnswer = 0;
      if (target !== st.inUse || st.endedIdle) {
        const old = st.inUse;
        const dropped = st.ranges.filter(r => r.at === old && r.state === 'active');
        dropped.forEach(r => { r.state = 'abandoned'; });
        if (dropped.length) st.releases.push({ at: old, b: Math.max(...dropped.map(r => r.to)) });
        st.inUse = target; st.next = p.from; st.silence = 0; st.endedIdle = false;
        msg = `${skipped.length ? `cp${skipped.join(', cp')} didn't answer, so the renewal walked down the list. ` : ''}<b>cp${target} confirmed [${p.from}, ${p.to}], and the broker switches at once.</b> ${dropped.length ? `It abandons the rest of its ranges at cp${old}, and owes cp${old} a lease release.` : ''}`;
        log(`cp${target} appended sequences-leased [${p.from}, ${p.to}]; the broker switched from cp${old}.`);
      } else {
        if (!active().some(r => r.from <= st.next && st.next <= r.to)) st.next = p.from;
        st.silence = 0;
        msg = `cp${target} confirmed [${p.from}, ${p.to}] in the background. The broker uses up its current range first.`;
        log(`cp${target} appended sequences-leased [${p.from}, ${p.to}].`);
      }
    }
    if (st.cut) {
      st.sinceAnswer++;
      if (isUp(st.inUse)) st.silence++;
      msg = `The broker is cut off: nothing it sends gets through, and it closes no files. cp${st.inUse} has heard nothing for ${st.silence} tick${st.silence === 1 ? '' : 's'}.`;
      if (st.silence >= IDLE && !st.endedIdle && active().length) {
        const left = active().flatMap(leftovers);
        active().forEach(r => { r.state = 'ended'; });
        left.forEach(s => st.queued.add(s));
        st.endedIdle = true;
        msg = `<b>The idle timer fires, as a backstop.</b> cp${st.inUse} heard nothing for ${IDLE} ticks, so it appends lease-ended. It fences the ${left.length} unused sequences at every partition and queues their keys. The broker doesn't know yet.`;
        log(`cp${st.inUse}: idle lease end; ${left.length} leftover keys fenced and queued.`);
      }
    } else if (st.quiet) {
      st.sinceAnswer++;
      if (isUp(st.inUse)) st.silence++;
      msg = `The broker is quiet and closes no file. ${st.sinceAnswer} tick${st.sinceAnswer === 1 ? '' : 's'} since its last answer from cp${st.inUse}.`;
      if (st.sinceAnswer >= KA && !st.pending) {
        if (isUp(st.inUse)) {
          st.keepalives++; st.sinceAnswer = 0; st.silence = 0;
          msg = `<b>Keepalive.</b> ${KA} ticks without an answer, so the broker asks cp${st.inUse} again for a range it already holds. cp${st.inUse} confirms it, appends nothing, and restarts its idle timer.`;
          log(`keepalive answered by cp${st.inUse}.`);
        } else {
          renew('keepalive');
          msg = `<b>The keepalive gets no answer</b>, because cp${st.inUse} is down. The broker starts a renewal and walks its list.`;
        }
      }
    } else if (st.endedIdle) {
      st.stale++;
      renew('back after the lease ended');
      msg = `<b>Stale sequence.</b> The broker is back, but cp${st.inUse} ended its lease while it was cut off. This file's parts get stale-sequence, so its producers retry. The broker renews above the floor.`;
      log('a file failed with stale-sequence after the idle lease end.');
    } else {
      st.sinceAnswer++;
      const r = active().find(x => x.from <= st.next && st.next <= x.to);
      if (r) {
        const s = st.next++;
        st.used.add(s);
        if (isUp(st.inUse)) {
          st.sinceAnswer = 0; st.silence = 0;
          msg = `The broker closed a file and took sequence ${s}. No request was needed.`;
        } else {
          st.trackFailed.add(s);
          msg = `The broker closed a file with sequence ${s}, but its tracking part to cp${st.inUse} fails at the deadline. <b>The broker renews at once.</b> The other parts commit normally.`;
          renew('failed tracking part');
        }
        if (!active().some(x => x.from <= st.next && st.next <= x.to)) {
          active().filter(x => x.to < st.next).forEach(x => { x.state = 'used'; });
          const later = active().filter(x => x.from >= st.next).sort((x, y) => x.from - y.from)[0];
          if (later) st.next = later.from;
        }
      } else {
        st.failed++;
        msg = '<b>No leased sequence.</b> The file fails at once with a retriable error and is never uploaded, so nothing unrecorded reaches storage.';
        log('a file failed without upload: no leased sequence.');
      }
      if (remaining() < K / 2) renew('threshold');
    }
    for (const rel of st.releases.slice()) {
      if (st.cut || !isUp(rel.at)) continue;
      const covered = st.ranges.filter(r => r.at === rel.at && r.state === 'abandoned' && r.to <= rel.b);
      const left = covered.flatMap(r => { const out = []; for (let s = r.from; s <= r.to; s++) if (!st.used.has(s)) out.push(s); return out; });
      const missed = covered.flatMap(r => { const out = []; for (let s = r.from; s <= r.to; s++) if (st.trackFailed.has(s)) out.push(s); return out; });
      covered.forEach(r => { r.state = 'released'; });
      left.forEach(s => st.queued.add(s));
      missed.forEach(s => st.trackFailed.delete(s));
      st.missed += missed.length;
      st.releases.splice(st.releases.indexOf(rel), 1);
      msg += ` <b>Lease release to cp${rel.at}</b> through ${rel.b}: cp${rel.at} ends those ranges, fences the ${left.length} unused sequences, and queues their keys.` +
        (missed.length ? ` Its finish report also reaches ${missed.length} sequence${missed.length === 1 ? '' : 's'} whose tracking part failed. cp${rel.at} never recorded ${missed.length === 1 ? 'it' : 'them'}, so it fences ${missed.length === 1 ? 'it' : 'them'} at all 8 partitions and tracks the objects from the answers.` : '');
      log(`lease release accepted by cp${rel.at}; ${left.length} keys queued${missed.length ? `, ${missed.length} missed fenced everywhere` : ''}.`);
    }
    render(msg);
  };
  const render = msg => {
    view.innerHTML = '';
    for (const r of st.ranges.slice(-4)) {
      const cells = [];
      for (let s = r.from; s <= r.to; s++) {
        let bg = 'var(--soft)', ink = 'var(--muted)', title = 'leased, unused', border = '';
        if (st.used.has(s)) { bg = 'var(--accent)'; ink = '#fff'; title = 'file uploaded'; if (st.trackFailed.has(s)) { border = '2px dashed var(--warn)'; title = 'uploaded; its tracking part failed'; } }
        else if (st.queued.has(s)) { bg = 'var(--warn)'; ink = '#fff'; title = 'fenced and queued for deletion'; }
        else if (r.state === 'abandoned') { border = '1px dashed var(--muted)'; title = 'abandoned: waits for the lease release'; }
        else if (s === st.next && r.state === 'active' && r.at === st.inUse) { bg = 'color-mix(in srgb, var(--accent) 25%, transparent)'; ink = 'var(--ink)'; title = 'next sequence'; }
        cells.push(h('span', { class: 'cell', style: { background: bg, color: ink, minWidth: '1.6rem', border }, title }, s));
      }
      const label = { active: r.at === st.inUse ? 'in use' : 'active', used: 'used up', abandoned: 'abandoned', released: 'released', ended: 'ended idle' }[r.state];
      view.append(h('div', { class: 'bar-row', style: { gridTemplateColumns: '11rem 1fr' } },
        h('span', { class: 'small', html: `[${r.from}, ${r.to}] at ${cpChip(r.at)} <span class="muted">${label}</span>` }),
        h('div', { class: 'log' }, cells)));
    }
    if (st.pending) view.append(h('div', { class: 'small muted' }, `Renewal in flight for [${st.pending.from}, ${st.pending.to}].`));
    view.append(h('div', { class: 'legend' },
      h('span', { html: '<span class="dot" style="background:var(--accent)"></span>uploaded' }),
      h('span', { html: '<span class="dot" style="background:var(--accent);outline:2px dashed var(--warn)"></span>tracking part failed' }),
      h('span', { html: '<span class="dot" style="background:var(--warn)"></span>fenced and queued' }),
      h('span', { html: '<span class="dot" style="border:1px dashed var(--muted)"></span>abandoned' })));
    view.append(h('div', { class: 'small' }, h('b', null, 'Recent events'),
      h('ol', { class: 'small mono', style: { margin: '0.2rem 0 0', paddingLeft: '1.1rem' } }, st.events.map(e => h('li', null, e)))));
    const reasons = Object.entries(st.reasons).map(([k, v]) => `${k} ${v}`).join(', ');
    show($('#lz-readout'), /No leased|Stale sequence|idle timer/.test(msg) ? 'warn' : '',
      `${msg} <div class="small muted" style="margin-top:0.4rem">Files uploaded: ${st.used.size}. Failed without upload: ${st.failed}. Failed with stale-sequence: ${st.stale}. Keepalives: ${st.keepalives}. Renewals by reason: ${reasons}. Keys queued: ${st.queued.size}. Missed sequences fenced: ${st.missed}.</div>`);
  };
  $('#lz-tick').addEventListener('click', tick);
  playBtn.addEventListener('click', () => {
    if (timer) { clearInterval(timer); timer = null; playBtn.classList.remove('on'); return; }
    playBtn.classList.add('on');
    timer = setInterval(tick, 800);
  });
  downBtn.addEventListener('click', () => {
    st.down = !st.down;
    downBtn.classList.toggle('on', st.down);
    render(st.down
      ? 'cp2 is down. The next tracking part to cp2 fails, which starts a renewal. Tick to see the broker walk its list.'
      : 'cp2 is back. A broker on a fallback doesn\'t rush back: its next renewal walks the list from the top, reaches cp2, and switches back the same way.');
  });
  quietBtn.addEventListener('click', () => {
    st.quiet = !st.quiet;
    quietBtn.classList.toggle('on', st.quiet);
    render(st.quiet ? `The broker stops closing files. Every ${KA} ticks without an answer, it sends a keepalive. Its lease never ends idle.` : 'The broker is busy again.');
  });
  cutBtn.addEventListener('click', () => {
    st.cut = !st.cut;
    cutBtn.classList.toggle('on', st.cut);
    render(st.cut ? `Nothing the broker sends gets through. Keep ticking: after ${IDLE} silent ticks, the idle timer ends the lease and collects the leftover keys.` : 'The broker is reachable again.');
  });
  $('#lz-reset').addEventListener('click', () => {
    if (timer) { clearInterval(timer); timer = null; playBtn.classList.remove('on'); }
    reset();
  });
  reset();

  const chart = $('#lat-chart');
  if (!chart) return;
  const rowsData = [
    ['PostgreSQL', [[250, 'buffer', 'var(--faint)'], [100, 'upload', 'var(--accent)'], [10, 'commit', 'var(--good)']]],
    ['Message before each upload (rejected)', [[250, 'buffer', 'var(--faint)'], [10, 'message', 'var(--bad)'], [100, 'upload', 'var(--accent)'], [10, 'commit', 'var(--good)']]],
    ['This design', [[250, 'buffer', 'var(--faint)'], [100, 'upload', 'var(--accent)'], [10, 'commit', 'var(--good)']]],
  ];
  const total = 380;
  for (const [name, segs] of rowsData) {
    let at = 0;
    const track = h('div', { class: 'bar-track' });
    for (const [ms, label, color] of segs) {
      track.append(h('div', { class: 'bar-seg', title: `${label}: ${ms} ms`, style: { left: (at / total) * 100 + '%', width: (ms / total) * 100 + '%', background: color } }, ms >= 50 ? `${label} ${ms} ms` : ''));
      at += ms;
    }
    chart.append(h('div', { class: 'bar-row', style: { gridTemplateColumns: '13rem 1fr' } }, h('span', null, h('b', null, name), h('span', { class: 'muted' }, ` ${at} ms`)), track));
  }
  chart.append(h('figcaption', null, 'Median time from a produce request to its answer. The red slice is a replicated, fsynced append before the upload can start. With leases, the produce path has the same steps as PostgreSQL.'));
}

/* ---------------------------------------------------------------- follow an object */
function demoLife() {
  const cpsHost = $('#lf-cps');
  if (!cpsHost) return;
  const key = s => `MkU3.3.7.${s}`;
  const init = () => ({
    cps: [0, 1, 2, 3].map(i => ({ floor: null, batches: [], records: [], extra: i === 2 ? { leases: [], through: null, entries: [], queue: [] } : null })),
    storage: [],
  });
  const rec = (s, i, r) => { s.cps[i].records.push(r); };
  const commit = (s, i, seq, batch) => {
    s.cps[i].floor = maxPair(s.cps[i].floor, [7, seq]);
    if (batch) s.cps[i].batches.push(`${batch} in ${key(seq)}`);
    rec(s, i, `file committed (7, ${seq})`);
  };
  const fence = (s, i, a, b) => { s.cps[i].floor = maxPair(s.cps[i].floor, [7, b]); rec(s, i, `sequences fenced [${a}, ${b}]`); };
  const entry = (s, seq) => s.cps[2].extra.entries.find(e => e.seq === seq);
  const setState = (s, seq, cp, state) => { entry(s, seq).st[cp] = state; };
  const removeBatches = (s, i, seq, record) => {
    s.cps[i].batches = s.cps[i].batches.filter(b => !b.endsWith(key(seq)));
    rec(s, i, record);
  };
  const release = (s, seq, cp) => { setState(s, seq, cp, 'released'); rec(s, cp, `release accepted: ${key(seq)}`); };
  const queueIfDone = (s, seq) => {
    const e = entry(s, seq);
    if (Object.values(e.st).every(x => x === 'released')) {
      s.cps[2].extra.entries = s.cps[2].extra.entries.filter(x => x !== e);
      s.cps[2].extra.queue.push(key(seq));
    }
  };
  const report = (s, through, text) => { s.cps[2].extra.through = through; rec(s, 2, text || `finish reported: through ${through}`); };
  const upload = (s, seq) => s.storage.push({ key: key(seq), state: 'stored' });
  const lease = { title: 'Lease the numbers', text: 'Broker 3 asks cp2, its tracking partition, for a lease. cp2 appends a sequences-leased record for <code>[0, 15]</code> in epoch 7. This happens in the background, long before any file closes.',
    run: s => { s.cps[2].extra.leases.push('epoch 7: [0, 15]'); rec(s, 2, 'sequences leased [0, 15]'); } };
  const closeUpload = { title: 'Close and upload', text: `The file closes with sequence 0 from the lease, so its key is <code>${key(0)}</code>. No request is sent: the lease already put the key on record. The broker uploads the object.`, run: s => upload(s, 0) };
  const allParts = s => {
    commit(s, 0, 0, 'orders-0'); commit(s, 1, 0, 'clicks-1'); commit(s, 3, 0, 'logs-2'); commit(s, 2, 0, null);
    s.cps[2].extra.entries.push({ seq: 0, st: { 0: 'pending', 1: 'pending', 3: 'pending' } });
  };
  const deleteStep = (keys, extra) => ({ title: 'Grace period, then delete',
    text: `After the one-minute grace period, <code>FileCleaner</code> on cp2's leader deletes the queued key${keys.length > 1 ? 's' : ''} and appends a files-deleted record.${extra || ''}`,
    run: s => {
      s.cps[2].extra.queue = [];
      rec(s, 2, `files deleted (${keys.length})`);
      for (const k of keys) {
        const o = s.storage.find(x => x.key === k);
        if (o) o.state = 'deleted'; else s.storage.push({ key: k, state: 'missing' });
      }
    } });

  const scenarios = [
    { name: 'Happy path', steps: [
      lease, closeUpload,
      { title: 'Send the commit parts', text: 'Parts with batches go to cp0, cp1, and cp3. The tracking part to cp2 carries no batches, only the participant set <code>{cp0, cp1, cp3}</code>. Each partition records the commit and raises broker 3\'s floor to <code>(7, 0)</code>. cp2 creates one entry per participant, all pending.', run: allParts },
      { title: 'The finish report arrives', text: 'Every part answered with the <code>Recorded</code> flag set, so the file is finished with no exceptions. Broker 3\'s next tracking part carries the report <code>finishedThrough = 0</code>. cp2 marks every entry held. <b>No fence is sent</b>: cp2 never fences a participant that answered.',
        run: s => { report(s, 0); for (const cp of [0, 1, 3]) setState(s, 0, cp, 'held'); } },
      { title: 'cp0: retention removes the batches', text: 'Retention removes the <i>orders-0</i> batches. In the same SQLite transaction, cp0 checks <code>batches(file_id)</code>: nothing points into the object anymore. It sends a release report to cp2 and records that cp2 accepted it.',
        run: s => { removeBatches(s, 0, 0, 'retention applied'); release(s, 0, 0); } },
      { title: 'cp1: DeleteRecords', text: 'A <code>DeleteRecords</code> request removes the <i>clicks-1</i> batches, and cp1 releases the object the same way.',
        run: s => { removeBatches(s, 1, 0, 'records deleted'); release(s, 0, 1); } },
      { title: 'cp3: consolidation prunes the batches', text: 'Consolidation copied the <i>logs-2</i> batches into a classic log, and pruning removes them from cp3. cp3 releases. Every entry is released, so cp2 puts the key in its deletion queue.',
        run: s => { removeBatches(s, 3, 0, 'log pruned'); release(s, 0, 3); queueIfDone(s, 0); } },
      deleteStep([key(0)], ' Sequences 1 to 15 stay leased for the next files.'),
    ] },
    { name: 'Crash before any commit', steps: [
      lease, closeUpload,
      { title: 'The broker crashes', text: 'Broker 3 crashes before it sends any part. The object is in storage, and no partition holds batches in it: an orphan. But cp2 leased its sequence, so its key is already on record.' },
      { title: 'The broker returns with a new epoch', text: 'Broker 3 registers again and gets epoch 8. Its first lease request at epoch 8 ends the epoch 7 lease at cp2, at a fixed log position. cp2 recorded no finish report for epoch 7, so the whole range <code>[0, 15]</code> is leftover.',
        run: s => { s.cps[2].extra.leases = ['epoch 7: ended, leftovers [0, 15]', 'epoch 8: [0, 15]']; rec(s, 2, 'lease ended (epoch 7)'); rec(s, 2, 'sequences leased [0, 15] (epoch 8)'); } },
      { title: 'Fence the leftovers everywhere', text: 'A lease end sends one range fence per partition. Each partition appends sequences-fenced, raises broker 3\'s fenced-through mark to <code>(7, 15)</code>, and answers that it holds none of those sequences. cp2 raises its own floor in the same way.',
        run: s => { for (let i = 0; i < 4; i++) fence(s, i, 0, 15); } },
      { title: 'Queue all sixteen keys', text: 'No partition holds any of the sixteen keys, so cp2 queues them all. Fifteen were never uploaded, which doesn\'t matter.',
        run: s => { for (let q = 0; q < 16; q++) s.cps[2].extra.queue.push(key(q)); } },
      deleteStep(Array.from({ length: 16 }, (_, q) => key(q)), ' One delete removes the real orphan. Deleting a key that doesn\'t exist counts as success.'),
    ] },
    { name: 'A part times out', steps: [
      lease, closeUpload,
      { title: 'Two parts commit, two time out', text: 'The parts to cp0 and cp2 commit, so cp2 has the entries. The parts to cp1 and cp3 pass the commit deadline: their outcome is unknown. Requests with batches on cp1 or cp3 get <code>KAFKA_STORAGE_ERROR</code> and retry. Requests with batches only on cp0 succeed.',
        run: s => { commit(s, 0, 0, 'orders-0'); commit(s, 2, 0, null); s.cps[2].extra.entries.push({ seq: 0, st: { 0: 'pending', 1: 'pending', 3: 'pending' } }); } },
      { title: 'An urgent finish report', text: 'The file is finished, with exceptions for cp1 and cp3. A report with an exception is urgent, so the broker\'s next tracking part carries it within one commit interval: <code>finishedThrough = 0</code>, exception ranges <code>[0, 0]</code> for cp1 and cp3. cp2 marks cp0 held and the other two fencing, and sends them range fences.',
        run: s => { report(s, 0, 'finish reported: through 0, exceptions cp1, cp3'); setState(s, 0, 0, 'held'); setState(s, 0, 1, 'fencing'); setState(s, 0, 3, 'fencing'); } },
      { title: 'The fences answer', text: 'cp1 and cp3 apply the fence: their floor rises to <code>(7, 0)</code>, and neither holds batches from sequence 0, so both answer "released." If a late part is still waiting in a queue, it now arrives at or below the floor and gets stale-sequence.',
        run: s => { fence(s, 1, 0, 0); fence(s, 3, 0, 0); setState(s, 0, 1, 'released'); setState(s, 0, 3, 'released'); } },
      { title: 'cp0 releases', text: 'Later, retention removes cp0\'s batches, and cp0 sends a release report. Every entry is released, so cp2 queues the key.',
        run: s => { removeBatches(s, 0, 0, 'retention applied'); release(s, 0, 0); queueIfDone(s, 0); } },
      deleteStep([key(0)]),
    ] },
    { name: 'Missed sequence', steps: [
      lease,
      { title: 'File 0: the tracking part fails', text: `The file with sequence 0 uploads as <code>${key(0)}</code>. Its parts to cp0, cp1, and cp3 commit, but the tracking part to cp2 passes the deadline. cp2 has no record of sequence 0, so it doesn't know the participants.`,
        run: s => { upload(s, 0); commit(s, 0, 0, 'orders-0'); commit(s, 1, 0, 'clicks-1'); commit(s, 3, 0, 'logs-2'); } },
      { title: 'File 1 carries the finish report', text: `The next file, sequence 1, uploads as <code>${key(1)}</code> and commits at cp0 and cp2. Its tracking part reports <code>finishedThrough = 0</code>: file 0 is finished. That report reaches sequence 0, which cp2 never recorded, so sequence 0 is <b>missed</b>.`,
        run: s => { upload(s, 1); commit(s, 0, 1, 'orders-0'); commit(s, 2, 1, null); report(s, 0); s.cps[2].extra.entries.push({ seq: 1, st: { 0: 'pending' } }); } },
      { title: 'Fence the missed sequence everywhere', text: 'cp2 fences sequence 0 at all four partitions, itself included. cp0, cp1, and cp3 answer that they hold batches. cp2 builds the participant set from these answers, never from the broker, and marks each of them held.',
        run: s => { for (let i = 0; i < 4; i++) fence(s, i, 0, 0); s.cps[2].extra.entries.unshift({ seq: 0, st: { 0: 'held', 1: 'held', 3: 'held' } }); } },
      { title: 'Track it like any other object', text: `From here, sequence 0 is an ordinary entry. Over time, retention at cp0, cp1, and cp3 releases it, and cp2 queues <code>${key(0)}</code>.`,
        run: s => {
          for (const i of [0, 1, 3]) { removeBatches(s, i, 0, 'retention applied'); release(s, 0, i); }
          queueIfDone(s, 0);
        } },
      deleteStep([key(0)], ` <code>${key(1)}</code> stays until cp0 releases it.`),
    ] },
  ];

  let current = 0;
  const tabs = $('#lf-tabs');
  const tabBtns = scenarios.map((sc, i) => {
    const b = h('button', { onclick: () => { current = i; setOn(tabBtns, b); ctl.go(0); } }, sc.name);
    tabs.append(b);
    return b;
  });
  setOn(tabBtns, tabBtns[0]);
  const roles = ['participant', 'participant', 'tracking partition', 'participant'];
  const stChip = { pending: 'info', fencing: 'warn', held: '', released: 'good' };
  const ctl = stepper({
    prev: $('#lf-prev'), next: $('#lf-next'), reset: $('#lf-reset'),
    init, steps: () => scenarios[current].steps,
    render: (s, prev, step, i, n) => {
      $('#lf-count').textContent = `Step ${i + 1} of ${n}`;
      $('#lf-narr').innerHTML = `<h6>${step.title}</h6><p>${step.text}</p>`;
      cpsHost.innerHTML = '';
      s.cps.forEach((c, k) => {
        const changed = prev && JSON.stringify(prev.cps[k]) !== JSON.stringify(c);
        let html = `<h6>${cpChip(k)}<span class="small muted">${roles[k]}</span></h6>` +
          `<dl class="kv"><dt>broker 3 floor</dt><dd>${pair(c.floor)}</dd><dt>batches</dt><dd>${c.batches.length ? c.batches.join('<br>') : 'none'}</dd></dl>`;
        if (c.extra) {
          const q = c.extra.queue;
          html += `<dl class="kv" style="margin-top:0.3rem"><dt>leases</dt><dd>${c.extra.leases.join('<br>') || 'none'}</dd>` +
            `<dt>finished through</dt><dd>${c.extra.through === null ? 'none' : c.extra.through}</dd>` +
            `<dt>entries</dt><dd>${c.extra.entries.map(e => `seq ${e.seq}: ${Object.entries(e.st).map(([p, v]) => chip(stChip[v], `cp${p} ${v}`)).join(' ')}`).join('<br>') || 'none'}</dd>` +
            `<dt>queue</dt><dd>${q.length > 3 ? `${q.length} keys: ${q[0]} … ${q[q.length - 1]}` : q.join('<br>') || 'empty'}</dd></dl>`;
        }
        const recent = c.records.slice(-4);
        html += '<div class="small muted" style="margin-top:0.35rem">Recent records</div>' +
          (recent.length ? `<ol class="small mono" start="${c.records.length - recent.length + 1}" style="margin:0;padding-left:1.4rem">${recent.map(r => `<li>${r}</li>`).join('')}</ol>` : '<div class="small muted">none</div>');
        cpsHost.append(h('div', { class: 'panel' + (changed ? ' changed' : ''), html }));
      });
      const stor = $('#lf-storage');
      stor.className = 'panel' + (prev && JSON.stringify(prev.storage) !== JSON.stringify(s.storage) ? ' changed' : '');
      if (!s.storage.length) { stor.innerHTML = '<span class="small muted">Empty</span>'; return; }
      const missing = s.storage.filter(o => o.state === 'missing').length;
      stor.innerHTML = '<div class="storage-objects">' + s.storage.filter(o => o.state !== 'missing').map(o =>
        `<span class="chip ${o.state === 'deleted' ? 'bad' : 'info'} mono">${o.key} · ${o.state}</span>`).join('') +
        (missing ? `<span class="chip">${missing} more deletes of keys that never existed</span>` : '') + '</div>';
    },
  });
}

/* ---------------------------------------------------------------- clock faults */
function demoClock() {
  const sel = $('#ck-fault');
  if (!sel) return;
  const faults = [
    { name: 'A tracking leader\'s monotonic clock jumps 20 minutes forward',
      what: 'Idle timers fire early at the tracking partitions this broker leads, so live brokers\' leases end without cause. Each lease end fences the rest of the range at every partition.',
      cost: 'Files in flight under those leases fail once with stale-sequence. Their brokers renew above the floor, and producers retry.', safe: true },
    { name: 'A broker pauses for 10 minutes (a long GC or VM freeze)',
      what: 'The pause is longer than half the idle timeout, so the keepalive is late and the lease can end idle. The first file after the pause gets stale-sequence.',
      cost: 'One failed file per paused broker. It renews above the floor and continues.', safe: true },
    { name: 'A broker pauses past the timeout with uploads in flight',
      what: 'Its lease ends, and the lease-end fences raise every floor. Its commits get stale-sequence. An upload that storage completes after the queue deleted its key stays behind.',
      cost: 'At most one leaked object per upload attempt. That\'s the accepted leak, and it costs storage, not data.', safe: true },
    { name: 'The deleting leader\'s wall clock runs 2 minutes ahead',
      what: 'The grace period is measured from the marking leader\'s clock to the deleting leader\'s, so <code>FileCleaner</code> deletes about 2 minutes early.',
      cost: 'A reader holding cached coordinates gets a storage error and retries with fresh ones. The batches were already gone from coordinator state, so nothing committed points at the object. PostgreSQL has the same exposure.', safe: true },
    { name: 'The deleting leader\'s wall clock runs behind',
      what: '<code>FileCleaner</code> waits longer than the grace period.',
      cost: 'Storage holds deleted objects a bit longer.', safe: true },
    { name: 'Tracking partition leadership changes every 5 minutes',
      what: 'Each new leader restarts every idle timer at its election, so the 15-minute timer never fires. Cut-off brokers\' leases don\'t end.',
      cost: 'Their leftover keys stay until leadership holds for one timeout. Live brokers don\'t depend on the timer, so they\'re unaffected.', safe: true },
    { name: 'A coordinator leader\'s clock runs a day ahead',
      what: 'Retention compares batch timestamps with the leader\'s wall clock, so it removes batches up to a day early. Their objects are then released and deleted normally.',
      cost: 'Records disappear early, exactly as on a classic topic whose leader\'s clock runs ahead. It\'s an accepted risk, covered by NTP monitoring.', safe: false },
  ];
  faults.forEach((f, i) => sel.append(h('option', { value: i }, f.name)));
  const pick = () => {
    const f = faults[sel.value];
    $('#ck-what').innerHTML = `<h6>What happens</h6><p class="small" style="margin:0">${f.what}</p>`;
    $('#ck-cost').innerHTML = `<h6>What it costs</h6><p class="small" style="margin:0">${f.cost}</p>`;
    show($('#ck-readout'), f.safe ? 'good' : 'warn', f.safe
      ? '<b class="good">No committed batch points at a deleted object.</b> Every key in the deletion queue got there through a record that made it impossible to reference. The clock only changed timing.'
      : '<b class="warn">Retention deletes data early</b>, as it would on a classic topic. That\'s the clock deciding when records expire, which is retention\'s job. The deletion protocol still never deletes an object a batch references.');
  };
  sel.addEventListener('change', pick);
  pick();
}

/* ---------------------------------------------------------------- the parts calculator */
function demoParts() {
  const br = $('#pt-br');
  if (!br) return;
  const racksSel = $('#pt-racks'), nSel = $('#pt-n'), gwBtn = $('#pt-gw');
  const R = 3;
  const calc = () => {
    const b = +br.value, racks = +racksSel.value, N = +nSel.value, on = gwBtn.classList.contains('on');
    $('#pt-br-v').textContent = b;
    const B = b * racks;
    const g = Math.max(1, Math.floor(b / N));
    const activeBrokers = Math.min(b, N * g);
    const ppf = (N * g) / activeBrokers;
    const perLeader = racks * g;
    const roundOn = N * racks * g, roundOff = B * N;
    const kv = rows => `<dl class="kv">${rows.map(([k, v]) => `<dt>${k}</dt><dd>${v}</dd>`).join('')}</dl>`;
    const off = $('#pt-off'), onP = $('#pt-on');
    off.className = 'panel' + (on ? '' : ' changed');
    onP.className = 'panel' + (on ? ' changed' : '');
    off.innerHTML = `<h6>Gateway routing off</h6>` + kv([
      ['parts per file', N], ['flushes per file, at most', N * R], ['parts per leader per round', B], ['parts per round, cluster', fmt(roundOff)]]);
    onP.innerHTML = `<h6>Gateway routing on</h6>` + kv([
      ['gateways per cp per rack', g], ['parts per file', +ppf.toFixed(2)], ['flushes per file, at most', +(ppf * R).toFixed(1)],
      ['parts per leader per round', perLeader], ['parts per round, cluster', fmt(roundOn)], ['spares per rack', b - activeBrokers]]);
    const ratio = roundOff / roundOn;
    show($('#pt-readout'), on ? (ratio > 1 ? 'good' : '') : 'warn',
      b === 1 ? 'With 1 broker per rack, every broker is the gateway for every coordinator partition in its rack, so routing changes nothing.'
        : on ? `Routing cuts the cluster's parts per round from <code>B × N</code> = ${fmt(roundOff)} to <code>N × racks × g</code> = ${fmt(roundOn)}: <b>${ratio.toFixed(1)} times fewer</b> parts and flushes. ${b >= N ? 'The racks are at saturation, so each gateway serves one coordinator partition and a file sends one part.' : `Below ${N} brokers per rack, each gateway serves about ${ppf.toFixed(1)} coordinator partitions.`}`
          : `Without routing, every file touches all ${N} coordinator partitions, and each leader receives one part from every broker on every round. Turn routing on to compare.`);
  };
  [br, racksSel, nSel].forEach(el => el.addEventListener('input', calc));
  gwBtn.addEventListener('click', () => { gwBtn.classList.toggle('on'); gwBtn.textContent = gwBtn.classList.contains('on') ? 'Gateway routing on' : 'Gateway routing off'; calc(); });
  calc();
}

/* ---------------------------------------------------------------- gateway assignment */
function demoGateways() {
  const grid = $('#gw-grid');
  if (!grid) return;
  const N = 8, plainBtn = $('#gw-plain');
  let ids, nextId, prevGw = null, lastChange = '';
  const reset = () => { ids = [1, 2, 3]; nextId = 4; prevGw = null; lastChange = ''; render(); };
  const render = () => {
    const plain = plainBtn.classList.contains('on');
    const { gw, load, g, cap } = assignRack(ids, N, plain);
    let moves = 0;
    if (prevGw) gw.forEach((list, cp) => { moves += list.filter(b => !prevGw[cp].includes(b)).length; });
    grid.innerHTML = '';
    const table = h('table', { class: 'gw-table' });
    const head = h('tr', null, h('th', null, 'cp \\ broker'));
    for (const b of ids) head.append(h('th', { title: 'Click to remove this broker', class: 'clickable', onclick: () => remove(b) }, `b${b} ✕`));
    table.append(head);
    for (let cp = 0; cp < N; cp++) {
      const ranked = ids.slice().sort((x, y) => gwScore(cp, y) - gwScore(cp, x) || x - y);
      const row = h('tr', null, h('th', { html: cpChip(cp) }));
      for (const b of ids) {
        const rank = ranked.indexOf(b);
        const alpha = Math.round(70 - (rank / Math.max(1, ids.length - 1)) * 60);
        const isGw = gw[cp].includes(b);
        row.append(h('td', { class: isGw ? 'gw-on' : '', title: `score ${gwScore(cp, b)}`,
          style: { background: `color-mix(in srgb, var(--accent) ${alpha}%, transparent)` } }, isGw ? `#${rank + 1} GW` : `#${rank + 1}`));
      }
      table.append(row);
    }
    const loadRow = h('tr', null, h('th', null, 'slots'));
    for (const b of ids) loadRow.append(h('td', { class: 'mono' }, load.get(b)));
    table.append(loadRow);
    grid.append(table);
    const maxLoad = Math.max(...ids.map(b => load.get(b)));
    const spares = ids.filter(b => load.get(b) === 0).length;
    show($('#gw-readout'), plain && maxLoad > Math.ceil((N * g) / ids.length) ? 'warn' : '',
      `<b>${ids.length} broker${ids.length === 1 ? '' : 's'}</b>: <code>g = ${g}</code>, cap ${plain ? 'none' : cap}. Busiest broker holds ${maxLoad} slot${maxLoad === 1 ? '' : 's'}${spares ? `, and ${spares} ${spares === 1 ? 'broker is a spare' : 'brokers are spares'}` : ''}. ` +
      (prevGw ? `${lastChange} moved <b>${moves}</b> gateway slot${moves === 1 ? '' : 's'}.` : 'Add or remove a broker to count moves.') +
      (plain && maxLoad > Math.ceil((N * g) / ids.length) ? ` Without the cap, balance is left to chance: the cap would allow at most ${Math.ceil((N * g) / ids.length)}.` : ''));
    prevGw = gw.map(l => l.slice());
  };
  const remove = b => {
    if (ids.length === 1) return;
    ids = ids.filter(x => x !== b);
    lastChange = `Removing b${b}`;
    render();
  };
  $('#gw-add').addEventListener('click', () => { ids.push(nextId); lastChange = `Adding b${nextId}`; nextId++; render(); });
  $('#gw-reset').addEventListener('click', reset);
  plainBtn.addEventListener('click', () => { plainBtn.classList.toggle('on'); prevGw = null; render(); });
  reset();
}

/* ---------------------------------------------------------------- routing a client */
function demoRoute() {
  const host = $('#rt-racks');
  if (!host) return;
  const N = 8, rackSel = $('#rt-rack'), cpSel = $('#rt-cp'), sizeSel = $('#rt-size');
  const RACKS = ['a', 'b', 'c'];
  const LOG_KEY = 'orders-0';
  let state, prevPick = null;
  for (let cp = 0; cp < N; cp++) cpSel.append(h('option', { value: cp }, `cp${cp}`));
  const brokers = () => {
    const size = +sizeSel.value;
    return RACKS.flatMap((_, r) => Array.from({ length: size }, (_, k) => ({ id: r * 100 + k + 1, rack: r })));
  };
  const reset = () => { state = new Map(brokers().map(b => [b.id, 'running'])); prevPick = null; render(); };
  const compute = () => {
    const all = brokers();
    const perRack = RACKS.map((_, r) => assignRack(all.filter(b => b.rack === r && state.get(b.id) === 'running').map(b => b.id), N));
    const r = +rackSel.value, p = +cpSel.value;
    const alive = all.filter(b => state.get(b.id) !== 'fenced');
    const steps = [];
    let pick = null, step = 0;
    if (r >= 0) {
      const gws = perRack[r].gw[p];
      steps.push(gws.length ? `1. A gateway of cp${p} in rack ${RACKS[r]}: b${gws.join(', b')}.` : `1. A gateway of cp${p} in rack ${RACKS[r]}: none, because rack ${RACKS[r]} has no running candidate.`);
      if (gws.length) { pick = logKeyPick(LOG_KEY, gws); step = 1; }
      if (!pick) {
        const others = alive.filter(b => b.rack === r && state.get(b.id) === 'running').map(b => b.id);
        steps.push(`2. Another broker in rack ${RACKS[r]}: ${others.length ? `b${others.join(', b')}` : 'none that isn\'t excluded'}.`);
        if (others.length) { pick = others.sort((x, y) => gwScore(p, y) - gwScore(p, x) || x - y)[0]; step = 2; }
      }
    } else {
      steps.push('1 and 2. The client\'s AZ is unknown, so it counts as a client of the rack-less group. Every broker has a rack, so the group has no brokers.');
    }
    if (!pick) {
      const remote = perRack.flatMap((a, rr) => (rr === r ? [] : a.gw[p]));
      steps.push(`3. A gateway of cp${p} in ${r >= 0 ? 'another rack' : 'any rack'}: ${remote.length ? `b${remote.join(', b')}` : 'none'}.`);
      if (remote.length) { pick = logKeyPick(LOG_KEY, remote); step = 3; }
    }
    if (!pick) {
      const any = alive.map(b => b.id);
      steps.push(`4. Any broker: ${any.length ? 'yes' : 'none is running'}.`);
      if (any.length) { pick = logKeyPick(LOG_KEY, any); step = 4; }
    }
    return { perRack, pick, step, steps, r, p, all };
  };
  const render = () => {
    const c = compute();
    host.innerHTML = '';
    RACKS.forEach((name, r) => {
      const panel = h('div', { class: 'panel' + (r === c.r ? ' changed' : '') }, h('h6', null, `Rack ${name}${r === c.r ? ' · the client\'s rack' : ''}`));
      for (const b of c.all.filter(x => x.rack === r)) {
        const st = state.get(b.id);
        const set = c.perRack[r].gw.map((l, cp) => (l.includes(b.id) ? cp : -1)).filter(cp => cp >= 0);
        const btn = h('button', { class: 'rt-broker' + (b.id === c.pick ? ' picked' : '') + (st !== 'running' ? ' off' : ''),
          title: 'Click to cycle: running, fenced, excluded',
          onclick: () => { state.set(b.id, { running: 'fenced', fenced: 'excluded', excluded: 'running' }[st]); render(); },
          html: `<b>b${b.id}</b> ${st === 'running' ? (set.length ? set.map(cpChip).join(' ') : '<span class="muted">spare</span>') : chip(st === 'fenced' ? 'bad' : 'warn', st)}` });
        panel.append(btn);
      }
      host.append(panel);
    });
    $('#rt-steps').innerHTML = `<h6>Fallback order for ${LOG_KEY} on cp${c.p}</h6>` + c.steps.map((s, i) =>
      `<div class="small">${i === c.steps.length - 1 && c.pick ? chip('good', 'match') : chip('', 'no match')} ${s}</div>`).join('');
    const pickB = c.all.find(b => b.id === c.pick);
    let text, cls = 'good';
    if (!pickB) { text = 'No broker can take the request.'; cls = 'bad'; }
    else {
      const cross = c.r < 0 ? 'The client\'s AZ is unknown, so about 2 picks in 3 cross an AZ under any rule.' : pickB.rack !== c.r ? `<b>The bytes cross from rack ${RACKS[c.r]} to rack ${RACKS[pickB.rack]}</b>, but only to a gateway that already sends a part for cp${c.p}.` : 'The bytes stay in the client\'s AZ.';
      const extra = c.step === 2 || c.step === 4 ? ' The receiving broker isn\'t a gateway of cp' + c.p + ', so this costs an extra commit part.' : ' No extra commit part.';
      text = `Metadata names <b>b${c.pick}</b> (step ${c.step}). ${cross}${extra}`;
      if (c.step !== 1) cls = 'warn';
    }
    const key = `${c.r}/${c.p}/${sizeSel.value}`;
    if (prevPick && prevPick.key === key && prevPick.id !== c.pick && c.pick) {
      const old = prevPick.id;
      text += state.get(old) === 'fenced'
        ? ` <br>A client that was using b${old}: b${old} is fenced, so its connections dropped. The client refreshes its metadata and reaches b${c.pick}.`
        : ` <br>A client with cached metadata keeps sending to b${old}. b${old} isn't cp${c.p}'s gateway anymore, so it rejects the first batch on that connection with <code>NOT_LEADER_OR_FOLLOWER</code>, before writing it. The client refreshes and moves to b${c.pick}. Further stale batches within <code>metadata.max.age.ms</code> commit at b${old} with an extra part.`;
    }
    prevPick = { key, id: c.pick };
    show($('#rt-readout'), cls, text);
  };
  rackSel.addEventListener('change', () => { prevPick = null; render(); });
  cpSel.addEventListener('change', () => { prevPick = null; render(); });
  sizeSel.addEventListener('change', reset);
  $('#rt-reset').addEventListener('click', reset);
  reset();
}

/* ---------------------------------------------------------------- the sticky tracking rule */
function demoTrack() {
  const stateEl = $('#tk-state');
  if (!stateEl) return;
  const N = 8, ME = 1;
  const fixed = toPositive(murmur2(be32(ME))) % N;
  let st;
  const top = (set, cur) => (set.length ? byScore(set, ME)[0] : cur);
  const reset = () => {
    const members = [1, 2, 3];
    const set = gatewaySet(members, N, ME);
    const start = set.length ? byScore(set, ME)[0] : byScore(Array.from({ length: N }, (_, i) => i), ME)[0];
    st = { members, nextId: 4, set, current: start, preferred: start, stale: false, staleCp: null, sticky: 0, highest: 0, highCur: start,
      records: 0, files: 0, partsSent: 0, fixedExtra: 0 };
    render('Broker 1 starts its epoch and takes the highest-scoring member of its gateway set as its tracking partition.');
  };
  const prefer = () => (st.set.includes(st.current) ? st.current : st.set.length ? byScore(st.set, ME)[0] : st.current);
  const change = (members, what) => {
    const before = st.set.slice();
    st.members = members;
    st.set = gatewaySet(members, N, ME);
    const lost = before.filter(cp => !st.set.includes(cp));
    if (lost.length) { st.stale = true; st.staleCp = lost; }
    st.preferred = prefer();
    const hi = top(st.set, st.highCur);
    if (hi !== st.highCur) { st.highest++; st.highCur = hi; }
    const gained = st.set.filter(cp => !before.includes(cp));
    render(`${what} Broker 1 ${gained.length ? `gains cp${gained.join(', cp')}` : ''}${gained.length && lost.length ? ' and ' : ''}${lost.length ? `loses cp${lost.join(', cp')}` : ''}${!gained.length && !lost.length ? 'keeps the same gateway set' : ''}. ` +
      (st.preferred !== st.current ? `<b>Its preferred tracking partition becomes cp${st.preferred}</b>, but it waits for the first file with no batch for cp${st.current}.` : `Its tracking partition cp${st.current} stays: ${st.set.includes(st.current) ? 'it\'s still in the set' : 'the set is empty, so it keeps the current one'}.`));
  };
  const closeFile = () => {
    st.files++;
    const routed = new Set(st.set);
    if (st.stale && st.staleCp) st.staleCp.forEach(cp => routed.add(cp));
    const hasCurrent = routed.has(st.current);
    let msg;
    if (st.preferred !== st.current && !hasCurrent) {
      const old = st.current;
      st.current = st.preferred; st.sticky++; st.records += 18;
      routed.add(st.current);
      msg = `<b>Switch.</b> The file holds no batch for cp${old}, so the broker renews at cp${st.current} with the reason <code>assignment</code>, switches, and sends a lease release to cp${old}. About 18 records, including the merged lease-end fences.`;
    } else if (st.preferred !== st.current) {
      msg = `Clients with cached metadata still send cp${st.current}'s traffic here, so the file holds cp${st.current} batches and its tracking part isn't empty. <b>No switch yet</b>: switching now would save nothing.`;
    } else {
      msg = 'Routine file. The tracking part goes to a partition the file already sends batches to.';
    }
    const parts = new Set(routed); parts.add(st.current);
    st.partsSent += parts.size;
    if (!routed.has(fixed)) st.fixedExtra++;
    render(`${msg} This file sent ${parts.size} part${parts.size === 1 ? '' : 's'}${routed.has(fixed) ? '' : `. With the tracking partition fixed at cp${fixed}, it would have sent ${parts.size + (parts.has(fixed) ? 0 : 1)}`}.`);
  };
  const render = msg => {
    const order = [st.preferred, ...byScore(st.set.filter(cp => cp !== st.preferred), ME),
      ...byScore(Array.from({ length: N }, (_, i) => i).filter(cp => cp !== st.preferred && !st.set.includes(cp)), ME)];
    stateEl.innerHTML = `<h6>Broker 1 in a rack of ${st.members.length}</h6><dl class="kv">` +
      `<dt>rack members</dt><dd>b${st.members.join(', b')}</dd>` +
      `<dt>gateway set</dt><dd>${st.set.length ? byScore(st.set, ME).map(cpChip).join(' ') : 'empty: a spare'}</dd>` +
      `<dt>tracking now</dt><dd>${cpChip(st.current)}</dd>` +
      `<dt>preferred</dt><dd>${cpChip(st.preferred)}${st.preferred !== st.current ? ' ' + chip('warn', 'switch pending') : ''}</dd>` +
      `<dt>renewal list</dt><dd>${order.map(cp => `cp${cp}`).join(' → ')}</dd>` +
      `<dt>stale clients</dt><dd>${st.stale ? `still send cp${st.staleCp.join(', cp')}` : 'none'}</dd></dl>`;
    $('#tk-compare').innerHTML = '<h6>Compare the rules</h6><dl class="kv">' +
      `<dt>sticky rule switches</dt><dd>${st.sticky}</dd>` +
      `<dt>"always highest" switches</dt><dd>${st.highest}</dd>` +
      `<dt>files closed</dt><dd>${st.files}</dd>` +
      `<dt>parts sent</dt><dd>${st.partsSent}</dd>` +
      `<dt>fixed at cp${fixed}: extra parts</dt><dd>${st.fixedExtra}</dd>` +
      `<dt>switch records</dt><dd>${st.records}</dd></dl>`;
    show($('#tk-readout'), /Switch\./.test(msg) ? 'good' : /No switch/.test(msg) ? 'warn' : '', msg);
  };
  $('#tk-add').addEventListener('click', () => { const id = st.nextId++; change([...st.members, id], `Broker ${id} joins the rack.`); });
  $('#tk-remove').addEventListener('click', () => {
    const others = st.members.filter(b => b !== ME);
    if (!others.length) return render('Broker 1 is the only broker left in the rack.');
    const gone = others[others.length - 1];
    change(st.members.filter(b => b !== gone), `Broker ${gone} leaves the rack.`);
  });
  $('#tk-refresh').addEventListener('click', () => { st.stale = false; st.staleCp = null; render('Clients refresh their metadata and send each coordinator partition\'s traffic to its current gateway.'); });
  $('#tk-file').addEventListener('click', closeFile);
  $('#tk-reset').addEventListener('click', reset);
  reset();
}

/* ---------------------------------------------------------------- where routing stops helping */
function demoLoad() {
  const tabs = $('#ld-tabs');
  if (!tabs) return;
  const controls = $('#ld-controls'), view = $('#ld-view');
  const bar = (label, value, max, color, text) => h('div', { class: 'bar-row', style: { gridTemplateColumns: '12rem 1fr' } },
    h('span', { class: 'small' }, label),
    h('div', { class: 'bar-track' }, h('div', { class: 'bar-seg', style: { left: 0, width: Math.min(100, (value / max) * 100) + '%', background: color } }, text)));
  const mix = () => {
    controls.innerHTML = '';
    const sel = h('select', null, ...[['0', 'none'], ['1', '1%'], ['2', '2%'], ['5', '5%'], ['10', '10%'], ['all', 'consolidation on for every topic']].map(([v, t]) => h('option', { value: v }, t)));
    controls.append(h('label', null, 'Traffic on consolidating topics ', sel));
    const parts = { 0: 1.0, 1: 2.1, 2: 3.1, 5: 5.1, 10: 6.8, all: 8 };
    const draw = () => {
      const v = parts[sel.value];
      view.innerHTML = '<h6><span>Parts per file at 24 brokers, <code>N = 8</code></span></h6>';
      view.append(bar('Routing on', v, 8, 'var(--accent)', `${v} parts`), bar('Routing off', 8, 8, 'var(--faint)', '8 parts'));
      show($('#ld-readout'), v >= 5 ? 'warn' : v > 1 ? '' : 'good', sel.value === 'all'
        ? 'With consolidation on, every new diskless topic also gets remote storage, so every partition needs a local log and keeps replica routing. Every file sends about 8 parts, and gateways save nothing. The per-read rule in the future-work list would fix this.'
        : `Replica-routed traffic spreads over all 8 coordinator partitions, so even a small share adds parts to every gateway's files. These numbers assume equal traffic per partition.`);
    };
    sel.addEventListener('change', draw);
    draw();
  };
  const loadTab = () => {
    controls.innerHTML = '';
    const slider = h('input', { type: 'range', min: 1, max: 16, value: 7 });
    const out = h('b', null, '7');
    controls.append(h('label', null, 'Brokers per rack ', slider, out));
    const simulated = { 3: 1.0, 4: 1.0, 5: 1.2, 6: 1.4, 7: 1.6, 8: 1.0, 12: 1.4, 15: 1.7, 16: 1.0 };
    const N = 8;
    const draw = () => {
      const b = +slider.value;
      out.textContent = b;
      const ids = Array.from({ length: b }, (_, i) => i + 1);
      const { load, g } = assignRack(ids, N);
      const shares = ids.map(id => load.get(id) / (N * g));
      const maxShare = Math.max(...shares);
      view.innerHTML = `<h6><span>Share of the rack's traffic per broker, ${b} broker${b === 1 ? '' : 's'}, <code>N = 8</code></span></h6>`;
      ids.forEach((id, i) => view.append(bar(`b${id}: ${load.get(id)} cp${load.get(id) === 1 ? '' : 's'}`, shares[i], Math.max(maxShare, 1 / b), shares[i] === maxShare ? 'var(--warn)' : 'var(--accent)', `${(shares[i] * 100).toFixed(1)}%`)));
      view.append(bar('Today, per broker', 1 / b, Math.max(maxShare, 1 / b), 'var(--faint)', `${(100 / b).toFixed(1)}%`));
      const ratio = maxShare * b;
      show($('#ld-readout'), ratio >= 1.3 ? 'warn' : 'good',
        `The busiest gateway carries about <b>${ratio.toFixed(2)} times</b> an even share of the rack.` +
        (simulated[b] ? ` The design's simulation, which also counts today's uneven hash, puts it at <b>${simulated[b].toFixed(1)} times today's busiest broker</b>.` : '') +
        (ratio >= 1.3 ? ' Levers: racks of up to 4 brokers or a multiple of 8, or the routing switch off.' : ' Racks of up to 4 brokers or a multiple of 8 stay even.'));
    };
    slider.addEventListener('input', draw);
    draw();
  };
  const all = [['Partitions that need a local log', mix], ['A hot gateway', loadTab]];
  const btns = all.map(([name, fn], i) => {
    const b = h('button', { onclick: () => { setOn(btns, b); fn(); } }, name);
    tabs.append(b);
    return b;
  });
  setOn(btns, btns[0]);
  mix();
}

/* ---------------------------------------------------------------- load bounds */
function demoBounds() {
  const slider = $('#bd-b');
  if (!slider) return;
  const caseSel = $('#bd-case'), N = 8;
  const draw = () => {
    const B = +slider.value;
    $('#bd-b-v').textContent = B;
    const userParts = 64 * B / N;
    const limits = [['Commit parts', B], ['Lease requests', B], ['Fence requests', N - 1], ['Release reports', N - 1], ['Read parts', 8 * B]];
    const raw = {
      steady: [0, 0, 0, 0, 0],
      storm: [0, 0, 0, 0, userParts * 3],
      recover: [4 * 120 * B, 0, 60 * B, 0, 0],
      retention: [0, 0, 0, 4 * 86400 * B / N, 0],
    }[caseSel.value];
    const max = Math.max(...limits.map(l => l[1]), ...raw);
    const w = v => (Math.log10(v + 1) / Math.log10(max + 1)) * 100;
    const host = $('#bd-bars');
    host.innerHTML = '<div class="small muted" style="margin-bottom:0.3rem">Log scale. Blue: the most that can wait with the limits. Gray: what this situation would pile up without them.</div>';
    limits.forEach(([name, lim], i) => {
      const track = h('div', { class: 'bar-track' });
      if (raw[i] > lim) track.append(h('div', { class: 'bar-seg', style: { left: 0, width: w(raw[i]) + '%', background: 'var(--faint)', justifyContent: 'flex-end', paddingRight: '0.4rem' } }, fmt(raw[i])));
      track.append(h('div', { class: 'bar-seg', style: { left: 0, width: w(lim) + '%', background: 'var(--accent)' } }, `≤ ${fmt(lim)}`));
      host.append(h('div', { class: 'bar-row', style: { gridTemplateColumns: '9rem 1fr' } }, h('span', { class: 'small' }, name), track));
    });
    const text = {
      steady: 'In steady state, the limits rarely bind. Each broker has one commit part in flight per coordinator partition, and reads mostly hit the cache.',
      storm: `Every consumer restarts at once, so every cache miss reaches the leaders together. Without a limit, about ${fmt(userParts * 3)} read parts could arrive for this coordinator partition, assuming 3 consumer groups. With 8 slots per calling broker, at most ${fmt(8 * B)} are in flight, and the rest wait on the callers. Reads run in the read pool, so commits don't wait behind them.`,
      recover: `During a 2-minute outage, brokers keep closing 4 files per second. Without the per-queue limit, ${fmt(4 * 120 * B)} commit parts would land on the returning leader at once. With it, each broker has one part in flight, and queued parts expire at their own deadline. Pending fence ranges from every other tracking partition merge into at most ${N - 1} requests.`,
      retention: `A day of objects expires at once. Without batching, each released object would be its own report. With one report in flight per participant and tracking partition, each tracking partition receives at most ${N - 1} reports at a time, each carrying many ranges.`,
    }[caseSel.value];
    show($('#bd-readout'), caseSel.value === 'steady' ? '' : 'good', text);
  };
  slider.addEventListener('input', draw);
  caseSel.addEventListener('change', draw);
  draw();
}

/* ---------------------------------------------------------------- benchmark plan */
function demoBench() {
  const tabs = $('#bn-tabs');
  if (!tabs) return;
  const benches = [
    ['Partition count', 'Runs first. The base workload at 3 brokers with <code>N</code> of 1, 2, 4, and 8, and at 12 and 24 brokers with <code>N</code> of 4, 8, and 16.',
      '<code>N = 8</code> passes the 1.1 band at all three sizes.', 'If 8 fails only at 3 brokers, the docs recommend a smaller <code>N</code> for small clusters. If it fails at 12 or 24, the default becomes the largest <code>N</code> that passes at both.'],
    ['Commit throughput', 'A rate sweep from 16 MiB/s per broker in 8 MiB/s steps, and a broker sweep from 24 to 96 brokers.',
      'The coordinator saturates at no less than PostgreSQL\'s rate divided by 1.1, and at no fewer brokers.', 'The saturation point goes into the docs\' guidance for choosing <code>N</code>. A failure ships the coordinator as opt-in.'],
    ['Partial failure', 'One coordinator partition accepts nothing for 5 minutes, then delays every append by 500 ms for 5 minutes.',
      'Requests that don\'t touch it keep P99 within 1.1 times with zero errors. Requests that do complete within one commit deadline.', 'Any miss is a defect that blocks release.'],
    ['Read-heavy and cache storm', 'A lagging consumer group per broker that misses the cache, then every consumer restarting at once.',
      '<code>FindBatchesTime</code> and produce latency within 1.1 times PostgreSQL\'s, and recovery time within 1.1 times.', 'If the read pool\'s queue dominates, rerun with 8 and 16 read threads and take the smallest count that passes.'],
    ['Co-tenancy', 'The base workload plus a classic topic at 16 MiB/s per broker with <code>acks=all</code>.',
      'Brokers that lead coordinator partitions keep classic and diskless latency within 1.1 times the brokers that lead none.', 'The coordinator gets a required separate disk, and the test reruns.'],
    ['Failover', 'A <code>kill -9</code> and a controlled shutdown of a coordinator leader, against a hard kill and a switchover of the PostgreSQL primary.',
      'Time to the first commit and time to steady P99, each within 1.1 times PostgreSQL\'s, and zero acknowledged records lost.', 'A lost record is a defect that blocks release.'],
    ['State size and snapshots', 'From a million to a billion batch rows, and followers that lose their storage.',
      'Latency and election time within 1.1 times PostgreSQL\'s at the same row count.', 'The largest passing size becomes the documented limit on rows per coordinator partition.'],
    ['Gateway pairs', 'Routing on and off at 12, 21, 24, 45, and 48 brokers, plus clients without <code>diskless_az</code>, a local-log mix, a rolling restart, and a switch flip.',
      'The on side is within 1.1 times the off side, on the cluster and on the busiest gateway, and parts per file match the prediction.', 'Fails only at 21 or 45: routing stays on and the docs name the load sizes. Fails at 12, 24, or 48: routing defaults to off.'],
    ['Inter-AZ traffic', 'Cross-AZ bytes per produced MB, by commit parts, follower copies, and control RPCs.',
      'At most 1.1 times PostgreSQL\'s.', 'Doesn\'t block release. The docs state the gap.'],
  ];
  const view = $('#bn-view');
  const btns = benches.map(([name, runs, pass, change], i) => {
    const b = h('button', { onclick: () => {
      setOn(btns, b);
      view.innerHTML = '';
      [['What it runs', runs], ['How it passes', pass], ['What a failure changes', change]].forEach(([t, x]) =>
        view.append(h('div', { class: 'panel', html: `<h6>${t}</h6><p class="small" style="margin:0">${x}</p>` })));
    } }, name);
    tabs.append(b);
    return b;
  });
  btns[0].click();
}

/* ---------------------------------------------------------------- glossary */
function buildGlossary() {
  const body = $('#glossary');
  if (!body) return;
  const terms = [
    ['Abandoned range', 'A leased range the broker stopped using after switching to another tracking partition. A lease release ends it.'],
    ['Barrier record', 'An empty record a leader appends and waits for before it answers "offset out of range," to prove it\'s still the leader.'],
    ['Blast radius', 'The share of diskless partitions one failure can reach. For one coordinator partition, about 1/N of every diskless topic.'],
    ['Broker epoch', 'A number KRaft gives a broker each time it registers: the offset of its registration record. A restart gives a higher epoch.'],
    ['Broker-epoch-retired record', 'The record that says every epoch at or below <code>retiredThrough</code> is gone for one broker ID.'],
    ['Broker row', 'A coordinator partition\'s state for one uploading broker: its last committed pair and result, its fenced-through mark, and its retired-through epoch.'],
    ['Commit part', 'The share of one file\'s batches that maps to one coordinator partition, sent to that partition\'s leader.'],
    ['Coordinator leader', 'The leader of a <code>__diskless_metadata</code> partition. It commits, fences, and answers reads for that partition.'],
    ['Coordinator partition', 'One partition of <code>__diskless_metadata</code>. Each is an independent coordinator for the user partitions that hash to it.'],
    ['Deletion queue', 'The keys a tracking partition has proven unreferenced, waiting for <code>FileCleaner</code> and the grace period.'],
    ['Deposed leader', 'A leader that was replaced but hasn\'t learned it yet. It answers from older state.'],
    ['Entry states', 'The states of a tracking entry for one participant: pending, fencing, held, and released. They only move forward, and released is final.'],
    ['Exception range', 'The part of a finish report that names a participant that didn\'t answer. The tracking partition fences it.'],
    ['Fence', 'A request that asks a partition which sequences of a range it holds, and raises its floor so the rest can never commit there.'],
    ['Fenced-through mark', 'The highest pair a fence has covered in a broker row.'],
    ['Finish report', 'The broker\'s statement of <code>finishedThrough</code> and its exception ranges, carried on tracking parts, lease requests, keepalives, and lease releases.'],
    ['<code>finishedThrough</code>', 'Every file of the broker epoch at or below this sequence is finished: every part committed, failed for good, or passed its deadline.'],
    ['Floor', 'The highest of a broker row\'s last commit, fenced-through mark, and retired epoch. A new commit needs a pair above it.'],
    ['Gateway', 'A broker that one rack\'s clients use for one coordinator partition\'s user partitions. It holds no coordinator state.'],
    ['Gateway set', 'The coordinator partitions a broker is a gateway for in its own rack.'],
    ['Grace period', 'One minute between queuing a key and deleting it, so readers with cached coordinates can finish.'],
    ['Idle timer', 'A tracking partition\'s 15-minute backstop that ends the lease of a broker it hears nothing from.'],
    ['Kafka parity', 'Diskless topics survive whatever a classic topic with the same replication settings survives, and lose no more when it doesn\'t.'],
    ['Keepalive', 'A lease request for a range the broker already holds, sent after half the idle timeout without an answer.'],
    ['Lease', 'A range of sequence numbers a tracking partition grants a broker for one epoch, before the broker uses them.'],
    ['Lease release', 'The broker\'s request that ends its abandoned ranges at a tracking partition, once no file under them is unfinished.'],
    ['Local-log partition', 'A diskless partition whose fetches need a broker-local log, such as a consolidating partition. It keeps replica routing.'],
    ['Maximal spread', 'Replicas placed across as many racks as the replication factor and the rack count allow.'],
    ['Misroute', 'A produce that reaches a broker that isn\'t the partition\'s gateway in its rack. It costs an extra part.'],
    ['Missed sequence', 'A leased sequence the tracking partition never recorded, once a finish report reaches it. It\'s fenced at every partition.'],
    ['Overlay', 'The leader\'s in-memory view of commits that are appended but not yet applied, so it can assign offsets without waiting.'],
    ['Participant', 'A coordinator partition that holds batches of an object.'],
    ['Release', 'A participant\'s statement that it holds no batch of an object anymore.'],
    ['Saturation', 'A rack with at least <code>N</code> brokers, where each gateway serves exactly one coordinator partition.'],
    ['Sequence', 'The number a broker gives each file within an epoch. With the epoch, it names the object and orders the broker\'s commits.'],
    ['Spare', 'A broker with an empty gateway set.'],
    ['Sticky rule', 'Keep the current tracking partition while it\'s in the gateway set. Otherwise, take the highest-scoring member.'],
    ['Tracking part', 'The part of a file sent to its tracking partition. It carries the participant set and the finish report, even with no batches.'],
    ['Tracking partition', 'The coordinator partition that leases a broker\'s sequences, tracks who references each of its objects, and deletes them.'],
    ['Unreconciled partition', 'A user partition a call names before the leader has recorded its creation. The call gets a retriable error.'],
    ['WAL file', 'One object in storage holding batches for many partitions, uploaded by one broker per commit interval.'],
  ];
  terms.sort((a, b) => a[0].replace(/<[^>]+>/g, '').localeCompare(b[0].replace(/<[^>]+>/g, ''), 'en', { sensitivity: 'base' }));
  for (const [t, d] of terms) body.append(h('tr', null, h('td', { html: `<b>${t}</b>` }), h('td', { html: d })));
}
