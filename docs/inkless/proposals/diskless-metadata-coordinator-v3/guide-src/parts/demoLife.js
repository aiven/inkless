/* ---------------------------------------------------------------- the object lifecycle */
function demoLife() {
  const cpsHost = $('#lf-cps');
  if (!cpsHost) return;
  const C = 'MkU3';
  const key = s => `${C}.3.7.${s}`;
  const init = () => ({
    cps: [0, 1, 2, 3].map(i => ({ floor: null, batches: [], records: [], extra: i === 2 ? { leases: [], tracking: [], queue: [] } : null })),
    storage: [],
  });
  const rec = (s, i, r) => { s.cps[i].records.push(r); };
  const commit = (s, i, seq, batch) => {
    s.cps[i].floor = maxPair(s.cps[i].floor, [7, seq]);
    if (batch) s.cps[i].batches.push(`${batch} in ${key(seq)}`);
    rec(s, i, `file committed (7, ${seq})`);
  };
  const removeBatches = (s, i, seq, record) => {
    s.cps[i].batches = s.cps[i].batches.filter(b => !b.endsWith(key(seq)));
    rec(s, i, record);
    rec(s, i, `release accepted for ${key(seq)}`);
  };
  const track = (s, seq) => s.cps[2].extra.tracking.find(t => t.seq === seq);
  const release = (s, seq, who) => {
    const t = track(s, seq);
    t.released.push(who);
    rec(s, 2, `release recorded: cp${who}`);
  };
  const queueIfDone = (s, seq) => {
    const t = track(s, seq);
    if (t.participants.every(p => t.released.includes(p))) {
      s.cps[2].extra.tracking = s.cps[2].extra.tracking.filter(x => x !== t);
      s.cps[2].extra.queue.push(key(seq));
      return true;
    }
    return false;
  };
  const upload = (s, seq) => s.storage.push({ key: key(seq), state: 'stored' });
  const lease = { title: 'Lease the numbers', text: 'Broker 3 asks its tracking partition, cp2, for a lease. cp2 appends a sequences-leased record for <code>[0, 15]</code> in epoch 7. This happens in the background, long before any file closes.',
    run: s => { s.cps[2].extra.leases.push('broker 3, epoch 7: [0, 15]'); rec(s, 2, 'sequences leased [0, 15]'); } };
  const deleteStep = (keys, extraText) => ({ title: 'Grace period, then delete',
    text: `After the one-minute grace period, <code>FileCleaner</code> on cp2's leader deletes the queued key${keys.length > 1 ? 's' : ''} from storage and appends a files-deleted record.${extraText || ''}`,
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
      lease,
      { title: 'Close a file', text: `The file closes with sequence 0 from the lease, so its key is <code>${key(0)}</code>. No request is sent: the lease already put every possible key on record.` },
      { title: 'Upload', text: 'The broker uploads the object under its predictable key.', run: s => upload(s, 0) },
      { title: 'Send the commit parts', text: 'Parts go to cp0, cp1, and cp3 with their batches. A fourth part goes to cp2, the tracking partition. It carries no batches, only the participant set. Each partition raises broker 3\'s floor to <code>(7, 0)</code>.',
        run: s => {
          commit(s, 0, 0, 'orders-0'); commit(s, 1, 0, 'clicks-1'); commit(s, 3, 0, 'logs-2'); commit(s, 2, 0, null);
          s.cps[2].extra.tracking.push({ seq: 0, participants: [0, 1, 3], released: [] });
        } },
      { title: 'cp0: retention removes the batches', text: 'Retention removes the <i>orders-0</i> batches. In the same SQLite transaction, cp0 checks the index on <code>batches(file_id)</code>: nothing points into the object any more. It reports a release to cp2, and then records that the release was accepted.',
        run: s => { removeBatches(s, 0, 0, 'retention applied'); release(s, 0, 0); } },
      { title: 'cp1: DeleteRecords', text: 'A <code>DeleteRecords</code> request removes the <i>clicks-1</i> batches. cp1 releases the object the same way.',
        run: s => { removeBatches(s, 1, 0, 'records deleted'); release(s, 0, 1); } },
      { title: 'cp3: consolidation prunes the batches', text: 'Consolidation copied the <i>logs-2</i> batches into a classic log, and pruning removes them from cp3. cp3 releases. Every participant is now released, so applying this release puts the key in cp2\'s deletion queue.',
        run: s => { removeBatches(s, 3, 0, 'log pruned'); release(s, 0, 3); queueIfDone(s, 0); } },
      deleteStep([key(0)], ' Sequences 1 to 15 stay leased for the next files.'),
    ] },
    { name: 'Crash before commit', steps: [
      lease,
      { title: 'Close and upload', text: `The file closes with sequence 0 and uploads as <code>${key(0)}</code>.`, run: s => upload(s, 0) },
      { title: 'The broker crashes', text: 'Broker 3 crashes before it sends any part. The object is in storage, and no partition holds batches in it: an orphan. But cp2 leased its sequence, so its key is already on record.' },
      { title: 'The broker returns with a new epoch', text: 'Broker 3 registers again and gets epoch 8. Each leader appends broker-epoch-retired once its metadata shows the new epoch, so the rows move to epoch 8. At cp2, this ends the epoch 7 lease. The whole range <code>[0, 15]</code> is leftover, because cp2 recorded none of it.',
        run: s => { for (let i = 0; i < 4; i++) rec(s, i, 'broker epoch retired (3, 7)'); s.cps[2].extra.leases = ['broker 3, epoch 7: ended, leftovers [0, 15]']; } },
      { title: 'Fence the leftovers everywhere', text: 'Each partition applies the retirement at its own pace, so cp2 doesn\'t rely on it. It sends a range fence for <code>[0, 15]</code> in epoch 7 to every partition. Each one appends sequences-fenced, raises fenced-through to <code>(7, 15)</code>, and answers that it holds nothing.',
        run: s => { for (let i = 0; i < 4; i++) { s.cps[i].floor = maxPair(s.cps[i].floor, [7, 15]); rec(s, i, 'sequences fenced [0, 15]'); } } },
      { title: 'Queue all sixteen keys', text: 'No partition holds any of the sixteen keys, so cp2 puts them all in the deletion queue. Fifteen were never uploaded, which doesn\'t matter.',
        run: s => { for (let q = 0; q < 16; q++) s.cps[2].extra.queue.push(key(q)); } },
      deleteStep(Array.from({ length: 16 }, (_, q) => key(q)), ' One delete removes the real orphan. The other fifteen keys don\'t exist, and deleting a missing key counts as success.'),
    ] },
    { name: 'Some parts commit', steps: [
      lease,
      { title: 'Close and upload', text: `The file closes with sequence 0 and uploads as <code>${key(0)}</code>.`, run: s => upload(s, 0) },
      { title: 'Two parts commit, two time out', text: 'The parts to cp0 and cp2 commit, so cp2 has the tracking entry. The parts to cp1 and cp3 time out: their outcome is unknown, and those producers get a retriable error. The broker keeps running.',
        run: s => { commit(s, 0, 0, 'orders-0'); commit(s, 2, 0, null); s.cps[2].extra.tracking.push({ seq: 0, participants: [0, 1, 3], released: [] }); } },
      { title: 'cp0 releases', text: 'Retention removes cp0\'s batches and cp0 reports a release. cp2 holds no batches of its own. Now every participant cp2 has heard from is released, but cp1 and cp3 are still silent.',
        run: s => { removeBatches(s, 0, 0, 'retention applied'); release(s, 0, 0); } },
      { title: 'Fence the silent participants', text: 'cp2 sends a sequence fence for <code>(7, 0)</code> to cp1 and cp3. Neither holds batches from sequence 0, so both answer "released." If either late part is still in a queue, it now arrives below the floor and is rejected.',
        run: s => {
          for (const i of [1, 3]) { s.cps[i].floor = maxPair(s.cps[i].floor, [7, 0]); rec(s, i, 'sequences fenced [0, 0]'); }
          release(s, 0, 1); release(s, 0, 3); queueIfDone(s, 0);
        } },
      deleteStep([key(0)], ' Sequences 1 to 15 are still leased. When this lease ends, its unused part is fenced and queued like the leftovers in the crash scenario.'),
    ] },
    { name: 'Missed sequence', steps: [
      lease,
      { title: 'File 0: the tracking part fails', text: `The file with sequence 0 uploads as <code>${key(0)}</code>. Its parts to cp0, cp1, and cp3 commit, but the part to cp2 fails. cp2 has no tracking entry, so it doesn't know this file's participants.`,
        run: s => { upload(s, 0); commit(s, 0, 0, 'orders-0'); commit(s, 1, 0, 'clicks-1'); commit(s, 3, 0, 'logs-2'); } },
      { title: 'File 1 commits at cp2', text: `The next file, sequence 1, uploads as <code>${key(1)}</code> and commits at cp0 and cp2. cp2 now has <code>(7, 1)</code> but never recorded sequence 0: a gap.`,
        run: s => { upload(s, 1); commit(s, 0, 1, 'orders-0'); commit(s, 2, 1, null); s.cps[2].extra.tracking.push({ seq: 1, participants: [0], released: [] }); } },
      { title: 'Fence the missed sequence everywhere', text: 'cp2 doesn\'t know who took part in sequence 0, so it fences sequence 0 at all four partitions. cp0, cp1, and cp3 answer that they hold batches. cp2 builds the participant set from the answers.',
        run: s => {
          for (let i = 0; i < 4; i++) { s.cps[i].floor = maxPair(s.cps[i].floor, [7, 0]); rec(s, i, 'sequences fenced [0, 0]'); }
          s.cps[2].extra.tracking.unshift({ seq: 0, participants: [0, 1, 3], released: [] });
        } },
      { title: 'Track it like any other object', text: `From here, sequence 0 is an ordinary tracking entry. Over time, retention at cp0, cp1, and cp3 releases it, and cp2 queues <code>${key(0)}</code>.`,
        run: s => {
          removeBatches(s, 0, 0, 'retention applied');
          removeBatches(s, 1, 0, 'retention applied'); removeBatches(s, 3, 0, 'retention applied');
          release(s, 0, 0); release(s, 0, 1); release(s, 0, 3); queueIfDone(s, 0);
        } },
      deleteStep([key(0)], ` <code>${key(1)}</code> stays until cp0 releases it.`),
    ] },
  ];

  let current = 0;
  const tabs = $('#lf-tabs');
  const tabBtns = scenarios.map((sc, i) => {
    const b = h('button', null, sc.name);
    b.addEventListener('click', () => { current = i; setOn(tabBtns, b); ctl.go(0); });
    tabs.append(b);
    return b;
  });
  setOn(tabBtns, tabBtns[0]);
  const roles = ['participant', 'participant', 'tracking partition', 'participant'];
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
          html += `<dl class="kv" style="margin-top:0.3rem"><dt>leases</dt><dd>${c.extra.leases.join('<br>') || 'none'}</dd>` +
            `<dt>tracking</dt><dd>${c.extra.tracking.map(t => `${key(t.seq)}: needs ${t.participants.map(p => (t.released.includes(p) ? `<s>cp${p}</s>` : `cp${p}`)).join(', ')}`).join('<br>') || 'none'}</dd>` +
            `<dt>queue</dt><dd>${c.extra.queue.length > 3 ? `${c.extra.queue.length} keys: ${c.extra.queue[0]} … ${c.extra.queue[c.extra.queue.length - 1]}` : c.extra.queue.join('<br>') || 'empty'}</dd></dl>`;
        }
        const recent = c.records.slice(-4);
        html += `<div class="small muted" style="margin-top:0.35rem">Recent records</div>` +
          (recent.length ? `<ol class="small mono" start="${c.records.length - recent.length + 1}" style="margin:0;padding-left:1.4rem">${recent.map(r => `<li>${r}</li>`).join('')}</ol>` : '<div class="small muted">none</div>');
        cpsHost.append(h('div', { class: 'panel' + (changed ? ' changed' : ''), html }));
      });
      const stor = $('#lf-storage');
      const storChanged = prev && JSON.stringify(prev.storage) !== JSON.stringify(s.storage);
      stor.className = 'panel' + (storChanged ? ' changed' : '');
      if (!s.storage.length) { stor.innerHTML = '<span class="small muted">Empty</span>'; return; }
      const real = s.storage.filter(o => o.state !== 'missing');
      const missing = s.storage.filter(o => o.state === 'missing').length;
      stor.innerHTML = '<div class="storage-objects">' + real.map(o =>
        `<span class="chip ${o.state === 'deleted' ? 'bad' : 'info'} mono">${o.key} · ${o.state === 'deleted' ? 'deleted' : 'stored'}</span>`).join('') +
        (missing ? `<span class="chip">${missing} more deletes of keys that never existed</span>` : '') + '</div>';
    },
  });
}
