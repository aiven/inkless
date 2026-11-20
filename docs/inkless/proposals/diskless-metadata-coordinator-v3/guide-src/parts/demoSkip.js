/* ---------------------------------------------------------------- stop, never skip */
function demoSkip() {
  const host = $('#sk2-replicas');
  if (!host) return;
  const skipBtn = $('#sk2-skip'), stopBtn = $('#sk2-stop'), upBtn = $('#sk2-upgrade');
  const records = [
    { text: 'file committed: orders-0 0–99 in X', v: 0, apply: t => t.push('orders-0 0–99 → X') },
    { text: 'retention applied (record version 1): orders-0 below 100', v: 1, apply: t => { const i = t.indexOf('orders-0 0–99 → X'); if (i >= 0) t.splice(i, 1); } },
    { text: 'file committed: orders-0 100–149 in Y', v: 0, apply: t => t.push('orders-0 100–149 → Y') },
    { text: 'files deleted: X', v: 0, apply: () => {} },
  ];
  let mode = 'stop', upgraded = false;
  const replay = (known, skip) => {
    const tables = [];
    let applied = 0, stoppedAt = null, skipped = [];
    for (let k = 0; k < records.length; k++) {
      const r = records[k];
      if (r.v > known) {
        if (skip) { skipped.push(k); applied = k + 1; continue; }
        stoppedAt = k;
        break;
      }
      r.apply(tables);
      applied = k + 1;
    }
    return { tables, applied, stoppedAt, skipped };
  };
  const render = () => {
    const b3Known = upgraded && mode === 'stop' ? 1 : 0;
    const states = [replay(1, false), replay(1, false), replay(b3Known, mode === 'skip')];
    host.innerHTML = '';
    const names = ['Broker 1 (leader, new binary)', 'Broker 2 (new binary)', upgraded ? 'Broker 3 (upgraded)' : 'Broker 3 (old binary)'];
    states.forEach((s, i) => {
      const status = s.stoppedAt != null
        ? chip('warn', `stopped at offset ${s.stoppedAt}`)
        : s.skipped.length ? chip('bad', 'diverged') : chip('good', 'in step');
      const log = records.map((r, k) => {
        const cls = s.skipped.includes(k) ? 'bad-text' : s.stoppedAt != null && k >= s.stoppedAt ? 'muted' : '';
        return `<li class="${cls}">${r.text}${s.skipped.includes(k) ? ' (skipped)' : s.stoppedAt === k ? ' (unknown version)' : ''}</li>`;
      }).join('');
      host.append(h('div', { class: 'panel' + (i === 2 ? ' changed' : ''), html:
        `<h6>${names[i]} ${status}</h6>` +
        `<div class="small muted">Log (all ${records.length} records fetched)</div><ol class="small mono" start="0" style="margin:0;padding-left:1.3rem">${log}</ol>` +
        `<div class="small muted" style="margin-top:0.35rem">SQLite batches</div><div class="small mono">${s.tables.join('<br>') || 'none'}</div>` }));
    });
    const b3 = states[2];
    let cls, text;
    if (b3.skipped.length) {
      cls = 'bad';
      text = 'Broker 3 skipped the retention record, so it still has a batch row for offsets 0–99 in object X, which was deleted. Its state differs from the others forever. If it becomes leader, it serves batches whose object is gone, and it reports a different earliest offset.' +
        (upgraded ? ' Upgrading doesn\'t help: the replica already moved past the record.' : '');
    } else if (b3.stoppedAt != null) {
      cls = 'warn';
      text = 'Broker 3 stopped applying at offset 1, the record it can\'t read. Its SQLite stays at offset 0. It doesn\'t serve reads or lead, but it keeps fetching, so it stays in sync and loses nothing. A metric and an error log name the record.';
    } else {
      cls = 'good';
      text = 'After the upgrade, broker 3 resumed at offset 1 and applied the rest. All three replicas have the same tables.';
    }
    show($('#sk2-readout'), cls, text);
  };
  skipBtn.addEventListener('click', () => { mode = 'skip'; upgraded = false; setOn([skipBtn, stopBtn], skipBtn); render(); });
  stopBtn.addEventListener('click', () => { mode = 'stop'; upgraded = false; setOn([skipBtn, stopBtn], stopBtn); render(); });
  upBtn.addEventListener('click', () => { upgraded = !upgraded; upBtn.textContent = upgraded ? 'Downgrade broker 3 again' : 'Upgrade broker 3'; render(); });
  render();
}
