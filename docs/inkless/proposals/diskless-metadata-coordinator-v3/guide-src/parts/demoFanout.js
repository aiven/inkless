/* ---------------------------------------------------------------- commit fan-out */
function demoFanout() {
  const gantt = $('#fo-gantt');
  if (!gantt) return;
  const stateEl = $('#fo-state'), everyBtn = $('#fo-every'), singleBtn = $('#fo-single'), queuesBtn = $('#fo-queues');
  const FILES = 6, GAP = 250, UPLOAD = 100, HEALTHY = 20, DEADLINE = 2000;
  let every = true, single = false;

  const render = () => {
    const cp2 = stateEl.value;
    const dur = cp2 === 'healthy' ? HEALTHY : cp2 === 'slow' ? 600 : Infinity;
    const touches = i => every || i % 2 === 0;
    const rows = [];
    const healthyLat = [], cp2Out = [];
    let prevEnd = 0, prevCp2 = 0;
    for (let i = 0; i < FILES; i++) {
      const close = i * GAP, ready = close + UPLOAD, deadline = close + DEADLINE;
      const segs = [{ from: close, to: ready, color: 'var(--faint)', text: 'upload' }];
      if (single) {
        const start = Math.max(ready, prevEnd);
        if (start > ready) segs.push({ from: ready, to: start, color: 'var(--line)', ink: 'var(--muted)', text: 'waits for earlier files' });
        let end, failed = false;
        if (start >= deadline) { end = start; failed = true; }
        else if (touches(i)) {
          end = start + dur;
          if (end > deadline) { end = deadline; failed = true; }
        } else end = start + HEALTHY;
        segs.push({ from: start, to: Math.max(start + 15, end), color: failed ? 'var(--bad)' : touches(i) && cp2 !== 'healthy' ? 'var(--cp2)' : 'var(--good)', text: failed ? 'deadline: retriable error' : 'commit' });
        healthyLat.push(Math.min(start, deadline) + HEALTHY - close);
        if (touches(i)) cp2Out.push(failed ? 'failed' : end - close);
        prevEnd = end;
      } else {
        segs.push({ from: ready, to: ready + HEALTHY, color: 'var(--good)', text: '' });
        healthyLat.push(UPLOAD + HEALTHY);
        if (touches(i)) {
          const start = Math.max(ready, prevCp2);
          let end, failed = false;
          if (start >= deadline) { end = start; failed = true; }
          else { end = start + dur; if (end > deadline) { end = deadline; failed = true; } }
          if (start > ready) segs.push({ from: ready + HEALTHY, to: start, color: 'color-mix(in srgb, var(--cp2) 22%, transparent)', ink: 'var(--ink)', text: 'cp2 part queued' });
          segs.push({ from: start, to: Math.max(start + 15, end), color: failed ? 'var(--bad)' : 'var(--cp2)', text: failed ? 'cp2: deadline' : 'cp2 part' });
          cp2Out.push(failed ? 'failed' : end - close);
          prevCp2 = end;
        }
      }
      rows.push({ label: `file ${i}`, segs });
    }
    const maxT = 4000;
    drawGantt(gantt, rows, maxT, 500, ' ms');
    const failedCount = cp2Out.filter(x => x === 'failed').length;
    const worstHealthy = Math.max(...healthyLat);
    let text;
    if (single) {
      text = `With one commit thread, every file waits for the file before it, including the cp2 part. ` +
        `Partitions on cp0, cp1, and cp3 wait up to <b>${fmt(worstHealthy)} ms</b> after the file closes for their answer` +
        (cp2 === 'healthy' ? '.' : `, although their own coordinator partitions are healthy.`);
    } else {
      text = `With one queue per coordinator partition, partitions on cp0, cp1, and cp3 get their answer <b>${UPLOAD + HEALTHY} ms</b> after every file closes. ` +
        (cp2 === 'healthy' ? 'cp2 is healthy too, so nothing waits.' : `Only cp2's user partitions wait for cp2's queue.`);
    }
    if (failedCount) text += ` ${failedCount} cp2 part${failedCount === 1 ? '' : 's'} passed the 2-second deadline, so those producers get a retriable error and retry.`;
    show($('#fo-readout'), single && cp2 !== 'healthy' ? 'bad' : failedCount ? 'warn' : 'good', text);
  };
  stateEl.addEventListener('change', render);
  everyBtn.addEventListener('click', () => { every = !every; everyBtn.classList.toggle('on', every); everyBtn.textContent = every ? 'Every file touches cp2' : 'Every other file touches cp2'; render(); });
  singleBtn.addEventListener('click', () => { single = true; setOn([singleBtn, queuesBtn], singleBtn); render(); });
  queuesBtn.addEventListener('click', () => { single = false; setOn([singleBtn, queuesBtn], queuesBtn); render(); });
  render();
}
