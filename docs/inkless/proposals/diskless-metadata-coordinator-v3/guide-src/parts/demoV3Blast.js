/* ---------------------------------------------------------------- blast radius */
function demoV3Blast() {
  const nEl = $('#brt-n');
  if (!nEl) return;
  const guardBtn = $('#brt-guard'), overBtn = $('#brt-override');
  const total = 10000;
  let guard = true;
  const render = () => {
    const n = +nEl.value, affected = Math.ceil(total / n);
    $('#brt-classic').innerHTML = `<h6>Classic partition ${chip('info', 'baseline')}</h6>` +
      `<p class="small" style="margin:0">One replica-set loss affects one partition: <b>1 of 10,000</b> (0.01%). An unclean election or a recreated topic brings it back empty.</p>`;
    $('#brt-v3').innerHTML = guard
      ? `<h6>Coordinator ${chip('warn', 'accepted risk')}</h6><p class="small" style="margin:0"><b>${fmt(affected)}</b> partitions can't be written or read. The coordinator partition stays leaderless, and the design has no procedure that brings it back. Fences to it go unanswered and releases from it never arrive, so the objects it touched leak instead of being deleted. No other partition's data is touched.</p>`
      : `<h6>Coordinator ${chip('bad', 'outside the bar')}</h6><p class="small" style="margin:0">With replication factor 1, one disk loses the coordinator partition. Clause 2's exception doesn't apply, which is why the override is internal and meant for tests and local development.</p>`;
    $('#brt-footer').innerHTML = `<h6>Footer recovery ${chip('info', 'future work')}</h6>` +
      `<p class="small" style="margin:0">A later version could rebuild the ${fmt(affected)} partitions from object footers at new offsets above a floor, with no acknowledged record lost. This design writes no footer.</p>`;
    show($('#brt-readout'), guard ? 'warn' : 'bad',
      `With N = ${n}, one coordinator partition's loss reaches <b>${fmt(affected)} of ${fmt(total)}</b> diskless partitions, or <b>${(100 / n).toFixed(2)}%</b>. ` +
      (guard
        ? 'The enforced guard makes the loss rare, and clause 2\'s exception makes it acceptable. A larger N shrinks the share but adds parts and flushes to every commit.'
        : 'The internal override removes the guard, so the bar no longer covers this cluster.'));
  };
  nEl.addEventListener('change', render);
  guardBtn.addEventListener('click', () => { guard = true; setOn([guardBtn, overBtn], guardBtn); render(); });
  overBtn.addEventListener('click', () => { guard = false; setOn([guardBtn, overBtn], overBtn); render(); });
  render();
}
