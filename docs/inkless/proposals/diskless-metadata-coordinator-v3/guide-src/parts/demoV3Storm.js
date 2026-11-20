/* ---------------------------------------------------------------- fence fan-out */
function demoV3Storm() {
  const evEl = $('#fx-event');
  if (!evEl) return;
  const nEl = $('#fx-n'), bEl = $('#fx-b'), dEl = $('#fx-d');
  const render = () => {
    const ev = evEl.value, n = +nEl.value, b = +bEl.value, d = +dEl.value;
    $('#fx-b-label').textContent = b + ' brokers';
    $('#fx-d-label').textContent = d + ' s';
    dEl.disabled = ev !== 'outage';
    $('#fx-d-wrap').style.opacity = ev === 'outage' ? '1' : '0.45';
    let before, beforeText, afterText;
    if (ev === 'outage') {
      const exc = Math.round(b * d / 0.25);
      before = exc + b;
      beforeText = `About <b>${fmt(exc)}</b> exception ranges, one per recorded finish report while the partition is down (<code>B × D / 0.25</code>), plus about <b>${fmt(b)}</b> lease-end ranges from the brokers that abandon their ranges there. All of them wait for recovery.`;
      afterText = `At most <b>${n - 1}</b> fence requests arrive at once, one from each other tracking partition, each carrying hull-merged ranges. The partition sends at most ${n - 1} more as a tracking partition.`;
    } else if (ev === 'restart') {
      before = b * n;
      beforeText = `Each epoch end costs ${n} range fences, or ${2 * n} with a fallback range: <b>${fmt(before)}</b> in total, spread over the restart.`;
      afterText = `Each restart's ${n} ranges merge into the pending request to each participant, so a participant has at most <b>${n - 1}</b> fence requests in flight.`;
    } else {
      const fenced = Math.round(b / 3);
      before = fenced * n;
      beforeText = `A fenced broker keeps its epoch, so nothing fires until the idle timers end the leases. Then about ${fmt(fenced)} broker${fenced === 1 ? '' : 's'} times ${n} partitions, <b>${fmt(before)}</b> range fences, arrive within seconds.`;
      afterText = `The ranges travel in at most <b>${n - 1}</b> requests per participant at a time, one from each other tracking partition.`;
    }
    $('#fx-before').innerHTML = `<h6>Fence ranges ${chip(before > 1000 ? 'bad' : before > 100 ? 'warn' : 'info', fmt(before))}</h6><p class="small" style="margin:0">${beforeText}</p>`;
    $('#fx-after').innerHTML = `<h6>Requests in flight per participant ${chip('good', 'at most ' + (n - 1))}</h6><p class="small" style="margin:0">${afterText}</p>`;
    show($('#fx-readout'), ev === 'outage' && before > 1000 ? 'warn' : 'good',
      ev === 'outage'
        ? `The range count grows with the brokers and the outage length: ${fmt(before)} here. Batching keeps what lands on the recovering partition at ${n - 1} requests at a time, and <code>PendingFenceRangeCount</code> shows the backlog draining.`
        : `Batching caps the request count at ${n - 1} per participant, whatever the broker count. <code>FenceRate</code> counts ranges by type, so the storm stays visible.`);
  };
  [bEl, dEl].forEach(el => el.addEventListener('input', render));
  [evEl, nEl].forEach(el => el.addEventListener('change', render));
  render();
}
