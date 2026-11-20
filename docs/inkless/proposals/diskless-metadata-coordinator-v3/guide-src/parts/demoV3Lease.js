/* ---------------------------------------------------------------- lease lifecycle */
function demoV3Lease() {
  const rateEl = $('#ll-rate');
  if (!rateEl) return;
  const quietEl = $('#ll-quiet');
  const v2Btn = $('#ll-v2'), v3Btn = $('#ll-v3');
  const primary = $('#ll-primary'), fallback = $('#ll-fallback'), idle = $('#ll-idle');
  const K = 1024, HALF = K / 2, IDLE = 15 * 60;
  let v3 = false;
  const render = () => {
    const rate = +rateEl.value, quiet = +quietEl.value;
    const reaches = HALF / rate;
    const v2Fires = reaches > IDLE;
    $('#ll-rate-label').textContent = rate.toFixed(2) + ' files/s';
    $('#ll-quiet-label').textContent = quiet + ' min quiet';
    if (v3) {
      primary.innerHTML = `<h6>Primary range at cp2 ${chip('info', '[0, 1023] abandoned')}</h6>` +
        `<div class="bar-track"><div class="bar-seg" style="left:0;width:50%;background:var(--accent)">used 0–511</div><div class="bar-seg" style="left:50%;width:50%;background:var(--soft);color:var(--muted)">abandoned 512–1023</div></div>` +
        `<div class="small" style="margin-top:0.4rem">The broker switched away at confirmation. Once cp2 answers again, a lease release with <code>b = 1023</code> ends this range, and its range fences queue the unused keys.</div>` +
        `<dl class="kv" style="margin-top:0.4rem"><dt>lease release</dt><dd>b = 1023, with the final finish report</dd><dt>floors after release</dt><dd>(7, 1023), below the live range</dd></dl>`;
      fallback.innerHTML = `<h6>Fallback range at cp3 ${chip('good', '[1024, 2047] in use')}</h6>` +
        `<div class="bar-track"><div class="bar-seg" style="left:0;width:100%;background:var(--accent)">in use from confirmation</div></div>` +
        `<div class="small" style="margin-top:0.4rem">The first file after confirmation names cp3. Keepalives at half the idle timeout keep the range live even at a low rate.</div>` +
        `<dl class="kv" style="margin-top:0.4rem"><dt>time to first use</dt><dd>0 s</dd><dt>timer outcome</dt><dd>rescheduled by every part or keepalive</dd></dl>`;
    } else {
      const primaryColor = v2Fires ? 'var(--bad)' : 'var(--accent)';
      primary.innerHTML = `<h6>Primary range at cp2 ${chip(v2Fires ? 'bad' : 'good', `[0, 1023] ${v2Fires ? 'fenced too early' : 'in use'}`)}</h6>` +
        `<div class="bar-track"><div class="bar-seg" style="left:0;width:50%;background:var(--accent)">used 0–511</div><div class="bar-seg" style="left:50%;width:50%;background:${primaryColor}">${v2Fires ? 'live range fenced' : 'live 512–1023'}</div></div>` +
        `<div class="small muted" style="margin-top:0.4rem">At fallback confirmation, the broker still needs 512 files to reach the next range.</div>` +
        `<dl class="kv" style="margin-top:0.4rem"><dt>time to live range end</dt><dd>${reaches.toFixed(1)} s</dd><dt>floor after timer</dt><dd>${v2Fires ? '(7, 2047)' : '(7, 511)'}</dd></dl>`;
      fallback.innerHTML = `<h6>Fallback range at cp3 ${chip(v2Fires ? 'bad' : 'info', '[1024, 2047]')}</h6>` +
        `<div class="bar-track"><div class="bar-seg" style="left:0;width:${v2Fires ? '100' : '50'}%;background:${v2Fires ? 'var(--bad)' : 'var(--soft)'};color:${v2Fires ? '#fff' : 'var(--muted)'}">${v2Fires ? 'ended and fenced' : 'above the live range'}</div></div>` +
        `<div class="small" style="margin-top:0.4rem">The fallback leader's 900-second timer starts at confirmation. ${v2Fires ? 'It fires before the broker reaches sequence 1024.' : 'The broker reaches sequence 1024 before the timer fires.'}</div>` +
        `<dl class="kv" style="margin-top:0.4rem"><dt>timer outcome</dt><dd>${v2Fires ? 'idle timer ends it first' : 'broker reaches it first'}</dd><dt>idle timeout</dt><dd>900 s</dd></dl>`;
    }
    const quietS = quiet * 60;
    const v2Ended = quietS >= IDLE;
    const keepalives = Math.floor(quietS / (IDLE / 2));
    idle.innerHTML = v3
      ? `<h6>After ${quiet} quiet minutes ${chip('good', 'lease kept')}</h6><p class="small" style="margin:0">The broker sent <b>${keepalives}</b> keepalive${keepalives === 1 ? '' : 's'}, one after each 450 seconds without an answered request. Each costs a request, no append, and no fsync. The first file after the quiet period commits with no wait, and <code>IdleLeaseEndRate</code> stays at 0.</p>`
      : `<h6>After ${quiet} quiet minutes ${chip(v2Ended ? 'bad' : 'good', v2Ended ? 'lease ended idle' : 'lease still live')}</h6><p class="small" style="margin:0">${v2Ended
        ? 'The idle timer ended the lease at 15 minutes, and nothing told the broker. Its next file gets stale-sequence, and files closed before a new lease confirms fail too.'
        : 'The quiet period is shorter than the 15-minute idle timeout, so the first file commits.'}</p>`;
    show($('#ll-readout'), v3 ? 'good' : (v2Fires || v2Ended) ? 'bad' : '',
      v3
        ? 'V3: the broker uses the fallback range at once, so the file rate no longer matters, and keepalives keep the lease live through any quiet period. No file fails, and the abandoned range ends with a scoped release.'
        : `V2: the broker reaches the fallback range in <b>${reaches.toFixed(1)} s</b> at ${rate.toFixed(2)} files/s, ${v2Fires ? 'after the 900-second timer, so the fallback range fence cuts the live primary range' : 'before the 900-second timer fires'}. ` +
          `${v2Ended ? `The ${quiet}-minute quiet period also ends the lease, so the first file after it fails.` : 'The quiet period stays under the idle timeout.'} The fallback threshold is ${(HALF / IDLE).toFixed(4)} files/s.`);
  };
  rateEl.addEventListener('input', render);
  quietEl.addEventListener('input', render);
  v2Btn.addEventListener('click', () => { v3 = false; setOn([v2Btn, v3Btn], v2Btn); render(); });
  v3Btn.addEventListener('click', () => { v3 = true; setOn([v2Btn, v3Btn], v3Btn); render(); });
  render();
}
