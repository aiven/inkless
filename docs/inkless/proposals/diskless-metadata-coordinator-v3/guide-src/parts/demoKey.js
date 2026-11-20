/* ---------------------------------------------------------------- object keys */
function demoKey() {
  const out = $('#ky-out');
  if (!out) return;
  const els = ['#ky-cluster', '#ky-broker', '#ky-epoch', '#ky-seq'].map(s => $(s));
  const render = () => {
    const [c, b, e, s] = els.map(el => el.value.trim());
    if (!c || [b, e, s].some(v => !/^\d+$/.test(v))) {
      out.className = 'readout warn';
      out.textContent = 'Enter a cluster ID and non-negative whole numbers.';
      return;
    }
    out.className = 'readout mono';
    out.innerHTML = `<div style="font-size:1rem"><b>${c}.${b}.${e}.${s}</b></div>` +
      `<div class="small" style="font-family:inherit;margin-top:0.4rem">Broker ${b} may upload this key only while it holds a lease that covers sequence ${s} in epoch ${e}. ` +
      `Any coordinator partition can compute it from those three numbers, so it can fence and delete the key without ever having heard of the object.</div>`;
  };
  els.forEach(el => el.addEventListener('input', render));
  render();
}
