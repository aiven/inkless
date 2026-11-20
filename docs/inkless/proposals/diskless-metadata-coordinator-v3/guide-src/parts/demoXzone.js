/* ---------------------------------------------------------------- cross-zone traffic */
function demoXzone() {
  const rate = $('#xz-rate');
  if (!rate) return;
  const markers = `<defs>
    <marker id="xz-ar-bad" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M0,0 L10,5 L0,10 z" style="fill:var(--bad)"/></marker>
    <marker id="xz-ar-good" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M0,0 L10,5 L0,10 z" style="fill:var(--good)"/></marker>
  </defs>`;
  const zones = () => [0, 1, 2].map(i => `
    <rect x="${6 + i * 104}" y="6" width="100" height="178" rx="10" style="fill:var(--soft);stroke:var(--line)"/>
    <text x="${56 + i * 104}" y="24" text-anchor="middle" style="fill:var(--muted);font-size:11px;font-weight:700">Zone ${'ABC'[i]}</text>`).join('');
  const box = (x, y, label, color) => `
    <rect x="${x - 34}" y="${y - 12}" width="68" height="24" rx="6" style="fill:var(--card);stroke:${color || 'var(--line)'};stroke-width:1.5"/>
    <text x="${x}" y="${y + 4}" text-anchor="middle" style="fill:var(--ink);font-size:10.5px;font-weight:600">${label}</text>`;
  const arrow = (x1, y1, x2, y2, good, w) =>
    `<line x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}" style="stroke:var(--${good ? 'good' : 'bad'});stroke-width:${w}" marker-end="url(#xz-ar-${good ? 'good' : 'bad'})"/>`;

  const render = () => {
    const p = +rate.value;
    $('#xz-rate-label').textContent = fmt(p) + ' MB/s';
    const w = (1.4 + 4.5 * Math.sqrt(p / 1000)).toFixed(2);
    const toLeader = (p * 2) / 3;
    const toFollowers = p * 2;
    const classic = toLeader + toFollowers;
    const tbPerDay = mbps => (mbps * 86400) / 1e6;

    $('#xz-classic-svg').innerHTML = `<svg viewBox="0 0 320 190" role="img" aria-label="Classic topic traffic">${markers}${zones()}
      ${arrow(58, 52, 132, 92, false, w)}${arrow(180, 112, 244, 112, false, w)}${arrow(134, 112, 82, 146, false, w)}
      ${box(56, 40, 'producer')}${box(160, 104, 'leader', 'var(--accent)')}${box(262, 112, 'follower')}${box(56, 158, 'follower')}
    </svg>`;
    $('#xz-diskless-svg').innerHTML = `<svg viewBox="0 0 320 190" role="img" aria-label="Diskless topic traffic">${markers}${zones()}
      ${[0, 1, 2].map(i => arrow(56 + i * 104, 52, 56 + i * 104, 82, true, w)).join('')}
      ${[0, 1, 2].map(i => arrow(56 + i * 104, 106, 56 + i * 104, 136, true, w)).join('')}
      ${[0, 1, 2].map(i => box(56 + i * 104, 40, 'producer')).join('')}
      ${[0, 1, 2].map(i => box(56 + i * 104, 94, 'broker', 'var(--accent)')).join('')}
      <rect x="16" y="140" width="288" height="34" rx="8" style="fill:var(--card);stroke:var(--good);stroke-width:1.5"/>
      <text x="160" y="161" text-anchor="middle" style="fill:var(--ink);font-size:11px;font-weight:600">object storage (spans all zones)</text>
    </svg>`;
    $('#xz-classic-text').innerHTML =
      `Producers to the leader: <b>${fmt(Math.round(toLeader))} MB/s</b> cross a zone, because two producers in three are elsewhere. ` +
      `Leader to its two followers: <b>${fmt(toFollowers)} MB/s</b>. ` +
      `Total: <b class="bad-text">${fmt(Math.round(classic))} MB/s</b>, about <b>${tbPerDay(classic).toFixed(1)} TB per day</b>.`;
    $('#xz-diskless-text').innerHTML =
      `Each producer writes to a broker in its own zone, and each broker writes to object storage. ` +
      `Consumers read through a broker in their own zone. Only small metadata requests cross zones: ` +
      `<b class="good-text">close to 0 MB/s</b>.`;
    show($('#xz-readout'), '',
      `At ${fmt(p)} MB/s, producers write ${tbPerDay(p).toFixed(1)} TB per day. The classic topic sends ` +
      `<b>${(classic / p).toFixed(2)}×</b> that across zones, and every broker also needs disks for its copies. ` +
      `The diskless topic trades this cost for latency: a produce waits for the buffer and the upload, ` +
      `hundreds of milliseconds instead of a few.`);
  };
  rate.addEventListener('input', render);
  render();
}
