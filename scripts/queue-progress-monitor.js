#!/usr/bin/env node

const defaults = {
  baseUrl: 'https://medici-monitor-dashboard.azurewebsites.net',
  pollSec: 60,
  secondsPerOrder: 30,
  currentOrder: 1521,
  oldRangeEnd: 1853,
  oldRemainingOverride: null,
  once: false
};

const hotelBlocks = [
  { hotel: 'Notebook Miami Beach', start: 1854, end: 1930, note: 'ראשון בתור' },
  { hotel: 'Eurostars Langford Hotel', start: 1931, end: 2007, note: '' },
  { hotel: 'FAIRWIND HOTEL', start: 2008, end: 2084, note: '' },
  { hotel: 'SLS LUX Brickell', start: 2085, end: 2161, note: '' },
  { hotel: 'Pullman Miami Airport', start: 2162, end: 2238, note: '' },
  { hotel: 'Savoy Hotel', start: 2239, end: 2392, note: 'אחרון' }
];

function parseArgs() {
  const args = { ...defaults };
  for (const raw of process.argv.slice(2)) {
    const [key, val] = raw.split('=');
    if (!key.startsWith('--')) continue;
    const k = key.slice(2);
    if (k === 'once') args.once = true;
    else if (k === 'base-url') args.baseUrl = val || args.baseUrl;
    else if (k === 'poll-sec') args.pollSec = Number(val || args.pollSec);
    else if (k === 'sec-per-order') args.secondsPerOrder = Number(val || args.secondsPerOrder);
    else if (k === 'current-order') args.currentOrder = Number(val || args.currentOrder);
    else if (k === 'old-range-end') args.oldRangeEnd = Number(val || args.oldRangeEnd);
    else if (k === 'old-remaining') args.oldRemainingOverride = Number(val);
  }
  return args;
}

function fmtDuration(totalSec) {
  const sec = Math.max(0, Math.round(totalSec));
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  return `${h}ש ${m}ד`;
}

function fmtClockFromNow(totalSec) {
  const d = new Date(Date.now() + Math.max(0, Math.round(totalSec)) * 1000);
  return d.toLocaleTimeString('he-IL', { hour: '2-digit', minute: '2-digit' });
}

function getDetectedCurrentOrder(d) {
  if (!d || !Array.isArray(d.runningOrderItems) || d.runningOrderItems.length === 0) return null;
  const inProgress = d.runningOrderItems
    .filter(x => x.webJobStatus === 'In Progress' && Number.isFinite(x.orderId))
    .map(x => x.orderId)
    .sort((a, b) => a - b);
  if (inProgress.length) return inProgress[0];

  const byId = d.runningOrderItems
    .filter(x => Number.isFinite(x.orderId))
    .map(x => x.orderId)
    .sort((a, b) => a - b);
  return byId.length ? byId[0] : null;
}

async function loadDiagnostics(baseUrl) {
  try {
    const res = await fetch(`${baseUrl}/api/salesorder/diagnostics`);
    if (!res.ok) return { ok: false, code: res.status };
    const json = await res.json();
    return { ok: true, data: json };
  } catch (e) {
    return { ok: false, error: e?.message || String(e) };
  }
}

function printReport(args, live) {
  const d = live?.ok ? live.data : null;
  const detectedCurrentOrder = getDetectedCurrentOrder(d);
  const currentOrder = detectedCurrentOrder || args.currentOrder;

  const oldRemaining = Number.isFinite(args.oldRemainingOverride)
    ? Math.max(0, args.oldRemainingOverride)
    : Math.max(0, args.oldRangeEnd - currentOrder);

  const newOrdersTotal = hotelBlocks.reduce((sum, b) => sum + (b.end - b.start + 1), 0);
  const totalRemaining = oldRemaining + newOrdersTotal;
  const totalEtaSec = totalRemaining * args.secondsPerOrder;

  console.clear();
  console.log('=== SalesOrder Queue Progress Monitor ===');
  console.log(`Now: ${new Date().toLocaleString('he-IL')}`);
  console.log(`Monitor API: ${args.baseUrl}`);
  if (live?.ok) {
    console.log(`Diagnostics: OK | pending=${d.pendingOrders ?? '-'} running=${d.runningOrders ?? '-'} completed=${d.completedOrders ?? '-'} failed=${d.failedOrders ?? '-'}`);
  } else {
    const reason = live?.code ? `HTTP ${live.code}` : (live?.error || 'unavailable');
    console.log(`Diagnostics: unavailable (${reason}) -> using manual baseline`);
  }

  console.log('');
  console.log(`Current order (estimated): ${currentOrder}`);
  console.log(`Old orders remaining (<= ${args.oldRangeEnd}): ${oldRemaining}`);
  console.log(`New orders total (1854-2392): ${newOrdersTotal}`);
  console.log(`Total remaining: ${totalRemaining}`);
  console.log(`Avg speed: ${args.secondsPerOrder} sec/order`);
  console.log(`ETA full queue: ${fmtDuration(totalEtaSec)} (~${fmtClockFromNow(totalEtaSec)})`);
  console.log('');

  const firstNewAhead = Math.max(0, hotelBlocks[0].start - currentOrder);
  const firstNewStartSec = firstNewAhead * args.secondsPerOrder;
  console.log(`התחלת הזמנות חדשות (1854+): בעוד ${fmtDuration(firstNewStartSec)} (~${fmtClockFromNow(firstNewStartSec)})`);
  console.log('');

  console.log('Hotel blocks ETA:');
  console.log('--------------------------------------------------------------------------');
  console.log('Hotel                          | Range      | Orders | Start ETA | End ETA');
  console.log('--------------------------------------------------------------------------');

  for (const b of hotelBlocks) {
    const count = b.end - b.start + 1;
    const startAhead = Math.max(0, b.start - currentOrder);
    const endAhead = Math.max(0, b.end - currentOrder + 1);
    const startSec = startAhead * args.secondsPerOrder;
    const endSec = endAhead * args.secondsPerOrder;

    const hotel = (b.hotel + (b.note ? ` (${b.note})` : '')).padEnd(30, ' ');
    const range = `${b.start}-${b.end}`.padEnd(10, ' ');
    const orders = String(count).padEnd(6, ' ');
    const startEta = `${fmtDuration(startSec)} ~${fmtClockFromNow(startSec)}`.padEnd(16, ' ');
    const endEta = `${fmtDuration(endSec)} ~${fmtClockFromNow(endSec)}`;

    console.log(`${hotel}| ${range}| ${orders}| ${startEta}| ${endEta}`);
  }
  console.log('--------------------------------------------------------------------------');
  console.log('');
}

async function tick(args) {
  const live = await loadDiagnostics(args.baseUrl);
  printReport(args, live);
}

async function main() {
  const args = parseArgs();

  await tick(args);
  if (args.once) return;

  setInterval(() => {
    tick(args).catch(err => {
      console.error('tick error:', err?.message || err);
    });
  }, Math.max(5, args.pollSec) * 1000);
}

main().catch(err => {
  console.error(err?.message || err);
  process.exit(1);
});
