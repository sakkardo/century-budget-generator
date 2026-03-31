/**
 * AP Aging (Open AP) Batch Downloader v4 — Iframe Postback
 *
 * Works from ANY Yardi page. Loads APAnalytics in a hidden iframe,
 * uses iframe postbacks for ReportType and Property changes, then
 * fetch with FormData from the iframe for Excel export.
 *
 * v4: Fix ReportType revert — re-set RT=3 after every property postback
 *     and verify before Excel export. Also fix file extension to .xlsx.
 *
 * BEFORE RUNNING: Edit the settings below
 */
(async function() {
  'use strict';

  const PERIOD_TO = '03/2026';
  const ENTITIES = [148, 204, 206, 805];

  const BASE = '/03578cms/Pages';
  const PAGE_URL = `${BASE}/APAnalytics.aspx?sMenuSet=iData`;

  const today = new Date();
  const AGE_AS_OF = `${String(today.getMonth() + 1).padStart(2, '0')}/${String(today.getDate()).padStart(2, '0')}/${today.getFullYear()}`;

  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const log = msg => console.log(`[APAging] ${msg}`);
  const results = { success: [], failed: [] };
  const startTime = Date.now();

  log('='.repeat(50));
  log(`AP Aging Batch Download v4`);
  log(`${ENTITIES.length} buildings, aging as of ${AGE_AS_OF}`);
  log('='.repeat(50));

  // ── Load APAnalytics in a working iframe ──────────────────────────────────
  log('Loading AP Analytics page in iframe...');
  const workFrame = document.createElement('iframe');
  workFrame.name = '_apAgingWork';
  workFrame.style.display = 'none';
  workFrame.src = PAGE_URL;
  document.body.appendChild(workFrame);
  await new Promise(r => { workFrame.onload = r; });
  await sleep(1000);

  const wDoc = () => workFrame.contentDocument;
  const wWin = () => workFrame.contentWindow;

  if (!wDoc()?.querySelector('select[name*="ReportType"]')) {
    document.body.removeChild(workFrame);
    throw new Error('Failed to load APAnalytics in iframe (session expired?)');
  }
  log('AP Analytics loaded.');

  // ── Helper: postback inside the work iframe ───────────────────────────────
  async function doPostback(eventTarget) {
    wWin().__doPostBack(eventTarget, '');
    await new Promise(r => { workFrame.onload = r; });
    await sleep(500);
  }

  // ── Helper: ensure ReportType is 3 (Aging) ───────────────────────────────
  async function ensureAgingRT() {
    const rtSelect = wDoc().querySelector('select[name*="ReportType"]');
    if (!rtSelect) {
      log('  WARNING: ReportType dropdown not found after postback');
      return false;
    }
    if (rtSelect.value !== '3') {
      log(`  RT was ${rtSelect.value}, resetting to 3 (Aging)...`);
      rtSelect.value = '3';
      await doPostback('ReportType:DropDownList');
      const newVal = wDoc().querySelector('select[name*="ReportType"]')?.value;
      if (newVal !== '3') {
        log(`  ERROR: RT still ${newVal} after postback`);
        return false;
      }
      log(`  RT confirmed: ${newVal}`);
    }
    return true;
  }

  // ── Step 1: Set ReportType to 3 (Aging) ───────────────────────────────────
  if (!(await ensureAgingRT())) {
    document.body.removeChild(workFrame);
    throw new Error('Could not set ReportType to Aging');
  }
  log('ReportType confirmed as 3 (Aging).');

  // ── Step 2: Process each entity ───────────────────────────────────────────
  for (const entity of ENTITIES) {
    try {
      log(`\n  ${entity}: Setting property and aging fields...`);

      const setFields = () => {
        const d = wDoc();
        const p = d.querySelector('input[name*="PropertyLookup"][name*="LookupCode"]');
        if (p) p.value = String(entity);
        const ap = d.querySelector('input[name*="APAccountLookup"][name*="LookupCode"]');
        if (ap) ap.value = '2210-0000';
        const pt = d.querySelector('input[name*="PeriodTo"]');
        if (pt) pt.value = PERIOD_TO;
        const age = d.querySelector('input[name*="AgeAsOf"]');
        if (age) age.value = AGE_AS_OF;
        const det = d.querySelector('input[name*="ShowDetail"]');
        if (det) det.checked = true;
        const grd = d.querySelector('input[name*="ShowGrid"]');
        if (grd) grd.checked = true;
      };

      setFields();
      await doPostback('PropertyLookup:LookupCode');

      // ── CRITICAL: Re-verify RT=3 after property postback ──
      // Property postback can reset ReportType to default (Expense Distribution)
      if (!(await ensureAgingRT())) {
        results.failed.push({ entity, ok: false, reason: 'RT reverted after property postback' });
        continue;
      }

      log(`  ${entity}: Property validated, RT confirmed, requesting Excel...`);

      // Re-set all fields after postbacks (form innerHTML gets replaced)
      setFields();

      // Final RT verification before export
      const finalRT = wDoc().querySelector('select[name*="ReportType"]')?.value;
      if (finalRT !== '3') {
        log(`  WARNING: RT is ${finalRT} before Excel export! Attempting fix...`);
        const rtFix = wDoc().querySelector('select[name*="ReportType"]');
        if (rtFix) rtFix.value = '3';
      }

      // Excel via fetch with FormData from iframe's form
      const form = wDoc().querySelector('form');
      const fd = new FormData(form);
      fd.set('__EVENTTARGET', 'Excel');
      fd.set('__EVENTARGUMENT', '');
      if (!fd.get('__VIEWSTATE') && fd.get('__VIEWSTATE__')) fd.delete('__VIEWSTATE');

      // ── CRITICAL: Force ReportType=3 in FormData ──
      // ASP.NET ViewState may override DOM dropdown value, so we
      // explicitly set every ReportType field in the FormData to "3"
      for (const [key, val] of [...fd.entries()]) {
        if (key.toLowerCase().includes('reporttype')) {
          log(`  FormData ${key} was "${val}", forcing to "3"`);
          fd.set(key, '3');
        }
      }

      const resp = await fetch(form.action || PAGE_URL, { method: 'POST', body: fd });
      const ct = resp.headers.get('content-type') || '';
      const cd = resp.headers.get('content-disposition') || '';

      let blob = null;
      if (ct.includes('spreadsheet') || ct.includes('excel') || ct.includes('octet-stream') || cd.includes('attachment')) {
        blob = await resp.blob();
      } else {
        const html = await resp.text();
        const dlMatch = html.match(/sFileName=([^'"&\s]+)/);
        if (dlMatch) {
          blob = await (await fetch(`${BASE}/SysShuttleDisplayHandler.ashx?sFileName=${dlMatch[1]}`)).blob();
        } else {
          const recMatch = html.match(/name="Records"\s+value="([^"]+)"/);
          if (recMatch) {
            const recordId = decodeURIComponent(recMatch[1]).split(',')[0].trim();
            log(`  ${entity}: Queued (${recordId}), polling...`);
            for (let p = 0; p < 20; p++) {
              await sleep(3000);
              const monResp = await fetch(`${BASE}/SysConductorReportMonitor.aspx?Records=${recordId}&FilterInfo=&sDir=0&bMonitor=0`);
              const monHtml = await monResp.text();
              const monDl = monHtml.match(/sFileName=([^'"&\s]+)/);
              if (monDl) {
                blob = await (await fetch(`${BASE}/SysShuttleDisplayHandler.ashx?sFileName=${monDl[1]}`)).blob();
                break;
              }
            }
          }
        }
      }

      if (blob && blob.size > 0) {
        triggerDownload(blob, entity);
        log(`  ✓ ${entity} — ${(blob.size / 1024).toFixed(0)} KB`);
        results.success.push({ entity, ok: true, size: blob.size });
      } else {
        log(`  ✗ ${entity} — no file received`);
        results.failed.push({ entity, ok: false, reason: 'no_file' });
      }
    } catch (ex) {
      log(`  ✗ ${entity} — ${ex.message}`);
      results.failed.push({ entity, ok: false, reason: ex.message });
    }
  }

  // Cleanup
  document.body.removeChild(workFrame);

  function triggerDownload(blob, entity) {
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `APAging_${entity}.xlsx`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(a.href);
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  log('\n' + '='.repeat(50));
  log(`DONE in ${elapsed}s: ${results.success.length} OK, ${results.failed.length} failed`);
  if (results.failed.length) results.failed.forEach(r => log(`  ${r.entity}: ${r.reason}`));
  log('='.repeat(50));
  return results;
})();
