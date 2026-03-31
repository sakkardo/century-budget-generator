/**
 * Expense Distribution Batch Downloader v4 — Iframe Approach
 *
 * KEY FIX: v3 used fetch-only approach and never set ReportType,
 * so it downloaded whatever the ASP.NET session had (usually Aging/RT=3).
 * v4 uses the proven iframe+postback approach from AP Aging Script v5,
 * but sets ReportType=1 (Expense Distribution) instead of 3 (Aging).
 *
 * Uses fresh iframe per entity to avoid session state contamination.
 * Sequential processing (not parallel) because iframe postbacks are stateful.
 *
 * BEFORE RUNNING: Edit the settings below
 */
(async function() {
  'use strict';

  const PERIOD_FROM = '01/2026';
  const PERIOD_TO   = '03/2026';
  const ENTITIES = [148, 204, 206, 805];

  const BASE = '/03578cms/Pages';
  const PAGE_URL = `${BASE}/APAnalytics.aspx?sMenuSet=iData`;

  const REPORT_TYPE = '1';  // 1 = Expense Distribution, 2 = Expense Dist (Paid Only), 3 = Aging

  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const log = msg => console.log(`[ExpDist] ${msg}`);
  const results = { success: [], failed: [] };
  const startTime = Date.now();

  log('='.repeat(50));
  log('Expense Distribution Batch Download v4');
  log(`${ENTITIES.length} buildings, period ${PERIOD_FROM}–${PERIOD_TO}`);
  log(`ReportType = ${REPORT_TYPE} (Expense Distribution)`);
  log('='.repeat(50));

  // ── Load APAnalytics in a working iframe ──────────────────────────────────
  log('Loading AP Analytics page in iframe...');
  const workFrame = document.createElement('iframe');
  workFrame.name = '_expDistWork';
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

  // ── Helper: set all form fields ───────────────────────────────────────────
  function setAllFields(entity) {
    const d = wDoc();
    const p = d.querySelector('input[name*="PropertyLookup"][name*="LookupCode"]');
    if (p) p.value = String(entity);
    const ap = d.querySelector('input[name*="APAccountLookup"][name*="LookupCode"]');
    if (ap) ap.value = '';
    const pf = d.querySelector('input[name*="PeriodFrom"]');
    if (pf) pf.value = PERIOD_FROM;
    const pt = d.querySelector('input[name*="PeriodTo"]');
    if (pt) pt.value = PERIOD_TO;
    const det = d.querySelector('input[name*="ShowDetail"]');
    if (det) det.checked = true;
    const grd = d.querySelector('input[name*="ShowGrid"]');
    if (grd) grd.checked = true;

    // Force RT dropdown to 1 (Expense Distribution)
    const rt = d.querySelector('select[name*="ReportType"]');
    if (rt) rt.value = REPORT_TYPE;
  }

  // ── Helper: log form state for debugging ──────────────────────────────────
  function logFormState(label) {
    const d = wDoc();
    const rt = d.querySelector('select[name*="ReportType"]');
    const prop = d.querySelector('input[name*="PropertyLookup"][name*="LookupCode"]');
    const pf = d.querySelector('input[name*="PeriodFrom"]');
    const pt = d.querySelector('input[name*="PeriodTo"]');
    log(`  [${label}] RT=${rt?.value} Prop=${prop?.value} From=${pf?.value} To=${pt?.value}`);
  }

  // ── Process each entity sequentially ──────────────────────────────────────
  for (const entity of ENTITIES) {
    try {
      log(`\n── ${entity}: Starting ──`);

      // STEP 1: Reload fresh iframe for each entity to avoid state pollution
      workFrame.src = PAGE_URL;
      await new Promise(r => { workFrame.onload = r; });
      await sleep(1000);

      logFormState('Fresh load');

      // STEP 2: Switch to Expense Distribution (RT=1) FIRST
      const rtSelect = wDoc().querySelector('select[name*="ReportType"]');
      if (!rtSelect) {
        results.failed.push({ entity, ok: false, reason: 'No ReportType dropdown' });
        continue;
      }
      rtSelect.value = REPORT_TYPE;
      log(`  Setting RT=${REPORT_TYPE} and posting back...`);
      await doPostback('ReportType:DropDownList');

      // Verify RT stuck
      const rtAfter = wDoc().querySelector('select[name*="ReportType"]')?.value;
      log(`  RT after postback: ${rtAfter}`);
      if (rtAfter !== REPORT_TYPE) {
        results.failed.push({ entity, ok: false, reason: `RT is ${rtAfter} after postback, expected ${REPORT_TYPE}` });
        continue;
      }

      // STEP 3: Set ALL fields including property (on the RT=1 page)
      setAllFields(entity);
      logFormState('Fields set');

      // STEP 4: Do property postback ON THE RT=1 PAGE
      log(`  Property postback...`);
      await doPostback('PropertyLookup:LookupCode');
      logFormState('After prop postback');

      // STEP 5: Check if RT reverted. If so, fix it and postback again
      const rtAfterProp = wDoc().querySelector('select[name*="ReportType"]')?.value;
      if (rtAfterProp !== REPORT_TYPE) {
        log(`  RT reverted to ${rtAfterProp} after property postback! Re-setting...`);
        const rtFix = wDoc().querySelector('select[name*="ReportType"]');
        rtFix.value = REPORT_TYPE;
        await doPostback('ReportType:DropDownList');
        const rtFixed = wDoc().querySelector('select[name*="ReportType"]')?.value;
        log(`  RT after re-fix: ${rtFixed}`);
        if (rtFixed !== REPORT_TYPE) {
          results.failed.push({ entity, ok: false, reason: `RT won't stick: ${rtFixed}` });
          continue;
        }
      }

      // STEP 6: Re-set ALL fields (postback replaces form HTML)
      setAllFields(entity);
      logFormState('Pre-export');

      // STEP 7: Excel export via FormData
      const form = wDoc().querySelector('form');
      const fd = new FormData(form);
      fd.set('__EVENTTARGET', 'Excel');
      fd.set('__EVENTARGUMENT', '');

      // Force all ReportType fields in FormData to our value
      let rtFieldCount = 0;
      for (const [key, val] of [...fd.entries()]) {
        if (key.toLowerCase().includes('reporttype')) {
          log(`  FormData: ${key} = "${val}" → forcing "${REPORT_TYPE}"`);
          fd.set(key, REPORT_TYPE);
          rtFieldCount++;
        }
      }
      log(`  Forced ${rtFieldCount} RT field(s) to "${REPORT_TYPE}" in FormData`);

      const vs = fd.get('__VIEWSTATE') || fd.get('__VIEWSTATE__') || '';
      log(`  ViewState length: ${vs.length}`);

      const resp = await fetch(form.action || PAGE_URL, { method: 'POST', body: fd });
      const ct = resp.headers.get('content-type') || '';
      const cd = resp.headers.get('content-disposition') || '';
      log(`  Response: ${resp.status} ct=${ct.substring(0, 50)} cd=${cd.substring(0, 50)}`);

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
    a.download = `ExpenseDistribution_${entity}.xlsx`;
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
