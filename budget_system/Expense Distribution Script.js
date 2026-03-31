/**
 * Expense Distribution Batch Downloader v7 — Direct Page + Frame Search
 *
 * PREREQUISITE: User must be on APAnalytics.aspx with "Expense Distribution
 * (Paid Only)" selected (RT=2). This script does NOT change the report type
 * — it trusts the page state.
 *
 * Automatically finds the correct frame containing the APAnalytics form,
 * even when Yardi loads the page inside nested frames.
 *
 * BEFORE RUNNING: Select "Expense Distribution (Paid Only)" in the dropdown
 */
(async function() {
  'use strict';

  const PERIOD_FROM = '01/2026';
  const PERIOD_TO   = '03/2026';
  const ENTITIES = [148, 204, 206, 805];

  const BASE = '/03578cms/Pages';
  const PAGE_URL = `${BASE}/APAnalytics.aspx?sMenuSet=iData`;

  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const log = msg => console.log(`[ExpDist] ${msg}`);
  const results = { success: [], failed: [] };
  const startTime = Date.now();

  // ── Find the document containing the APAnalytics form ─────────────────
  // Yardi loads pages in nested frames. Search all frames for the RT dropdown.
  function findAPDoc() {
    // Check top document first
    if (document.querySelector('select[name*="ReportType"]')) return document;
    // Search child frames
    for (let i = 0; i < window.frames.length; i++) {
      try {
        const fd = window.frames[i].document;
        if (fd.querySelector('select[name*="ReportType"]')) return fd;
        // Check nested frames
        for (let j = 0; j < window.frames[i].frames.length; j++) {
          try {
            const fd2 = window.frames[i].frames[j].document;
            if (fd2.querySelector('select[name*="ReportType"]')) return fd2;
          } catch(e) {}
        }
      } catch(e) {}
    }
    return null;
  }

  function findAPWin() {
    if (document.querySelector('select[name*="ReportType"]')) return window;
    for (let i = 0; i < window.frames.length; i++) {
      try {
        const fd = window.frames[i].document;
        if (fd.querySelector('select[name*="ReportType"]')) return window.frames[i];
        for (let j = 0; j < window.frames[i].frames.length; j++) {
          try {
            const fd2 = window.frames[i].frames[j].document;
            if (fd2.querySelector('select[name*="ReportType"]')) return window.frames[i].frames[j];
          } catch(e) {}
        }
      } catch(e) {}
    }
    return null;
  }

  let doc = findAPDoc();
  let win = findAPWin();

  if (!doc) {
    throw new Error('Cannot find APAnalytics form in any frame. Make sure you are on the Payable Analytics page in Yardi.');
  }

  const rtCheck = doc.querySelector('select[name*="ReportType"]');
  log(`Found APAnalytics form. Current RT value: ${rtCheck.value} ("${rtCheck.options[rtCheck.selectedIndex]?.text}")`);

  if (rtCheck.value !== '2') {
    throw new Error(`ReportType is "${rtCheck.options[rtCheck.selectedIndex]?.text}" (value=${rtCheck.value}). Please select "Expense Distribution (Paid Only)" from the dropdown and try again.`);
  }

  log('='.repeat(50));
  log('Expense Distribution Batch Download v7 — Direct Page');
  log(`${ENTITIES.length} buildings, period ${PERIOD_FROM}–${PERIOD_TO}`);
  log('RT verified as 2 (Expense Distribution — Paid Only)');
  log('='.repeat(50));

  // ── Process each entity ──────────────────────────────────────────────────
  for (const entity of ENTITIES) {
    try {
      log(`\n── ${entity}: Starting ──`);

      // Re-find doc after postbacks (frame may have reloaded)
      doc = findAPDoc();
      win = findAPWin();
      if (!doc) {
        results.failed.push({ entity, ok: false, reason: 'Lost frame after postback' });
        continue;
      }

      // Set property
      const prop = doc.querySelector('input[name*="PropertyLookup"][name*="LookupCode"]');
      if (prop) prop.value = String(entity);

      // Set periods
      const pf = doc.querySelector('input[name*="PeriodFrom"]');
      if (pf) pf.value = PERIOD_FROM;
      const pt = doc.querySelector('input[name*="PeriodTo"]');
      if (pt) pt.value = PERIOD_TO;

      // Set AP Account (blank for all)
      const ap = doc.querySelector('input[name*="APAccountLookup"][name*="LookupCode"]');
      if (ap) ap.value = '';

      // Checkboxes
      const det = doc.querySelector('input[name*="ShowDetail"]');
      if (det) det.checked = true;
      const grd = doc.querySelector('input[name*="ShowGrid"]');
      if (grd) grd.checked = true;

      // Property postback to validate entity
      log(`  Property postback for ${entity}...`);
      win.__doPostBack('PropertyLookup:LookupCode', '');

      // Wait for frame to reload after postback
      await sleep(3000);

      // Re-find doc after postback
      doc = findAPDoc();
      win = findAPWin();
      if (!doc) {
        results.failed.push({ entity, ok: false, reason: 'Lost frame after property postback' });
        continue;
      }

      // Verify RT is still 1 after property postback
      const rtAfterProp = doc.querySelector('select[name*="ReportType"]')?.value;
      log(`  RT after property postback: ${rtAfterProp}`);
      if (rtAfterProp !== '2') {
        log(`  WARNING: RT changed to ${rtAfterProp} — re-selecting Expense Distribution (Paid Only)`);
        const rtFix = doc.querySelector('select[name*="ReportType"]');
        if (rtFix) {
          rtFix.value = '2';
          rtFix.dispatchEvent(new Event('change', { bubbles: true }));
          win.__doPostBack('ReportType:DropDownList', '');
          await sleep(2000);
          doc = findAPDoc();
          win = findAPWin();
          if (!doc) {
            results.failed.push({ entity, ok: false, reason: 'Lost frame after RT fix' });
            continue;
          }
        }
      }

      // Re-set fields after postback (ASP.NET replaces form HTML)
      const prop2 = doc.querySelector('input[name*="PropertyLookup"][name*="LookupCode"]');
      if (prop2) prop2.value = String(entity);
      const pf2 = doc.querySelector('input[name*="PeriodFrom"]');
      if (pf2) pf2.value = PERIOD_FROM;
      const pt2 = doc.querySelector('input[name*="PeriodTo"]');
      if (pt2) pt2.value = PERIOD_TO;
      const ap2 = doc.querySelector('input[name*="APAccountLookup"][name*="LookupCode"]');
      if (ap2) ap2.value = '';
      const det2 = doc.querySelector('input[name*="ShowDetail"]');
      if (det2) det2.checked = true;
      const grd2 = doc.querySelector('input[name*="ShowGrid"]');
      if (grd2) grd2.checked = true;

      // Excel export via FormData
      const form = doc.querySelector('form');
      const fd = new FormData(form);
      fd.set('__EVENTTARGET', 'Excel');
      fd.set('__EVENTARGUMENT', '');

      // Force RT in FormData
      for (const [key] of [...fd.entries()]) {
        if (key.toLowerCase().includes('reporttype')) {
          fd.set(key, '2');
        }
      }

      const rtFinal = doc.querySelector('select[name*="ReportType"]')?.value;
      log(`  Exporting... RT=${rtFinal} Prop=${prop2?.value} From=${pf2?.value} To=${pt2?.value}`);

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
