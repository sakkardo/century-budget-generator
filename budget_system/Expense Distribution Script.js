/**
 * Expense Distribution Batch Downloader v6 — Direct Page Approach
 *
 * PREREQUISITE: User must be on APAnalytics.aspx with "Expense Distribution"
 * already selected in the ReportType dropdown. This script does NOT change
 * the report type — it trusts the page state and just fills in property/period
 * fields and exports.
 *
 * This avoids the ASP.NET ViewState issue where RT=1 reverts to RT=3 during
 * postbacks. By running directly on the page (no iframe), the server state
 * is whatever the user already set.
 *
 * BEFORE RUNNING: Select "Expense Distribution" in the Report Type dropdown
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

  // ── Verify we're on the right page with RT=1 ─────────────────────────────
  const rtCheck = document.querySelector('select[name*="ReportType"]');
  if (!rtCheck) {
    throw new Error('Not on APAnalytics page — no ReportType dropdown found. Navigate to AP Analytics first.');
  }
  if (rtCheck.value !== '1') {
    throw new Error(`ReportType is "${rtCheck.value}" — please select "Expense Distribution" from the dropdown before running this script.`);
  }

  log('='.repeat(50));
  log('Expense Distribution Batch Download v6 — Direct Page');
  log(`${ENTITIES.length} buildings, period ${PERIOD_FROM}–${PERIOD_TO}`);
  log('RT verified as 1 (Expense Distribution)');
  log('='.repeat(50));

  // ── Process each entity ──────────────────────────────────────────────────
  for (const entity of ENTITIES) {
    try {
      log(`\n── ${entity}: Starting ──`);

      // Set property
      const prop = document.querySelector('input[name*="PropertyLookup"][name*="LookupCode"]');
      if (prop) prop.value = String(entity);

      // Set periods
      const pf = document.querySelector('input[name*="PeriodFrom"]');
      if (pf) pf.value = PERIOD_FROM;
      const pt = document.querySelector('input[name*="PeriodTo"]');
      if (pt) pt.value = PERIOD_TO;

      // Set AP Account (blank for all)
      const ap = document.querySelector('input[name*="APAccountLookup"][name*="LookupCode"]');
      if (ap) ap.value = '';

      // Checkboxes
      const det = document.querySelector('input[name*="ShowDetail"]');
      if (det) det.checked = true;
      const grd = document.querySelector('input[name*="ShowGrid"]');
      if (grd) grd.checked = true;

      // Property postback to validate entity
      log(`  Property postback for ${entity}...`);
      __doPostBack('PropertyLookup:LookupCode', '');
      await new Promise(r => {
        const check = setInterval(() => {
          // Wait for page to settle after postback
          if (document.readyState === 'complete') {
            clearInterval(check);
            r();
          }
        }, 200);
      });
      await sleep(1500);

      // Verify RT is still 1 after property postback
      const rtAfterProp = document.querySelector('select[name*="ReportType"]')?.value;
      log(`  RT after property postback: ${rtAfterProp}`);
      if (rtAfterProp !== '1') {
        log(`  WARNING: RT changed to ${rtAfterProp} — re-selecting Expense Distribution`);
        const rtFix = document.querySelector('select[name*="ReportType"]');
        if (rtFix) {
          rtFix.value = '1';
          rtFix.dispatchEvent(new Event('change', { bubbles: true }));
          __doPostBack('ReportType:DropDownList', '');
          await new Promise(r => {
            const check = setInterval(() => {
              if (document.readyState === 'complete') { clearInterval(check); r(); }
            }, 200);
          });
          await sleep(1000);
        }
      }

      // Re-set fields after postback (ASP.NET may have reset them)
      const prop2 = document.querySelector('input[name*="PropertyLookup"][name*="LookupCode"]');
      if (prop2) prop2.value = String(entity);
      const pf2 = document.querySelector('input[name*="PeriodFrom"]');
      if (pf2) pf2.value = PERIOD_FROM;
      const pt2 = document.querySelector('input[name*="PeriodTo"]');
      if (pt2) pt2.value = PERIOD_TO;
      const ap2 = document.querySelector('input[name*="APAccountLookup"][name*="LookupCode"]');
      if (ap2) ap2.value = '';
      const det2 = document.querySelector('input[name*="ShowDetail"]');
      if (det2) det2.checked = true;
      const grd2 = document.querySelector('input[name*="ShowGrid"]');
      if (grd2) grd2.checked = true;

      // Excel export via FormData
      const form = document.querySelector('form');
      const fd = new FormData(form);
      fd.set('__EVENTTARGET', 'Excel');
      fd.set('__EVENTARGUMENT', '');

      // Force RT in FormData
      for (const [key] of [...fd.entries()]) {
        if (key.toLowerCase().includes('reporttype')) {
          fd.set(key, '1');
        }
      }

      const rtFinal = document.querySelector('select[name*="ReportType"]')?.value;
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
