/**
 * Expense Distribution Batch Downloader v8 — Click-the-Button Approach
 *
 * Instead of building FormData and using fetch (which ASP.NET ignores
 * because ViewState overrides our field values), this script:
 * 1. Finds the APAnalytics form in whatever Yardi frame it lives in
 * 2. Fills in property/period fields
 * 3. Clicks the actual "Excel" button on the page
 * 4. Waits for the download, then moves to the next entity
 *
 * PREREQUISITE: Select "Expense Distribution (Paid Only)" in the dropdown FIRST.
 * The script verifies this before running.
 */
(async function() {
  'use strict';

  const PERIOD_FROM = '01/2026';
  const PERIOD_TO   = '03/2026';
  const ENTITIES = [148, 204, 206, 805];

  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const log = msg => console.log(`[ExpDist] ${msg}`);
  const results = { success: [], failed: [] };
  const startTime = Date.now();

  // ── Find the document containing the APAnalytics form ─────────────────
  function findDoc() {
    if (document.querySelector('select[name*="ReportType"]')) return { doc: document, win: window };
    for (let i = 0; i < window.frames.length; i++) {
      try {
        const fd = window.frames[i].document;
        if (fd.querySelector('select[name*="ReportType"]')) return { doc: fd, win: window.frames[i] };
        for (let j = 0; j < window.frames[i].frames.length; j++) {
          try {
            const fd2 = window.frames[i].frames[j].document;
            if (fd2.querySelector('select[name*="ReportType"]')) return { doc: fd2, win: window.frames[i].frames[j] };
          } catch(e) {}
        }
      } catch(e) {}
    }
    return null;
  }

  let ctx = findDoc();
  if (!ctx) {
    throw new Error('Cannot find APAnalytics form. Navigate to Payable Analytics in Yardi first.');
  }

  const rtCheck = ctx.doc.querySelector('select[name*="ReportType"]');
  const rtText = rtCheck.options[rtCheck.selectedIndex]?.text || '';
  log(`Found form. RT="${rtText}" (value=${rtCheck.value})`);

  // Accept RT=1 or RT=2 (both are Expense Distribution variants)
  if (rtCheck.value !== '1' && rtCheck.value !== '2') {
    throw new Error(`Report Type is "${rtText}". Select "Expense Distribution" or "Expense Distribution (Paid Only)" first.`);
  }

  log('='.repeat(50));
  log('Expense Distribution v8 — Click-the-Button');
  log(`${ENTITIES.length} buildings, period ${PERIOD_FROM}–${PERIOD_TO}`);
  log(`Report: ${rtText}`);
  log('='.repeat(50));

  for (const entity of ENTITIES) {
    try {
      log(`\n── ${entity}: Starting ──`);

      ctx = findDoc();
      if (!ctx) { results.failed.push({ entity, ok: false, reason: 'Lost frame' }); continue; }

      // Fill in property
      const prop = ctx.doc.querySelector('input[name*="PropertyLookup"][name*="LookupCode"]');
      if (prop) prop.value = String(entity);

      // Fill in periods
      const pf = ctx.doc.querySelector('input[name*="PeriodFrom"]');
      if (pf) pf.value = PERIOD_FROM;
      const pt = ctx.doc.querySelector('input[name*="PeriodTo"]');
      if (pt) pt.value = PERIOD_TO;

      // Clear AP Account
      const ap = ctx.doc.querySelector('input[name*="APAccountLookup"][name*="LookupCode"]');
      if (ap) ap.value = '';

      // Checkboxes
      const det = ctx.doc.querySelector('input[name*="ShowDetail"]');
      if (det) det.checked = true;
      const grd = ctx.doc.querySelector('input[name*="ShowGrid"]');
      if (grd) grd.checked = true;

      // Do property postback so server validates the entity
      log(`  Property postback...`);
      ctx.win.__doPostBack('PropertyLookup:LookupCode', '');
      await sleep(3000);

      // Re-find after postback
      ctx = findDoc();
      if (!ctx) { results.failed.push({ entity, ok: false, reason: 'Lost frame after postback' }); continue; }

      // Check RT didn't change
      const rtNow = ctx.doc.querySelector('select[name*="ReportType"]')?.value;
      log(`  RT after postback: ${rtNow}`);

      // Re-fill periods (postback may have cleared them)
      const pf2 = ctx.doc.querySelector('input[name*="PeriodFrom"]');
      if (pf2) pf2.value = PERIOD_FROM;
      const pt2 = ctx.doc.querySelector('input[name*="PeriodTo"]');
      if (pt2) pt2.value = PERIOD_TO;
      const ap2 = ctx.doc.querySelector('input[name*="APAccountLookup"][name*="LookupCode"]');
      if (ap2) ap2.value = '';

      // Find and click the Excel button
      const excelBtn = ctx.doc.querySelector('input[value="Excel"]')
                    || ctx.doc.querySelector('input[id*="Excel"]')
                    || ctx.doc.querySelector('button[id*="Excel"]');

      if (!excelBtn) {
        // Try finding by text content
        const allInputs = ctx.doc.querySelectorAll('input[type="submit"], input[type="button"], button');
        let found = null;
        for (const btn of allInputs) {
          if ((btn.value || btn.textContent || '').includes('Excel')) {
            found = btn;
            break;
          }
        }
        if (found) {
          log(`  Clicking Excel button (by text)...`);
          found.click();
        } else {
          // Last resort: use __doPostBack with Excel target
          log(`  No Excel button found — using __doPostBack('Excel', '')...`);
          ctx.win.__doPostBack('Excel', '');
        }
      } else {
        log(`  Clicking Excel button...`);
        excelBtn.click();
      }

      // Wait for download — Yardi will either download directly or queue the report
      log(`  Waiting for download...`);
      await sleep(5000);

      log(`  ✓ ${entity} — Excel triggered (check your downloads)`);
      results.success.push({ entity, ok: true });

    } catch (ex) {
      log(`  ✗ ${entity} — ${ex.message}`);
      results.failed.push({ entity, ok: false, reason: ex.message });
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  log('\n' + '='.repeat(50));
  log(`DONE in ${elapsed}s: ${results.success.length} OK, ${results.failed.length} failed`);
  if (results.failed.length) results.failed.forEach(r => log(`  ${r.entity}: ${r.reason}`));
  log('='.repeat(50));
  return results;
})();
