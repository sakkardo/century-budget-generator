/**
 * Expense Distribution (Paid Only) Batch Downloader v3
 * Paste into Chrome Console on any Yardi page (must be logged in)
 *
 * Uses fetch()-based approach for reliability and speed.
 * Runs entities in parallel batches (default 5 at a time) for maximum
 * throughput while avoiding Yardi rate limits.
 *
 * BEFORE RUNNING: Edit the settings below ↓↓↓
 */
(async function() {
  'use strict';

  // ╔══════════════════════════════════════════════════╗
  // ║  EDIT THESE BEFORE EACH RUN                      ║
  // ╠══════════════════════════════════════════════════╣
  // ║                                                  ║
  const PERIOD_FROM = '01/2026';
  const PERIOD_TO   = '03/2026';
  // ║                                                  ║
  const ENTITIES = [148, 204, 206, 805];
  // ║                                                  ║
  const BATCH_SIZE = 5;  // How many to run in parallel
  // ║                                                  ║
  // ╚══════════════════════════════════════════════════╝

  const BASE = '/03578cms/Pages';
  const PAGE_URL = `${BASE}/APAnalytics.aspx?sMenuSet=iData`;
  const BATCH_DELAY = 1000;

  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const log = msg => console.log(`[ExpDist] ${msg}`);
  const results = { success: [], failed: [] };
  const startTime = Date.now();

  log('='.repeat(50));
  log(`Expense Distribution Batch Download`);
  log(`${ENTITIES.length} buildings, period ${PERIOD_FROM}–${PERIOD_TO}`);
  log(`Parallel batch size: ${BATCH_SIZE}`);
  log('='.repeat(50));

  // ── Download a single entity ──────────────────────────────────────────────

  async function downloadEntity(entity) {
    try {
      // Step 1: GET fresh viewstate
      const getResp = await fetch(PAGE_URL);
      const html = await getResp.text();
      const doc = new DOMParser().parseFromString(html, 'text/html');

      const hidden = {};
      doc.querySelectorAll('input[type="hidden"]').forEach(el => {
        if (el.name) hidden[el.name] = el.value || '';
      });

      if (!hidden['__VIEWSTATE'] && !hidden['__VIEWSTATE__']) {
        return { entity, ok: false, reason: 'no_viewstate (session expired?)' };
      }

      // Step 2a: POST to change ReportType to "Expense Distribution (Paid Only)" (value=2)
      // ASP.NET WebForms requires the dropdown change as its own postback
      const rtPost = {
        ...hidden,
        '__EVENTTARGET': 'ReportType:DropDownList',
        '__EVENTARGUMENT': '',
        'ReportType:DropDownList': '2',
      };
      const rtBody = Object.entries(rtPost).map(([k, v]) =>
        encodeURIComponent(k) + '=' + encodeURIComponent(v)
      ).join('&');
      const rtResp = await fetch(PAGE_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: rtBody,
      });
      const rtHtml = await rtResp.text();
      const rtDoc = new DOMParser().parseFromString(rtHtml, 'text/html');
      const hidden1b = {};
      rtDoc.querySelectorAll('input[type="hidden"]').forEach(el => {
        if (el.name) hidden1b[el.name] = el.value || '';
      });

      log(`  ${entity}: Report type set to Expense Distribution (Paid Only)`);

      // Step 2b: POST to set the property filter (trigger lookup validation)
      const lookupPost = {
        ...hidden1b,
        '__EVENTTARGET': 'PropertyLookup:LookupCode',
        '__EVENTARGUMENT': '',
        'ReportType:DropDownList': '2',
        'PropertyLookup:LookupCode': String(entity),
        'PropertyLookup:LookupDesc': '',
        'APAccountLookup:LookupCode': '',
        'PostCodeLookup:LookupCode': '',
        'ControlNoFrom:TextBox': '',
        'ControlNoTo:TextBox': '',
        'BatchNoFrom:TextBox': '',
        'BatchNoTo:TextBox': '',
        'PeriodFrom:TextBox': PERIOD_FROM,
        'PeriodTo:TextBox': PERIOD_TO,
        'AgeAsOf:TextBox': '',
        'DateFrom:TextBox': '',
        'DateTo:TextBox': '',
        'DueDateFromText:TextBox': '',
        'DueDateText:TextBox': '',
        'CheckNoFrom:TextBox': '',
        'CheckNoTo:TextBox': '',
        'CheckPeriodFrom:TextBox': '',
        'CheckPeriodTo:TextBox': '',
        'BankLookup:LookupCode': '',
        'CompanyLookup:LookupCode': '',
        'VendorLookup:LookupCode': '',
        'AccountLookup:LookupCode': '',
        'StateCountryText:TextBox': '',
        'CityText:TextBox': '',
        'ZipText:TextBox': '',
        'WCExpDate:TextBox': '',
        'LiabInsDate:TextBox': '',
        'ReferenceText:TextBox': '',
        'NotesText:TextBox': '',
        'ShowDetail:CheckBox': 'on',
        'ShowGrid:CheckBox': 'on',
      };

      const lookupBody = Object.entries(lookupPost).map(([k, v]) =>
        encodeURIComponent(k) + '=' + encodeURIComponent(v)
      ).join('&');

      const lookupResp = await fetch(PAGE_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: lookupBody,
      });

      // Parse the response to get updated viewstate after lookup validation
      const lookupHtml = await lookupResp.text();
      const lookupDoc = new DOMParser().parseFromString(lookupHtml, 'text/html');
      const hidden2 = {};
      lookupDoc.querySelectorAll('input[type="hidden"]').forEach(el => {
        if (el.name) hidden2[el.name] = el.value || '';
      });

      log(`  ${entity}: Property lookup validated, requesting Excel...`);

      // Step 2c: Third POST to actually get the Excel export with validated property
      const post = {
        ...hidden2,
        '__EVENTTARGET': 'Excel',
        '__EVENTARGUMENT': '',
        'ReportType:DropDownList': '2',
        'PropertyLookup:LookupCode': String(entity),
        'PropertyLookup:LookupDesc': '',
        'APAccountLookup:LookupCode': '',
        'PostCodeLookup:LookupCode': '',
        'ControlNoFrom:TextBox': '',
        'ControlNoTo:TextBox': '',
        'BatchNoFrom:TextBox': '',
        'BatchNoTo:TextBox': '',
        'PeriodFrom:TextBox': PERIOD_FROM,
        'PeriodTo:TextBox': PERIOD_TO,
        'AgeAsOf:TextBox': '',
        'DateFrom:TextBox': '',
        'DateTo:TextBox': '',
        'DueDateFromText:TextBox': '',
        'DueDateText:TextBox': '',
        'CheckNoFrom:TextBox': '',
        'CheckNoTo:TextBox': '',
        'CheckPeriodFrom:TextBox': '',
        'CheckPeriodTo:TextBox': '',
        'BankLookup:LookupCode': '',
        'CompanyLookup:LookupCode': '',
        'VendorLookup:LookupCode': '',
        'AccountLookup:LookupCode': '',
        'StateCountryText:TextBox': '',
        'CityText:TextBox': '',
        'ZipText:TextBox': '',
        'WCExpDate:TextBox': '',
        'LiabInsDate:TextBox': '',
        'ReferenceText:TextBox': '',
        'NotesText:TextBox': '',
        'ShowDetail:CheckBox': 'on',
        'ShowGrid:CheckBox': 'on',
      };

      const body = Object.entries(post).map(([k, v]) =>
        encodeURIComponent(k) + '=' + encodeURIComponent(v)
      ).join('&');

      const postResp = await fetch(PAGE_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body,
      });

      // Step 3: Handle response
      const contentType = postResp.headers.get('content-type') || '';
      const contentDisp = postResp.headers.get('content-disposition') || '';

      // Path A: Direct file download
      if (contentType.includes('spreadsheet') || contentType.includes('excel') ||
          contentType.includes('octet-stream') || contentDisp.includes('attachment')) {
        const blob = await postResp.blob();
        triggerDownload(blob, entity);
        return { entity, ok: true, size: blob.size };
      }

      // Path B: HTML response — check for shuttle or monitor
      const respHtml = await postResp.text();

      // B1: Shuttle download link in response
      const dlMatch = respHtml.match(/sFileName=([^'"&\s]+)/);
      if (dlMatch) {
        const dlResp = await fetch(`${BASE}/SysShuttleDisplayHandler.ashx?sFileName=${dlMatch[1]}`);
        const blob = await dlResp.blob();
        triggerDownload(blob, entity);
        return { entity, ok: true, size: blob.size, via: 'shuttle' };
      }

      // B2: Monitor queue pattern
      const recMatch = respHtml.match(/name="Records"\s+value="([^"]+)"/);
      if (recMatch) {
        const recordId = decodeURIComponent(recMatch[1]).split(',')[0].trim();
        log(`  ${entity}: Queued (${recordId}), polling...`);

        for (let p = 0; p < 20; p++) {
          await sleep(3000);
          const monResp = await fetch(
            `${BASE}/SysConductorReportMonitor.aspx?Records=${recordId}&FilterInfo=&sDir=0&bMonitor=0`
          );
          const monHtml = await monResp.text();
          const monDl = monHtml.match(/sFileName=([^'"&\s]+)/);
          if (monDl) {
            const dlResp = await fetch(`${BASE}/SysShuttleDisplayHandler.ashx?sFileName=${monDl[1]}`);
            const blob = await dlResp.blob();
            triggerDownload(blob, entity);
            return { entity, ok: true, size: blob.size, via: 'monitor' };
          }
        }
        return { entity, ok: false, reason: 'monitor_timeout' };
      }

      return { entity, ok: false, reason: 'unexpected_response' };
    } catch (ex) {
      return { entity, ok: false, reason: ex.message };
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

  // ── Run in parallel batches ───────────────────────────────────────────────

  const batches = [];
  for (let i = 0; i < ENTITIES.length; i += BATCH_SIZE) {
    batches.push(ENTITIES.slice(i, i + BATCH_SIZE));
  }

  for (let b = 0; b < batches.length; b++) {
    const batch = batches[b];
    log(`\nBatch ${b + 1}/${batches.length}: entities ${batch.join(', ')}`);

    // Run all entities in this batch in parallel
    const batchResults = await Promise.all(batch.map(entity => downloadEntity(entity)));

    // Process results
    batchResults.forEach(r => {
      if (r.ok) {
        log(`  ✓ ${r.entity} — ${(r.size / 1024).toFixed(0)} KB${r.via ? ' (' + r.via + ')' : ''}`);
        results.success.push(r);
      } else {
        log(`  ✗ ${r.entity} — ${r.reason}`);
        results.failed.push(r);
      }
    });

    // Brief pause between batches
    if (b < batches.length - 1) await sleep(BATCH_DELAY);
  }

  // ── Summary ───────────────────────────────────────────────────────────────

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  log('\n' + '='.repeat(50));
  log(`DONE in ${elapsed}s: ${results.success.length} OK, ${results.failed.length} failed`);
  if (results.failed.length > 0) {
    log('Failed entities:');
    results.failed.forEach(r => log(`  ${r.entity}: ${r.reason}`));
  }
  log('='.repeat(50));
  return results;
})();
