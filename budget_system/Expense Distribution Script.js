/**
 * Expense Distribution (Paid Only) Batch Downloader v6 — Full FormData
 * Runs as part of the combined Yardi script (or standalone in console).
 *
 * Fetches APAnalytics.aspx, parses the full form into a virtual DOM,
 * builds FormData from it (capturing ALL inputs/selects/textareas), and
 * POSTs back. This fixes the old approach which only sent hidden fields
 * and caused ASP.NET to ignore ReportType changes.
 *
 * Entities are processed sequentially.
 *
 * BEFORE RUNNING: Edit the settings below
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
  // ╚══════════════════════════════════════════════════╝

  const BASE = '/03578cms/Pages';
  const PAGE_URL = `${BASE}/APAnalytics.aspx?sMenuSet=iData`;

  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const log = msg => console.log(`[ExpDist] ${msg}`);
  const results = { success: [], failed: [] };
  const startTime = Date.now();

  log('='.repeat(50));
  log(`Expense Distribution Batch Download v6`);
  log(`${ENTITIES.length} buildings, period ${PERIOD_FROM}–${PERIOD_TO}`);
  log('='.repeat(50));

  // ── Core: parse HTML into a virtual document ───────────────────────────
  function parseDoc(html) {
    return new DOMParser().parseFromString(html, 'text/html');
  }

  // ── Core: build full FormData from a parsed document's form ────────────
  // This captures hidden fields, selects, text inputs, checkboxes — everything.
  // We then override specific fields for the postback.
  function buildFormData(doc, overrides) {
    const form = doc.querySelector('form');
    if (!form) throw new Error('No form found in page');

    // FormData from DOMParser forms may not work in all browsers,
    // so we manually collect all named form elements.
    const fd = new URLSearchParams();

    // Hidden inputs
    form.querySelectorAll('input[type="hidden"]').forEach(el => {
      if (el.name) fd.set(el.name, el.value || '');
    });

    // Text/password inputs
    form.querySelectorAll('input[type="text"], input[type="password"]').forEach(el => {
      if (el.name) fd.set(el.name, el.value || '');
    });

    // Select elements
    form.querySelectorAll('select').forEach(el => {
      if (el.name) fd.set(el.name, el.value || '');
    });

    // Checkboxes (only send if checked)
    form.querySelectorAll('input[type="checkbox"]').forEach(el => {
      if (el.name && el.checked) fd.set(el.name, el.value || 'on');
    });

    // Radio buttons (only send if checked)
    form.querySelectorAll('input[type="radio"]').forEach(el => {
      if (el.name && el.checked) fd.set(el.name, el.value || '');
    });

    // Textareas
    form.querySelectorAll('textarea').forEach(el => {
      if (el.name) fd.set(el.name, el.textContent || '');
    });

    // Apply overrides
    if (overrides) {
      for (const [key, val] of Object.entries(overrides)) {
        fd.set(key, val);
      }
    }

    return fd;
  }

  // ── Core: POST a form and return the response ──────────────────────────
  async function postPage(doc, eventTarget, overrides) {
    const allOverrides = {
      '__EVENTTARGET': eventTarget,
      '__EVENTARGUMENT': '',
      ...overrides,
    };
    const fd = buildFormData(doc, allOverrides);

    return await fetch(PAGE_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: fd.toString(),
    });
  }

  // ── Core: POST and parse the HTML response into a new virtual doc ──────
  async function postAndParse(doc, eventTarget, overrides) {
    const resp = await postPage(doc, eventTarget, overrides);
    const html = await resp.text();
    return parseDoc(html);
  }

  // ── Core: POST for Excel export and return blob ────────────────────────
  async function postForExcel(doc, overrides) {
    const resp = await postPage(doc, 'Excel', overrides);

    const contentType = resp.headers.get('content-type') || '';
    const contentDisp = resp.headers.get('content-disposition') || '';

    // Direct file response
    if (contentType.includes('spreadsheet') || contentType.includes('excel') ||
        contentType.includes('octet-stream') || contentDisp.includes('attachment')) {
      return await resp.blob();
    }

    // HTML response — check for shuttle or monitor fallback
    const respHtml = await resp.text();

    const dlMatch = respHtml.match(/sFileName=([^'"&\s]+)/);
    if (dlMatch) {
      const dlResp = await fetch(`${BASE}/SysShuttleDisplayHandler.ashx?sFileName=${dlMatch[1]}`);
      return await dlResp.blob();
    }

    const recMatch = respHtml.match(/name="Records"\s+value="([^"]+)"/);
    if (recMatch) {
      const recordId = decodeURIComponent(recMatch[1]).split(',')[0].trim();
      log(`  Queued (${recordId}), polling monitor...`);
      for (let p = 0; p < 20; p++) {
        await sleep(3000);
        const monResp = await fetch(
          `${BASE}/SysConductorReportMonitor.aspx?Records=${recordId}&FilterInfo=&sDir=0&bMonitor=0`
        );
        const monHtml = await monResp.text();
        const monDl = monHtml.match(/sFileName=([^'"&\s]+)/);
        if (monDl) {
          const dlResp = await fetch(`${BASE}/SysShuttleDisplayHandler.ashx?sFileName=${monDl[1]}`);
          return await dlResp.blob();
        }
      }
      throw new Error('monitor_timeout');
    }

    throw new Error('unexpected_response (got HTML instead of file)');
  }

  // ── Step 1: GET the APAnalytics page ───────────────────────────────────

  log('Fetching AP Analytics page...');
  const getResp = await fetch(PAGE_URL);
  let doc = parseDoc(await getResp.text());

  const rtSelect = doc.querySelector('select[name*="ReportType"]');
  if (!rtSelect) throw new Error('ReportType dropdown not found (session expired?)');
  log('Page loaded.');

  // ── Step 2: Set ReportType to Expense Distribution (Paid Only) ─────────

  log('Setting ReportType to Expense Distribution (Paid Only)...');
  rtSelect.value = '2';
  doc = await postAndParse(doc, 'ReportType:DropDownList', {
    'ReportType:DropDownList': '2',
  });
  log('ReportType set.');

  // ── Step 3: Process each entity ────────────────────────────────────────

  for (const entity of ENTITIES) {
    try {
      log(`\n  ${entity}: Validating property...`);

      // POST to validate property lookup
      doc = await postAndParse(doc, 'PropertyLookup:LookupCode', {
        'ReportType:DropDownList': '2',
        'PropertyLookup:LookupCode': String(entity),
        'PropertyLookup:LookupDesc': '',
        'PeriodFrom:TextBox': PERIOD_FROM,
        'PeriodTo:TextBox': PERIOD_TO,
        'ShowDetail:CheckBox': 'on',
        'ShowGrid:CheckBox': 'on',
      });

      log(`  ${entity}: Property validated, requesting Excel...`);

      // POST to trigger Excel export
      const blob = await postForExcel(doc, {
        'ReportType:DropDownList': '2',
        'PropertyLookup:LookupCode': String(entity),
        'PropertyLookup:LookupDesc': '',
        'PeriodFrom:TextBox': PERIOD_FROM,
        'PeriodTo:TextBox': PERIOD_TO,
        'ShowDetail:CheckBox': 'on',
        'ShowGrid:CheckBox': 'on',
      });

      triggerDownload(blob, entity);

      const sizeKB = (blob.size / 1024).toFixed(0);
      log(`  ${entity}: OK — ${sizeKB} KB`);
      results.success.push({ entity, ok: true, size: blob.size });

    } catch (ex) {
      log(`  ${entity}: FAILED — ${ex.message}`);
      results.failed.push({ entity, ok: false, reason: ex.message });
    }
  }

  // ── triggerDownload ────────────────────────────────────────────────────

  function triggerDownload(blob, entity) {
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `ExpenseDistribution_${entity}.xlsx`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(a.href);
  }

  // ── Summary ────────────────────────────────────────────────────────────

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
