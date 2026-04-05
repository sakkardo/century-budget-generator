/**
 * CMS Association Maintenance Proof Batch Downloader v2 — Iframe Approach
 *
 * KEY FIX: v1 used fetch-only approach which doesn't maintain ASP.NET session
 * state properly. v2 uses an iframe with real postbacks (same pattern as
 * AP Aging Script v5). Two-step process per entity: Generate → find shuttle
 * link in response → download via SysShuttleDisplayHandler.
 *
 * Uses fresh iframe per entity to avoid state pollution.
 *
 * BEFORE RUNNING: Edit the settings below
 */
(async function() {
  'use strict';

  // ╔══════════════════════════════════════════════════╗
  // ║  EDIT THESE BEFORE EACH RUN                      ║
  // ╠══════════════════════════════════════════════════╣
  // Entity → charge code mapping (coop=maint, condo=common)
  const ENTITY_CHARGES = {204: 'maint', 206: 'common', 148: 'maint', 805: 'maint'};
  const ENTITIES = Object.keys(ENTITY_CHARGES).map(Number);
  // ╚══════════════════════════════════════════════════╝

  const BASE = '/03578cms/Pages';
  const PAGE_URL = `${BASE}/CustomCorrespGenerate.aspx?ReportCode=Adhoc_AMP`;
  const SHUTTLE_URL = `${BASE}/SysShuttleDisplayHandler.ashx`;

  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const log = msg => console.log(`[MaintProof] ${msg}`);
  const results = { success: [], failed: [] };
  const startTime = Date.now();

  log('='.repeat(50));
  log('Maintenance Proof Batch Download v2 (Iframe)');
  log(`${ENTITIES.length} buildings`);
  log('='.repeat(50));

  // ── Load page in a working iframe ─────────────────────────────────────────
  log('Loading Maintenance Proof page in iframe...');
  const workFrame = document.createElement('iframe');
  workFrame.name = '_maintProofWork';
  workFrame.style.display = 'none';
  workFrame.src = PAGE_URL;
  document.body.appendChild(workFrame);
  await new Promise(r => { workFrame.onload = r; });
  await sleep(1000);

  const wDoc = () => workFrame.contentDocument;
  const wWin = () => workFrame.contentWindow;

  if (!wDoc()?.querySelector('input[name*="Ysi2376"]')) {
    document.body.removeChild(workFrame);
    throw new Error('Failed to load Maintenance Proof page in iframe (session expired?)');
  }
  log('Maintenance Proof page loaded.');

  // ── Helper: set form fields ───────────────────────────────────────────────
  function setFields(entity) {
    const d = wDoc();
    const chargeCode = ENTITY_CHARGES[entity] || 'maint';

    // Property
    const prop = d.querySelector('input[name="Ysi2376:LookupCode"]');
    if (prop) prop.value = String(entity);

    // Unit Code — blank = all
    const unit = d.querySelector('input[name="Ysi2377:LookupCode"]');
    if (unit) unit.value = '';

    // Status
    const status = d.querySelector('input[name="Ysi2378:LookupCode"]');
    if (status) status.value = 'Current';

    // Charge Code
    const charge = d.querySelector('input[name="Ysi2379:LookupCode"]');
    if (charge) charge.value = chargeCode;

    // Is Excluded dropdown
    const excluded = d.querySelector('select[name="Ysi2380:DropDownList"]');
    if (excluded) excluded.value = 'No';

    // Output Type
    const output = d.querySelector('select[name="YsiOutpuType:DropDownList"]');
    if (output) output.value = 'Excel';

    // Checkboxes — Merge Reports and Show Grid ON, others OFF
    const merge = d.querySelector('input[name="IsMergedReport:CheckBox"]');
    if (merge) merge.checked = true;
    const grid = d.querySelector('input[name="IsShowGrid:CheckBox"]');
    if (grid) grid.checked = true;

    log(`  ${entity}: Fields set (charge: ${chargeCode})`);
  }

  // ── Process each entity sequentially ──────────────────────────────────────
  for (const entity of ENTITIES) {
    try {
      log(`\n── ${entity}: Starting ──`);

      // STEP 1: Reload fresh iframe for each entity
      workFrame.src = PAGE_URL;
      await new Promise(r => { workFrame.onload = r; });
      await sleep(1000);

      if (!wDoc()?.querySelector('input[name*="Ysi2376"]')) {
        results.failed.push({ entity, ok: false, reason: 'Page load failed' });
        continue;
      }

      // STEP 2: Set all form fields
      setFields(entity);

      // STEP 2b: Validate Property lookup via postback so Yardi binds the
      // entity server-side BEFORE submit. Without this, the server reused
      // the last entity's cached context and could return data for the
      // wrong entity under the new entity's filename.
      log(`  ${entity}: Property lookup postback...`);
      try {
        wWin().__doPostBack('Ysi2376:LookupCode', '');
        await new Promise(r => { workFrame.onload = r; });
        await sleep(500);
      } catch (e) {
        log(`  ${entity}: Property postback warning: ${e.message}`);
      }

      // STEP 2c: Re-set all fields — postback replaces form HTML
      setFields(entity);

      // STEP 2d: Guard — verify Property value stuck after the postback
      const propCheck = wDoc().querySelector('input[name="Ysi2376:LookupCode"]')?.value;
      if (String(propCheck) !== String(entity)) {
        log(`  ✗ ${entity} ABORT — Property="${propCheck}" after lookup postback, expected "${entity}"`);
        results.failed.push({ entity, ok: false, reason: `property_bind_${propCheck}` });
        continue;
      }

      // STEP 3: Submit Generate via fetch POST (NOT iframe postback).
      // ROOT CAUSE FIX: Iframe __doPostBack('btnSubmit') caused Yardi to
      // respond with Content-Disposition: attachment, triggering a native
      // browser download using Yardi's internal filename (e.g. Adhoc_AMP_433)
      // BEFORE our shuttle-link logic could run. That produced a duplicate
      // byte-identical file with the wrong name on every run.
      // Fetch POST with RequestAction=AutoPostBack parses the response HTML
      // without navigating the iframe, so no auto-download fires.
      log(`  ${entity}: Generating via fetch POST (property=${propCheck})...`);
      const form = wDoc().querySelector('form');
      const fd = new FormData(form);
      fd.set('__EVENTTARGET', 'btnSubmit');
      fd.set('__EVENTARGUMENT', '');
      fd.set('RequestAction', 'AutoPostBack');
      fd.set('BDATACHANGED', '1');
      const actionUrl = form.action || `${BASE}/CustomCorrespGenerate.aspx?ReportCode=Adhoc_AMP`;
      const genResp = await fetch(actionUrl, { method: 'POST', body: fd, credentials: 'include' });
      const responseHtml = await genResp.text();
      await sleep(500);

      // Look for the merged report "here" link first (most reliable)
      // Pattern: SysShuttleDisplayHandler.ashx?sFileName=XXX
      const shuttleMatches = responseHtml.match(/SysShuttleDisplayHandler\.ashx\?sFileName=[^'"&\s\)]+/g);

      if (shuttleMatches && shuttleMatches.length > 0) {
        // Use the first shuttle link (the merged "here" link)
        const shuttleUrl = shuttleMatches[0];
        log(`  ${entity}: Found ${shuttleMatches.length} shuttle link(s), downloading first...`);

        const dlResp = await fetch(`${BASE}/${shuttleUrl.replace(/.*SysShuttleDisplayHandler/, 'SysShuttleDisplayHandler')}`);
        const blob = await dlResp.blob();

        if (blob && blob.size > 100) {
          triggerDownload(blob, entity);
          log(`  ✓ ${entity} — ${(blob.size / 1024).toFixed(0)} KB`);
          results.success.push({ entity, ok: true, size: blob.size });
        } else {
          log(`  ✗ ${entity} — shuttle returned ${blob.size} bytes (too small)`);
          results.failed.push({ entity, ok: false, reason: `shuttle_tiny_${blob.size}` });
        }
      } else {
        // Fallback: check for monitor/queue pattern
        const recMatch = responseHtml.match(/name="Records"\s+value="([^"]+)"/);
        if (recMatch) {
          const recordId = decodeURIComponent(recMatch[1]).split(',')[0].trim();
          log(`  ${entity}: Queued (${recordId}), polling...`);

          let downloaded = false;
          for (let p = 0; p < 20; p++) {
            await sleep(3000);
            const monResp = await fetch(
              `${BASE}/SysConductorReportMonitor.aspx?Records=${recordId}&FilterInfo=&sDir=0&bMonitor=0`
            );
            const monHtml = await monResp.text();
            const monDl = monHtml.match(/SysShuttleDisplayHandler\.ashx\?sFileName=[^'"&\s]+/);
            if (monDl) {
              const dlResp = await fetch(`${BASE}/${monDl[0].replace(/.*SysShuttleDisplayHandler/, 'SysShuttleDisplayHandler')}`);
              const blob = await dlResp.blob();
              triggerDownload(blob, entity);
              log(`  ✓ ${entity} — ${(blob.size / 1024).toFixed(0)} KB (via monitor)`);
              results.success.push({ entity, ok: true, size: blob.size, via: 'monitor' });
              downloaded = true;
              break;
            }
            log(`  ${entity}: Poll ${p+1}/20...`);
          }
          if (!downloaded) {
            results.failed.push({ entity, ok: false, reason: 'monitor_timeout' });
          }
        } else {
          // Check if the response shows an error or "no data" message
          if (responseHtml.includes('No records found') || responseHtml.includes('no data')) {
            log(`  ✗ ${entity} — no records found for this entity/charge code`);
            results.failed.push({ entity, ok: false, reason: 'no_records' });
          } else {
            log(`  ✗ ${entity} — no shuttle link or monitor queue found`);
            results.failed.push({ entity, ok: false, reason: 'no_download_link' });
          }
        }
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
    a.download = `Adhoc_AMP_${entity}.xlsx`;
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
