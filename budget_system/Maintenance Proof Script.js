/**
 * CMS Association Maintenance Proof Batch Downloader v1
 * Paste into Chrome Console on any Yardi page (must be logged in)
 *
 * Downloads the Ad Hoc Maintenance Proof (Adhoc_AMP) report for each entity.
 * Uses CustomCorrespGenerate.aspx with ReportCode=Adhoc_AMP.
 *
 * BEFORE RUNNING: Edit the settings below ↓↓↓
 */
(async function() {
  'use strict';

  // ╔══════════════════════════════════════════════════╗
  // ║  EDIT THESE BEFORE EACH RUN                      ║
  // ╠══════════════════════════════════════════════════╣
  // ║                                                  ║
  // Entity → charge code mapping (coop=maint, condo=common)
  const ENTITY_CHARGES = {204: 'maint', 206: 'common', 148: 'maint', 805: 'maint'};
  const ENTITIES = Object.keys(ENTITY_CHARGES).map(Number);
  // ║                                                  ║
  const BATCH_SIZE = 3;  // How many to run in parallel
  // ║                                                  ║
  // ╚══════════════════════════════════════════════════╝

  const BASE = '/03578cms/Pages';
  const PAGE_URL = `${BASE}/CustomCorrespGenerate.aspx?ReportCode=Adhoc_AMP`;
  const SHUTTLE_URL = `${BASE}/SysShuttleDisplayHandler.ashx`;
  const BATCH_DELAY = 1000;

  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const log = msg => console.log(`[MaintProof] ${msg}`);
  const results = { success: [], failed: [] };
  const startTime = Date.now();

  log('='.repeat(50));
  log(`Maintenance Proof Batch Download`);
  log(`${ENTITIES.length} buildings, batch size ${BATCH_SIZE}`);
  log('='.repeat(50));

  // ── Download a single entity ──────────────────────────────────────────────

  async function downloadEntity(entity) {
    try {
      // Step 1: GET the page for fresh viewstate + event validation
      const getResp = await fetch(PAGE_URL);
      const html = await getResp.text();
      const doc = new DOMParser().parseFromString(html, 'text/html');

      const hidden = {};
      doc.querySelectorAll('input[type="hidden"]').forEach(el => {
        if (el.name) hidden[el.name] = el.value || '';
      });

      if (!hidden['__VIEWSTATE__'] && !hidden['__VIEWSTATE']) {
        return { entity, ok: false, reason: 'no_viewstate (session expired?)' };
      }

      const chargeCode = ENTITY_CHARGES[entity] || 'maint';
      log(`  ${entity}: Submitting Generate request (charge: ${chargeCode})...`);

      // Step 2: POST to generate the report
      const post = {
        ...hidden,
        '__EVENTTARGET': 'btnSubmit_Button',
        '__EVENTARGUMENT': '',
        '__LASTFOCUS': '',
        'txtCheckBoxValues:TextBox': '',
        'Ysi2376:LookupCode': String(entity),  // Property
        'Ysi2377:LookupCode': '',               // Unit Code (blank = all)
        'Ysi2378:LookupCode': 'Current',        // Status
        'Ysi2379:LookupCode': chargeCode,       // Charge Code (maint or common)
        'Ysi2380:DropDownList': 'No',           // Is Excluded
        'txtMergedFileEmailID:TextBox': '',
        'txtEmailRoleFlag:TextBox': 'False',
        'YsiOutpuType:DropDownList': 'Excel',   // Output Type
        'IsAttach:CheckBox': 'on',
        'IsShowGrid:CheckBox': 'on',
        'txtEmailAddress:TextBox': '',
        'BSAVE': '',
        'bDevMode': '',
        'IsEditMode': '',
        'RequestAction': '',
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
        return { entity, ok: true, size: blob.size, via: 'direct' };
      }

      // Path B: HTML response — look for shuttle download link
      const respHtml = await postResp.text();

      // B1: Shuttle link in response (View Report link pattern)
      const dlMatch = respHtml.match(/sFileName=([^'"&\s\)]+)/);
      if (dlMatch) {
        log(`  ${entity}: Found shuttle link, downloading...`);
        const dlResp = await fetch(`${SHUTTLE_URL}?sFileName=${dlMatch[1]}`);
        const blob = await dlResp.blob();
        triggerDownload(blob, entity);
        return { entity, ok: true, size: blob.size, via: 'shuttle' };
      }

      // B2: Monitor queue pattern (fallback)
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
            const dlResp = await fetch(`${SHUTTLE_URL}?sFileName=${monDl[1]}`);
            const blob = await dlResp.blob();
            triggerDownload(blob, entity);
            return { entity, ok: true, size: blob.size, via: 'monitor' };
          }
          log(`  ${entity}: Poll ${p+1}/20...`);
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
    a.download = `Adhoc_AMP_${entity}.xlsx`;
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

    const batchResults = await Promise.all(batch.map(entity => downloadEntity(entity)));

    batchResults.forEach(r => {
      if (r.ok) {
        log(`  ✓ ${r.entity} — ${(r.size / 1024).toFixed(0)} KB${r.via ? ' (' + r.via + ')' : ''}`);
        results.success.push(r);
      } else {
        log(`  ✗ ${r.entity} — ${r.reason}`);
        results.failed.push(r);
      }
    });

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
