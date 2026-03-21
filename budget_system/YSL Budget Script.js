/**
 * YSL Annual Budget Batch Downloader v4
 * Paste into Chrome Console on any Yardi page (must be logged in)
 *
 * BEFORE RUNNING: Edit the settings below ↓↓↓
 */
(async function() {
  'use strict';

  // ╔══════════════════════════════════════════════╗
  // ║  EDIT THESE BEFORE EACH RUN                  ║
  // ╠══════════════════════════════════════════════╣
  // ║                                              ║
  const AS_OF_PERIOD = '02/2026';
  // ║                                              ║
  const ENTITIES = [148, 204, 206, 805];
  // ║                                              ║
  const EMAIL = 'JSirotkin@Centuryny.com';
  // ║                                              ║
  // ╚══════════════════════════════════════════════╝
  //
  // AS_OF_PERIOD: MM/YYYY  (e.g. '08/2027')
  // ENTITIES: array of entity codes (e.g. [148, 204, 206, 805])
  // EMAIL: your Century email for Yardi report delivery

  const BASE = '/03578cms/pages';
  const FILTER_URL = `${BASE}/SysSqlScript.aspx?action=Filter&select=reports%2frs_YSL_CMS_Annual_Budget_60612.txt&sMenuSet=iData`;
  const MONITOR_URL = `${BASE}/SysConductorReportMonitor.aspx`;
  const DOWNLOAD_BASE = `${BASE}/SysShuttleDisplayHandler.ashx`;
  const POLL_INTERVAL = 3000;
  const POLL_MAX = 20;
  const DELAY = 2000;

  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const log = msg => console.log(`[YSL] ${msg}`);
  const results = { success: [], failed: [] };

  log('='.repeat(50));
  log(`YSL Batch Download — ${ENTITIES.length} buildings, period ${AS_OF_PERIOD}`);
  log(`User: ${EMAIL}`);
  log('='.repeat(50));

  for (let i = 0; i < ENTITIES.length; i++) {
    const entity = ENTITIES[i];
    log(`\n[${i+1}/${ENTITIES.length}] Entity ${entity}`);

    try {
      // Step 1: GET filter page for fresh hidden fields
      log('  Loading filter page...');
      const f = await fetch(FILTER_URL);
      const fh = await f.text();
      const doc = new DOMParser().parseFromString(fh, 'text/html');
      const hidden = {};
      doc.querySelectorAll('input[type="hidden"]').forEach(el => {
        if (el.name) hidden[el.name] = el.value || '';
      });

      // Step 2: POST to submit report
      log('  Submitting...');
      const post = {
        ...hidden,
        hProp: String(entity),
        sBook: 'Cash',
        sTree: 'tempcen_cf',
        dtAsOfPeriod: AS_OF_PERIOD,
        sTitle: '',
        RptOutput: 'Filexlsx',
        select: 'reports/rs_YSL_CMS_Annual_Budget_60612.txt',
        BPOSTED: '-1',
        WEBSHARENAME: '/03578cms',
        ReportMonitor: '../pages/SysConductorReportMonitor.aspx',
        Records: '',
        FileName: 'YSL_Annual_Budget_(60612)',
        EmailAddr: EMAIL,
        bHasSelectToken: '1',
        __EVENTTARGET: '',
        __EVENTARGUMENT: '',
        download_token_value_id: '',
      };
      const body = Object.entries(post).map(([k,v]) =>
        encodeURIComponent(k) + '=' + encodeURIComponent(v)
      ).join('&');

      const s = await fetch(FILTER_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body,
      });
      const sh = await s.text();

      // Step 3: Extract new record ID from response
      const doc2 = new DOMParser().parseFromString(sh, 'text/html');
      const recEl = doc2.querySelector('input[name="Records"]');
      const recVal = recEl ? recEl.value : '';
      if (!recVal) {
        log('  FAILED: No Records in response');
        results.failed.push({ entity, reason: 'no_records' });
        continue;
      }
      const newId = decodeURIComponent(recVal).split(',')[0].trim();
      log(`  Record ID: ${newId}`);

      // Step 4: Poll monitor for download link
      log('  Polling monitor...');
      let downloaded = false;
      for (let p = 0; p < POLL_MAX; p++) {
        await sleep(POLL_INTERVAL);
        const m = await fetch(`${MONITOR_URL}?Records=${newId}&FilterInfo=&sDir=0&bMonitor=0`);
        const mh = await m.text();
        const dl = mh.match(/sFileName=([^'"&\s]+)/);
        if (dl) {
          // Step 5: Download
          log('  Downloading...');
          const fr = await fetch(`${DOWNLOAD_BASE}?sFileName=${dl[1]}`);
          const blob = await fr.blob();
          const a = document.createElement('a');
          a.href = URL.createObjectURL(blob);
          a.download = `YSL_Annual_Budget_${entity}.xlsx`;
          document.body.appendChild(a);
          a.click();
          document.body.removeChild(a);
          URL.revokeObjectURL(a.href);
          log(`  SUCCESS: ${(blob.size/1024).toFixed(0)} KB`);
          results.success.push({ entity, size: blob.size, id: newId });
          downloaded = true;
          break;
        }
        log(`  Poll ${p+1}/${POLL_MAX}...`);
      }
      if (!downloaded) {
        log('  FAILED: Timed out waiting for report');
        results.failed.push({ entity, reason: 'timeout', id: newId });
      }
    } catch (ex) {
      log(`  ERROR: ${ex.message}`);
      results.failed.push({ entity, reason: ex.message });
    }

    if (i < ENTITIES.length - 1) await sleep(DELAY);
  }

  log('\n' + '='.repeat(50));
  log(`DONE: ${results.success.length} OK, ${results.failed.length} failed`);
  results.success.forEach(r => log(`  OK  ${r.entity} — ${(r.size/1024).toFixed(0)} KB`));
  results.failed.forEach(r => log(`  FAIL ${r.entity} — ${r.reason}`));
  log('='.repeat(50));
  return results;
})();
