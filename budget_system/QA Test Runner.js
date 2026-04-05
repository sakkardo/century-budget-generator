/**
 * QA Test Runner — End-to-end validation of the combined Yardi script.
 *
 * Run in the Yardi browser tab (same origin required for iframe downloads).
 * Needs an active Yardi session + Railway backend reachable.
 *
 * What it does:
 *   1. Installs download interceptor (captures each <a download>.click()).
 *   2. Fetches the combined script from /api/generate-script.
 *   3. Evals it and waits for completion.
 *   4. Polls /api/qa/verify to get server-side truth.
 *   5. Prints a structured PASS/FAIL report.
 *
 * EDIT THE CONFIG BELOW if needed.
 */
(async function() {
  'use strict';

  // ╔══════════════════════════════════════╗
  // ║  TEST CONFIG                         ║
  // ╠══════════════════════════════════════╣
  const CFG = {
    railway: 'https://century-budget-generator-production.up.railway.app',
    entities: [148],
    period: '02/2026',
    email: 'JSirotkin@Centuryny.com',
    freshStart: false,
    expected: {
      // one file per entity per report type
      fileTypes: ['YSL_Annual_Budget', 'Adhoc_AMP', 'APAging'],
      minSize: 1024,  // any file under 1KB is suspicious
    },
    maxWaitSec: 120,
  };
  // ╚══════════════════════════════════════╝

  const out = (level, msg) => console.log(`[QA ${level}] ${msg}`);
  const bar = () => console.log('─'.repeat(60));
  const downloads = [];
  const startTime = Date.now();

  bar(); out('RUN', `Starting QA test — entities: ${CFG.entities.join(',')}`); bar();

  // ── Step 1: Install download interceptor ────────────────────────────────
  out('SETUP', 'Installing download interceptor...');
  const origCreate = document.createElement.bind(document);
  document.createElement = function(tag) {
    const el = origCreate(tag);
    if (String(tag).toLowerCase() === 'a') {
      const origClick = el.click.bind(el);
      el.click = function() {
        if (el.download) {
          downloads.push({
            name: el.download,
            href: el.href ? el.href.slice(0, 60) : '',
            t: Math.round((Date.now() - startTime) / 100) / 10,
          });
        }
        return origClick();
      };
    }
    return el;
  };
  out('SETUP', 'Interceptor installed.');

  // ── Step 2: Fetch combined script ───────────────────────────────────────
  out('FETCH', `Requesting combined script from ${CFG.railway}...`);
  let scriptText;
  try {
    const resp = await fetch(`${CFG.railway}/api/generate-script`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        entities: CFG.entities,
        email: CFG.email,
        period: CFG.period,
        fresh_start: CFG.freshStart,
      }),
    });
    const data = await resp.json();
    scriptText = data.script || data.combined || data;
    if (typeof scriptText !== 'string') throw new Error('No script in response');
    out('FETCH', `Got script: ${(scriptText.length/1024).toFixed(1)} KB`);
  } catch (e) {
    out('FAIL', `Could not fetch combined script: ${e.message}`);
    return { pass: false, stage: 'fetch', error: e.message };
  }

  // ── Step 3: Execute combined script ─────────────────────────────────────
  out('EXEC', 'Running combined script...');
  let scriptResult = null, scriptError = null;
  try {
    scriptResult = await (new Function(`return (async () => { ${scriptText} })()`))();
  } catch (e) {
    scriptError = e.message;
    out('EXEC', `Script threw: ${e.message}`);
  }

  // Restore createElement
  document.createElement = origCreate;

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  out('EXEC', `Script finished in ${elapsed}s`);

  // ── Step 4: Query server QA endpoint ────────────────────────────────────
  out('VERIFY', 'Querying /api/qa/verify...');
  let qa;
  try {
    const resp = await fetch(
      `${CFG.railway}/api/qa/verify?entity=${CFG.entities[0]}`
    );
    qa = await resp.json();
  } catch (e) {
    out('FAIL', `QA endpoint error: ${e.message}`);
    return { pass: false, stage: 'verify', error: e.message };
  }

  // ── Step 5: Assertions ──────────────────────────────────────────────────
  bar(); out('REPORT', 'Results'); bar();

  const checks = [];
  const check = (name, ok, detail) => {
    checks.push({ name, ok, detail });
    console.log(`  ${ok ? '✓' : '✗'} ${name}${detail ? ' — ' + detail : ''}`);
  };

  console.log('\nDownloads observed:');
  downloads.forEach(d => console.log(`  • ${d.name} @${d.t}s`));
  console.log(`  Total: ${downloads.length} files`);

  console.log('\nServer archive:');
  if (qa.archive && qa.archive.exists) {
    qa.archive.files.forEach(f =>
      console.log(`  • ${f.name} — ${(f.size/1024).toFixed(1)} KB`)
    );
  } else {
    console.log('  (empty)');
  }

  console.log('\nServer DB state:');
  if (qa.db && qa.db.budget_id) {
    console.log(`  budget_id: ${qa.db.budget_id}`);
    console.log(`  lines: ${qa.db.lines}`);
    console.log(`  lines_with_unpaid: ${qa.db.lines_with_unpaid}`);
    console.log(`  unpaid_bills_total: $${qa.db.unpaid_bills_total.toFixed(2)}`);
    console.log(`  ytd_actual_total: $${qa.db.ytd_actual_total.toFixed(2)}`);
  } else {
    console.log(`  ${JSON.stringify(qa.db)}`);
  }

  console.log('\nChecks:');
  check('script completed without error', !scriptError, scriptError || '');

  const expectedDownloadCount = CFG.entities.length * CFG.expected.fileTypes.length;
  check(
    `downloaded ${expectedDownloadCount} files`,
    downloads.length === expectedDownloadCount,
    `got ${downloads.length}`
  );

  for (const ent of CFG.entities) {
    for (const ft of CFG.expected.fileTypes) {
      const found = downloads.find(d => d.name.includes(ft) && d.name.includes(String(ent)));
      check(`${ft}_${ent} downloaded`, !!found, found ? found.name : 'missing');
    }
  }

  // No duplicate filenames
  const nameSet = new Set(downloads.map(d => d.name));
  check('no duplicate downloads', nameSet.size === downloads.length,
        `${downloads.length - nameSet.size} dupes`);

  // No unexpected filenames (like Adhoc_AMP_433)
  const unexpected = downloads.filter(d => {
    return !CFG.entities.some(e => d.name.includes(String(e)));
  });
  check('no unexpected entity codes in filenames', unexpected.length === 0,
        unexpected.map(d => d.name).join(',') || '');

  // Server archive count matches
  check('server archived all files',
        qa.archive && qa.archive.count === expectedDownloadCount,
        `archived ${qa.archive?.count || 0}`);

  // Unpaid bills populated
  check('unpaid bills applied (lines_with_unpaid > 0)',
        qa.db?.lines_with_unpaid > 0,
        `${qa.db?.lines_with_unpaid || 0} lines`);

  // YTD actuals populated (proves YSL processed)
  check('YTD actuals populated (YSL processed)',
        qa.db?.ytd_actual_total !== 0,
        `$${(qa.db?.ytd_actual_total || 0).toFixed(2)}`);

  const passed = checks.filter(c => c.ok).length;
  const failed = checks.filter(c => !c.ok).length;

  bar();
  const allPass = failed === 0;
  console.log(`%c${allPass ? '✅ PASS' : '❌ FAIL'} — ${passed}/${checks.length} checks passed`,
              `color:${allPass ? 'green' : 'red'};font-weight:bold;font-size:14px;`);
  bar();

  return { pass: allPass, checks, downloads, qa, elapsed: parseFloat(elapsed) };
})();
