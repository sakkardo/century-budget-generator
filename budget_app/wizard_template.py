WIZARD_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Budget Wizard - Century Management</title>
<style>
::-webkit-scrollbar { width: 12px; height: 12px; -webkit-appearance: none; }
::-webkit-scrollbar-track { background: #f1f5f9; }
::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 6px; border: 2px solid #f1f5f9; }
::-webkit-scrollbar-thumb:hover { background: #94a3b8; }
::-webkit-scrollbar-corner { background: #f1f5f9; }
* { scrollbar-width: thin; scrollbar-color: #cbd5e1 #f1f5f9; }

@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap');

:root {
  --blue: #5a4a3f;
  --blue-light: #f5efe7;
  --amber: #f59e0b;
  --green: #057a55;
  --green-light: #def7ec;
  --red: #e02424;
  --red-light: #fee2e2;
  --gray-50: #f4f1eb;
  --gray-100: #ede9e1;
  --gray-200: #e5e0d5;
  --gray-300: #d5cfc5;
  --gray-500: #8a7e72;
  --gray-700: #4a4039;
  --gray-900: #1a1714;
}

* { box-sizing: border-box; margin: 0; padding: 0; }

body {
  font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  background: var(--gray-50);
  color: var(--gray-900);
  line-height: 1.5;
}

/* Header Navigation */
header {
  background: linear-gradient(135deg, #2c2825 0%, #3d322a 100%);
  color: white;
  padding: 20px 28px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  box-shadow: 0 2px 8px rgba(0,0,0,0.1);
}

.header-left {
  display: flex;
  align-items: center;
  gap: 20px;
}

.header-logo {
  font-size: 16px;
  font-weight: 700;
  letter-spacing: 0.5px;
}

.header-nav {
  display: flex;
  gap: 24px;
  align-items: center;
}

.nav-item {
  font-size: 13px;
  font-weight: 500;
  text-decoration: none;
  color: rgba(255,255,255,0.7);
  transition: color 0.2s;
  cursor: pointer;
}

.nav-item:hover { color: white; }
.nav-item.active { color: white; font-weight: 600; }
.nav-item.locked { color: rgba(255,255,255,0.4); cursor: not-allowed; }

.header-right {
  display: flex;
  gap: 12px;
}

.btn-admin-bypass {
  background: var(--amber);
  color: white;
  border: none;
  padding: 8px 16px;
  border-radius: 6px;
  font-size: 12px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s;
}

.btn-admin-bypass:hover { background: #d97706; }

/* Main Layout */
.wizard-container {
  display: flex;
  min-height: calc(100vh - 64px);
}

/* Left Sidebar */
.wizard-rail {
  width: 200px;
  background: white;
  border-right: 1px solid var(--gray-200);
  padding: 32px 20px;
  overflow-y: auto;
}

.rail-entity {
  margin-bottom: 28px;
}

.rail-entity-name {
  font-size: 13px;
  font-weight: 700;
  color: var(--gray-700);
  text-transform: uppercase;
  letter-spacing: 0.5px;
  margin-bottom: 16px;
}

.rail-steps {
  list-style: none;
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.rail-phase {
  font-size: 11px;
  font-weight: 700;
  color: var(--gray-500);
  text-transform: uppercase;
  letter-spacing: 0.5px;
  margin-top: 20px;
  margin-bottom: 12px;
}

.rail-step {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 10px 12px;
  border-radius: 6px;
  cursor: pointer;
  transition: all 0.2s;
  font-size: 13px;
  font-weight: 500;
}

.rail-step:hover { background: var(--blue-light); }
.rail-step.active { background: var(--blue-light); color: var(--blue); }
.rail-step.locked { opacity: 0.4; cursor: not-allowed; }

.step-circle {
  width: 24px;
  height: 24px;
  border-radius: 50%;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 11px;
  font-weight: 700;
  flex-shrink: 0;
  background: var(--gray-200);
  color: var(--gray-700);
}

.step-circle.done {
  background: var(--green);
  color: white;
}

.step-circle.active {
  background: var(--blue);
  color: white;
}

.step-circle.locked {
  background: var(--gray-100);
  color: var(--gray-500);
}

/* Right Content Area */
.wizard-content {
  flex: 1;
  padding: 48px;
  overflow-y: auto;
}

.step-header {
  margin-bottom: 32px;
}

.step-badge {
  display: inline-block;
  background: var(--blue-light);
  color: var(--blue);
  font-size: 11px;
  font-weight: 700;
  padding: 6px 12px;
  border-radius: 4px;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  margin-bottom: 12px;
}

.step-title {
  font-size: 32px;
  font-weight: 700;
  color: var(--gray-900);
  margin-bottom: 8px;
}

.step-description {
  font-size: 15px;
  color: var(--gray-700);
  margin-bottom: 20px;
}

.prompt-banner {
  background: #eff6ff;
  border-left: 4px solid var(--blue);
  padding: 16px;
  border-radius: 6px;
  font-size: 14px;
  color: #1e40af;
  margin-bottom: 32px;
}

/* Step Content */
.step-content {
  display: none;
}

.step-content.active {
  display: block;
}

/* FA Selector */
.fa-selector-wrapper {
  background: white;
  border: 1px solid var(--gray-200);
  border-radius: 10px;
  padding: 18px 22px;
  margin-bottom: 20px;
  box-shadow: 0 1px 3px rgba(0,0,0,0.04);
}
.fa-selector-label {
  font-size: 11px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--gray-500);
  margin-bottom: 8px;
}
.fa-selector-row {
  display: flex;
  align-items: center;
  gap: 14px;
}
.fa-select {
  flex: 1;
  max-width: 360px;
  font-family: inherit;
  font-size: 15px;
  font-weight: 600;
  color: var(--gray-900);
  padding: 10px 14px;
  border: 2px solid var(--gray-200);
  border-radius: 8px;
  background: var(--gray-50);
  cursor: pointer;
  transition: border-color 0.15s;
  appearance: none;
  background-image: url("data:image/svg+xml,%3Csvg xmlns=\'http://www.w3.org/2000/svg\' width=\'12\' height=\'8\' viewBox=\'0 0 12 8\'%3E%3Cpath d=\'M1 1l5 5 5-5\' stroke=\'%238a7e72\' stroke-width=\'2\' fill=\'none\'/%3E%3C/svg%3E");
  background-repeat: no-repeat;
  background-position: right 12px center;
  padding-right: 36px;
}
.fa-select:focus {
  outline: none;
  border-color: var(--amber);
}
.fa-entity-count {
  font-size: 13px;
  color: var(--gray-500);
  white-space: nowrap;
}

/* Entity Search */
.entity-search-bar {
  position: relative;
  margin-bottom: 20px;
}
.search-icon {
  position: absolute;
  left: 14px;
  top: 50%;
  transform: translateY(-50%);
  color: var(--gray-500);
  pointer-events: none;
}
.entity-search-input {
  width: 100%;
  font-family: inherit;
  font-size: 14px;
  padding: 11px 14px 11px 40px;
  border: 2px solid var(--gray-200);
  border-radius: 8px;
  background: white;
  color: var(--gray-900);
  transition: border-color 0.15s;
}
.entity-search-input:focus {
  outline: none;
  border-color: var(--amber);
}
.entity-search-input::placeholder {
  color: var(--gray-300);
}

/* Step 1: Entity Grid */
.entity-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
  gap: 20px;
  margin-bottom: 32px;
}

.entity-card {
  background: white;
  border: 2px solid var(--gray-200);
  border-radius: 12px;
  padding: 24px;
  cursor: pointer;
  transition: all 0.2s;
}

.entity-card:hover {
  border-color: var(--blue);
  box-shadow: 0 4px 12px rgba(0,0,0,0.1);
}

.entity-card.selected {
  border-color: var(--blue);
  background: var(--blue-light);
}

.entity-status {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  font-weight: 600;
  margin-bottom: 12px;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.status-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
}

.status-fresh { background: var(--gray-500); }
.status-in-progress { background: var(--amber); }
.status-has-edits { background: var(--blue); }
.status-complete { background: var(--green); }

.entity-name {
  font-size: 16px;
  font-weight: 700;
  color: var(--gray-900);
  margin-bottom: 8px;
}

.entity-address {
  font-size: 13px;
  color: var(--gray-500);
  line-height: 1.4;
}

/* Step 2: Upload Checklist */
.upload-section {
  background: white;
  border: 1px solid var(--gray-200);
  border-radius: 12px;
  padding: 28px;
  margin-bottom: 32px;
}

.checklist {
  list-style: none;
  margin-bottom: 32px;
}

.checklist-item {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 16px 12px;
  border-bottom: 1px solid var(--gray-100);
  border-radius: 6px;
  transition: background 0.15s;
}
.checklist-item:hover {
  background: var(--gray-50);
}
.checklist-item:last-child {
  border-bottom: none;
}
.checklist-link {
  margin-left: auto;
  color: var(--gray-300);
  font-size: 18px;
  font-weight: 600;
  transition: color 0.15s;
}
.checklist-item:hover .checklist-link {
  color: var(--amber);
}

.checklist-icon {
  flex-shrink: 0;
  width: 24px;
  height: 24px;
  border-radius: 4px;
  background: var(--green-light);
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 14px;
  color: var(--green);
}

.checklist-item.pending .checklist-icon {
  background: var(--gray-100);
  color: var(--gray-500);
  content: '';
}

.checklist-content {
  flex: 1;
}

.checklist-name {
  font-size: 14px;
  font-weight: 600;
  color: var(--gray-900);
  margin-bottom: 4px;
}

.checklist-required {
  font-size: 11px;
  color: var(--red);
  font-weight: 600;
  text-transform: uppercase;
  margin-left: 6px;
}
.checklist-badge {
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  margin-left: 8px;
  padding: 2px 8px;
  border-radius: 4px;
  letter-spacing: 0.03em;
}
.badge-uploaded { background: #e0f2fe; color: #0369a1; }
.badge-extracted { background: #fef3c7; color: #92400e; }
.badge-mapped { background: #ede9fe; color: #6d28d9; }
.badge-confirmed { background: var(--green-light); color: var(--green); }

.checklist-item.confirmed .checklist-icon {
  background: var(--green);
  color: white;
  border-color: var(--green);
}
.checklist-item.audit-uploaded .checklist-icon {
  background: #0ea5e9;
  color: white;
  border-color: #0ea5e9;
}
.checklist-item.audit-extracted .checklist-icon {
  background: #f59e0b;
  color: white;
  border-color: #f59e0b;
}
.checklist-item.audit-mapped .checklist-icon {
  background: #8b5cf6;
  color: white;
  border-color: #8b5cf6;
}

.checklist-description {
  font-size: 13px;
  color: var(--gray-500);
}

.upload-dropzone {
  border: 2px dashed var(--gray-300);
  border-radius: 8px;
  padding: 32px;
  text-align: center;
  background: var(--gray-50);
  cursor: pointer;
  transition: all 0.2s;
}

.upload-dropzone:hover {
  border-color: var(--blue);
  background: var(--blue-light);
}

.upload-dropzone-icon {
  font-size: 32px;
  margin-bottom: 12px;
}

.upload-dropzone-text {
  font-size: 14px;
  font-weight: 600;
  color: var(--gray-900);
  margin-bottom: 4px;
}

.upload-dropzone-hint {
  font-size: 12px;
  color: var(--gray-500);
}

#fileInput {
  display: none;
}

/* Step 3: Read-only Assumptions */
.assumptions-grid {
  display: grid;
  gap: 16px;
  margin-bottom: 32px;
}

.assumption-row {
  background: white;
  border: 1px solid var(--gray-200);
  border-radius: 8px;
  padding: 16px;
  display: grid;
  grid-template-columns: 200px 1fr;
  gap: 20px;
  align-items: center;
}

.assumption-label {
  font-size: 14px;
  font-weight: 600;
  color: var(--gray-900);
}

.assumption-value {
  font-size: 14px;
  color: var(--gray-500);
}

.version-chip {
  display: inline-block;
  background: var(--gray-100);
  color: var(--gray-700);
  font-size: 12px;
  font-weight: 600;
  padding: 6px 12px;
  border-radius: 4px;
  margin-bottom: 20px;
}

/* Step 4: Editable Assumptions */
.building-assumptions-form {
  background: white;
  border: 1px solid var(--gray-200);
  border-radius: 12px;
  padding: 28px;
  margin-bottom: 32px;
}

.form-row {
  display: grid;
  grid-template-columns: 200px 150px 1fr;
  gap: 20px;
  align-items: center;
  padding: 16px 0;
  border-bottom: 1px solid var(--gray-100);
}

.form-row:last-child {
  border-bottom: none;
}

.form-label {
  font-size: 14px;
  font-weight: 600;
  color: var(--gray-900);
}

.form-default {
  font-size: 13px;
  color: var(--gray-500);
  font-weight: 500;
}

.form-input {
  padding: 10px 12px;
  border: 1px solid var(--gray-300);
  border-radius: 6px;
  font-size: 14px;
  font-family: 'Plus Jakarta Sans', sans-serif;
}

.form-input:focus {
  outline: none;
  border-color: var(--blue);
  box-shadow: 0 0 0 3px rgba(90, 74, 63, 0.1);
}

/* Step 5: Preview Table */
.preview-table {
  width: 100%;
  border-collapse: collapse;
  background: white;
  border: 1px solid var(--gray-200);
  border-radius: 8px;
  overflow: hidden;
  margin-bottom: 32px;
}

.preview-table th {
  background: var(--gray-50);
  padding: 14px;
  text-align: left;
  font-size: 12px;
  font-weight: 700;
  color: var(--gray-700);
  border-bottom: 1px solid var(--gray-200);
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.preview-table td {
  padding: 14px;
  border-bottom: 1px solid var(--gray-100);
  font-size: 13px;
}

.preview-table tr:last-child td {
  border-bottom: none;
}

.preview-category {
  font-weight: 600;
  color: var(--gray-900);
}

.preview-value {
  text-align: right;
  font-family: 'Monaco', 'Courier New', monospace;
  font-size: 12px;
}

.preview-increase {
  color: var(--red);
  font-weight: 600;
}

.preview-decrease {
  color: var(--green);
  font-weight: 600;
}

.preview-neutral {
  color: var(--gray-500);
}

/* Step 6: Success */
.success-card {
  background: var(--green-light);
  border: 1px solid var(--green);
  border-radius: 12px;
  padding: 32px;
  text-align: center;
  margin-bottom: 32px;
}

.success-icon {
  font-size: 48px;
  margin-bottom: 16px;
}

.success-message {
  font-size: 18px;
  font-weight: 700;
  color: var(--green);
  margin-bottom: 8px;
}

.success-details {
  font-size: 14px;
  color: #065f46;
  line-height: 1.6;
}

/* Action Buttons */
.action-buttons {
  display: flex;
  gap: 12px;
  justify-content: flex-end;
  margin-top: 32px;
}

.btn {
  padding: 12px 24px;
  border-radius: 6px;
  font-size: 14px;
  font-weight: 600;
  cursor: pointer;
  border: none;
  transition: all 0.2s;
  font-family: 'Plus Jakarta Sans', sans-serif;
}

.btn-primary {
  background: var(--blue);
  color: white;
}

.btn-primary:hover { background: #4a3a2f; }
.btn-primary:disabled { opacity: 0.6; cursor: not-allowed; }

.btn-secondary {
  background: var(--gray-100);
  color: var(--gray-900);
  border: 1px solid var(--gray-300);
}

.btn-secondary:hover { background: var(--gray-200); }

.btn-full {
  width: 100%;
  text-align: center;
}

/* Modal Overlay */
.modal-overlay {
  display: none;
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0,0,0,0.4);
  z-index: 1000;
}

.modal-overlay.active {
  display: flex;
  align-items: center;
  justify-content: center;
}

.modal {
  background: white;
  border-radius: 12px;
  padding: 32px;
  max-width: 480px;
  box-shadow: 0 20px 25px -5px rgba(0,0,0,0.1);
}

.modal-title {
  font-size: 18px;
  font-weight: 700;
  color: var(--red);
  margin-bottom: 16px;
}

.modal-content {
  font-size: 14px;
  color: var(--gray-700);
  line-height: 1.6;
  margin-bottom: 28px;
}

.modal-buttons {
  display: flex;
  gap: 12px;
  justify-content: flex-end;
}

/* Responsive */
@media (max-width: 1024px) {
  .wizard-container {
    flex-direction: column;
  }

  .wizard-rail {
    width: 100%;
    border-right: none;
    border-bottom: 1px solid var(--gray-200);
    padding: 20px;
    display: flex;
    gap: 32px;
    overflow-x: auto;
  }

  .rail-entity {
    flex-shrink: 0;
  }

  .wizard-content {
    padding: 32px 20px;
  }

  .entity-grid {
    grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
  }

  .assumption-row {
    grid-template-columns: 1fr;
  }

  .form-row {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 640px) {
  header {
    flex-direction: column;
    gap: 16px;
  }

  .header-left {
    width: 100%;
  }

  .header-right {
    width: 100%;
  }

  .wizard-content {
    padding: 24px 16px;
  }

  .step-title {
    font-size: 24px;
  }

  .entity-grid {
    grid-template-columns: 1fr;
  }

  .action-buttons {
    flex-direction: column;
  }

  .btn {
    width: 100%;
  }

  .preview-table {
    font-size: 12px;
  }

  .preview-table th,
  .preview-table td {
    padding: 10px;
  }
}
</style>
</head>
<body>

<!-- Header (FA dir 2026-05-22: rebuilt nav to match the rest of the app —
     "Dashboard" was hardcoded `locked` with an alert popup that did nothing,
     "Home" routed to /dashboard, and the logo had a "← " arrow that implied
     back-direction even though it pointed forward. Now matches the dashboard,
     PM portal, and audited-financials nav.) -->
<header>
  <div class="header-left">
    <a href="/" class="header-logo" style="text-decoration:none;color:white;">Century Budget</a>
    <nav class="header-nav">
      <a href="/" class="nav-item">Home</a>
      <a class="nav-item active" onclick="showStep(1)" style="cursor:pointer;">Wizard</a>
      <a href="/dashboard" class="nav-item">FA Dashboard</a>
      <a href="/pm" class="nav-item">PM Portal</a>
      <a href="/audited-financials" class="nav-item">Audited Financials</a>
    </nav>
  </div>
  <div class="header-right">
    <a href="/admin/login?next=/wizard" class="nav-item" style="font-size:12px;color:rgba(255,255,255,0.6);text-decoration:none;margin-right:14px;" title="Sign in with ADMIN_KEY to access /api/admin/* endpoints">🔑 Admin</a>
    <button class="btn-admin-bypass" onclick="showAdminBypassModal()">⚡ Admin Upload</button>
  </div>
</header>

<!-- Wizard Container -->
<div class="wizard-container">
  <!-- Left Sidebar -->
  <div class="wizard-rail">
    <div class="rail-entity">
      <div class="rail-entity-name" id="railEntityName">No Entity Selected</div>
      <ul class="rail-steps">
        <li class="rail-phase">Setup</li>
        <li class="rail-step" onclick="showStep(1)" data-step="1">
          <div class="step-circle" data-step="1">1</div>
          <span>Select Entity</span>
        </li>
        <li class="rail-step" onclick="showStep(2)" data-step="2">
          <div class="step-circle" data-step="2">2</div>
          <span>Foundation</span>
        </li>

        <li class="rail-phase">Assumptions</li>
        <li class="rail-step" onclick="showStep(3)" data-step="3">
          <div class="step-circle" data-step="3">3</div>
          <span>Set Assumptions</span>
        </li>

        <li class="rail-phase">Generate</li>
        <li class="rail-step" onclick="showStep(4)" data-step="4">
          <div class="step-circle" data-step="4">4</div>
          <span>Preview & Generate</span>
        </li>
        <li class="rail-step" onclick="showStep(5)" data-step="5">
          <div class="step-circle" data-step="5">5</div>
          <span>Complete</span>
        </li>
      </ul>
    </div>
  </div>

  <!-- Right Content -->
  <div class="wizard-content">

    <!-- Step 1: Select Entity -->
    <div class="step-content active" data-step="1">
      <div class="step-header">
        <div class="step-badge">Step 1 of 5</div>
        <h1 class="step-title">Select Entity</h1>
        <p class="step-description">Choose which building to create a budget for</p>
      </div>

      <!-- Monday.com sync error banner (hidden unless sync failed) -->
      <div id="mondaySyncBanner" style="display:none; padding:10px 14px; margin-bottom:12px; background:#fffbe6; border:1px solid #ffe58f; border-radius:6px; color:#874d00; font-size:13px;"></div>

      <!-- FA dir 2026-05-24 redesign: compact top bar replaces the old
           "FINANCIAL ANALYST" label + "All Entities" dropdown + verbose
           helper banner. Title shows live portfolio summary; sync status
           sits inline on the right. The FA filter is moved into the
           filter band below (chips + FA + search on one row). -->
      <div class="wizard-toptitle" style="display:flex; align-items:center; gap:14px; margin-bottom:14px;">
        <div style="flex:1;">
          <div style="font-size:14px; font-weight:700; color:var(--gray-900);">
            <span>Wizard</span>
            <span id="portfolioSummary" style="font-size:12px; font-weight:500; color:var(--gray-600); margin-left:8px;"></span>
          </div>
        </div>
        <span class="monday-sync-status" id="mondaySyncStatus" style="font-size:11px; color:#15803d; font-weight:600;">Checking sync…</span>
        <button type="button" id="mondayRefreshBtn" onclick="refreshFromMonday()" style="font-size:11px; padding:5px 11px; border:1px solid var(--gray-300); background:#fff; border-radius:6px; cursor:pointer; font-weight:600;" title="Force a fresh pull from Monday.com Building Master List">↻ Refresh</button>
      </div>

      <!-- Lifecycle stage filter chips -->
      <!-- FA dir 2026-05-23: hero callout + readiness-tier filter chips.
           Surfaces the FA's actionable queue at first glance instead of
           making them scan 147 rows. -->
      <div id="readinessHero" style="display:none; margin-bottom:14px;"></div>

      <!-- One filter band: readiness chips + FA filter + search. FA dir
           2026-05-24: collapsed from 3 separate rows to 1 to recover
           vertical space and put the table closer to the fold. -->
      <div class="wizard-filter-band" style="display:flex; align-items:center; gap:10px; flex-wrap:wrap; background:white; border:1px solid var(--gray-200); border-radius:10px; padding:8px 12px; margin-bottom:10px;">
        <div class="stage-filter-row" id="stageFilterRow" style="display:flex; gap:6px; flex-wrap:wrap; align-items:center;">
          <!-- Populated by JavaScript -->
        </div>
        <span style="width:1px; height:22px; background:var(--gray-200);"></span>
        <span style="font-size:11px; color:var(--gray-700); display:inline-flex; align-items:center; gap:6px;">
          FA:
          <select id="faSelector" onchange="filterByFA()" style="padding:5px 8px; border:1px solid var(--gray-300); border-radius:6px; background:white; font-size:11px; font-family:inherit; color:var(--gray-900);">
            <option value="">Any</option>
          </select>
        </span>
        <span style="width:1px; height:22px; background:var(--gray-200);"></span>
        <div class="entity-search-bar" style="flex:1; min-width:160px; background:var(--gray-50); border:1px solid transparent; border-radius:7px; padding:6px 10px; display:flex; align-items:center; gap:6px;">
          <svg class="search-icon" viewBox="0 0 20 20" fill="currentColor" width="14" height="14" style="opacity:0.5;"><path fill-rule="evenodd" d="M8 4a4 4 0 100 8 4 4 0 000-8zM2 8a6 6 0 1110.89 3.476l4.817 4.817a1 1 0 01-1.414 1.414l-4.816-4.816A6 6 0 012 8z" clip-rule="evenodd"/></svg>
          <input type="text" id="entitySearch" class="entity-search-input" placeholder="Search name or entity code" oninput="renderEntityGrid()" style="flex:1; border:none; outline:none; font-size:11px; font-family:inherit; background:transparent; color:var(--gray-900);">
        </div>
      </div>

      <!-- Source legend strip: decodes the B / E / Y / A / Au letters
           used in the Data-status / Missing column. Cheap to render,
           saves new FAs from having to guess. FA dir 2026-05-24. -->
      <div class="wizard-legend" style="margin-bottom:12px; padding:7px 14px; background:#faf7f4; border:1px solid var(--gray-200); border-radius:8px; font-size:10px; color:var(--gray-600); display:flex; align-items:center; gap:14px; flex-wrap:wrap;">
        <span style="font-size:9px; font-weight:700; color:var(--gray-500); text-transform:uppercase; letter-spacing:0.7px;">Source legend</span>
        <span style="display:inline-flex; align-items:center; gap:5px;"><span style="display:inline-flex; align-items:center; justify-content:center; min-width:18px; height:16px; padding:0 4px; border-radius:3px; font-size:9px; font-weight:700; background:#fef3c7; color:#92400e; border:1px solid #fcd34d;">B</span><span style="font-size:10px; color:var(--gray-700); font-weight:500;">2026 Approved Budget</span></span>
        <span style="display:inline-flex; align-items:center; gap:5px;"><span style="display:inline-flex; align-items:center; justify-content:center; min-width:18px; height:16px; padding:0 4px; border-radius:3px; font-size:9px; font-weight:700; background:#fef3c7; color:#92400e; border:1px solid #fcd34d;">E</span><span style="font-size:10px; color:var(--gray-700); font-weight:500;">Expense Distribution</span></span>
        <span style="display:inline-flex; align-items:center; gap:5px;"><span style="display:inline-flex; align-items:center; justify-content:center; min-width:18px; height:16px; padding:0 4px; border-radius:3px; font-size:9px; font-weight:700; background:#fef3c7; color:#92400e; border:1px solid #fcd34d;">Y</span><span style="font-size:10px; color:var(--gray-700); font-weight:500;">YSL (Yardi)</span></span>
        <span style="display:inline-flex; align-items:center; gap:5px;"><span style="display:inline-flex; align-items:center; justify-content:center; min-width:18px; height:16px; padding:0 4px; border-radius:3px; font-size:9px; font-weight:700; background:#fef3c7; color:#92400e; border:1px solid #fcd34d;">A</span><span style="font-size:10px; color:var(--gray-700); font-weight:500;">AP Aging</span></span>
        <span style="display:inline-flex; align-items:center; gap:5px;"><span style="display:inline-flex; align-items:center; justify-content:center; min-width:18px; height:16px; padding:0 4px; border-radius:3px; font-size:9px; font-weight:700; background:#fef3c7; color:#92400e; border:1px solid #fcd34d;">Au</span><span style="font-size:10px; color:var(--gray-700); font-weight:500;">2025 Audit PDF</span></span>
      </div>

      <div class="entity-table-wrap" style="background:white; border:1px solid var(--gray-200); border-radius:10px; overflow:hidden; box-shadow:0 1px 3px rgba(0,0,0,0.04); margin-bottom:32px;">
        <table class="entity-table" id="entityTable" style="width:100%; border-collapse:collapse; table-layout:fixed;">
          <colgroup>
            <col style="width:30%"/>
            <col style="width:6%"/>
            <col style="width:14%"/>
            <col style="width:22%"/>
            <col style="width:10%"/>
            <col style="width:18%"/>
          </colgroup>
          <thead>
            <tr style="background:var(--gray-50); border-bottom:1px solid var(--gray-200);">
              <th data-col="building_name" onclick="setEntitySort(\'building_name\')" style="padding:10px 14px; text-align:left; font-size:11px; font-weight:700; color:var(--gray-700); letter-spacing:0.04em; cursor:pointer; user-select:none;">Building <span class="sort-arrow" data-arrow="building_name" style="opacity:0.3;">&#9650;</span></th>
              <th data-col="entity_code" onclick="setEntitySort(\'entity_code\')" style="padding:10px 14px; text-align:left; font-size:11px; font-weight:700; color:var(--gray-700); letter-spacing:0.04em; cursor:pointer; user-select:none;">Entity <span class="sort-arrow" data-arrow="entity_code" style="opacity:0.3;">&#9650;</span></th>
              <th data-col="fa_name" onclick="setEntitySort(\'fa_name\')" style="padding:10px 14px; text-align:left; font-size:11px; font-weight:700; color:var(--gray-700); letter-spacing:0.04em; cursor:pointer; user-select:none;">FA <span class="sort-arrow" data-arrow="fa_name" style="opacity:0.3;">&#9650;</span></th>
              <th style="padding:10px 14px; text-align:left; font-size:11px; font-weight:700; color:var(--gray-700); letter-spacing:0.04em;" title="2026 Approved Budget / Expense Distribution / YSL / AP Aging / 2025 Audit — see legend above">Data status</th>
              <th data-col="age" onclick="setEntitySort(\'age\')" style="padding:10px 14px; text-align:left; font-size:11px; font-weight:700; color:var(--gray-700); letter-spacing:0.04em; cursor:pointer; user-select:none;" title="Days since last activity on this building">Waiting <span class="sort-arrow" data-arrow="age" style="opacity:0.3;">&#9650;</span></th>
              <th style="padding:10px 14px; text-align:right; font-size:11px; font-weight:700; color:var(--gray-700); letter-spacing:0.04em;">Next step</th>
            </tr>
          </thead>
          <tbody id="entityTableBody">
            <!-- Populated by JavaScript -->
          </tbody>
        </table>
      </div>
      <!-- Hidden legacy container kept for backward-compat references; not used. -->
      <div class="entity-grid" id="entityGrid" style="display:none;"></div>
    </div>

    <!-- Step 2: Upload Sources -->
    <div class="step-content" data-step="2">
      <div class="step-header">
        <div class="step-badge">Step 2 of 5</div>
        <h1 class="step-title">Foundation</h1>
        <p class="step-description">Lock in the prior-year framework. The 2026 Approved Budget and 2025 Audit must both be confirmed before the rest of the budget process unlocks.</p>
      </div>
      <div class="prompt-banner">
        Step 2 collects the source files. Nothing is committed to your budget until you click "Build Budget" in Step 5. Files staged in SharePoint will be detected here — you confirm each one explicitly.
      </div>

      <!-- Foundation Panel (Phase E) -->
      <div id="foundationPanel" style="display:none; margin-bottom:16px;">
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:12px;">
          <div class="foundation-card" id="foundationApprovedCard" style="background:white; border:1px solid var(--gray-200); border-radius:10px; padding:18px 22px; box-shadow:0 1px 3px rgba(0,0,0,0.04);">
            <div style="font-size:11px; font-weight:700; letter-spacing:0.08em; color:var(--gray-500); text-transform:uppercase; margin-bottom:6px;">2026 Approved Budget</div>
            <div style="font-size:13px; font-weight:600; margin-bottom:12px; color:var(--gray-700, #374151);">Sets Col 1 (2024 Actual) + Col 6 + categories</div>
            <div id="foundationApprovedBody"><div style="color:var(--gray-500); font-size:13px;">Loading…</div></div>
          </div>
          <div class="foundation-card" id="foundationAuditCard" style="background:white; border:1px solid var(--gray-200); border-radius:10px; padding:18px 22px; box-shadow:0 1px 3px rgba(0,0,0,0.04);">
            <div style="font-size:11px; font-weight:700; letter-spacing:0.08em; color:var(--gray-500); text-transform:uppercase; margin-bottom:6px;">2025 Audited Financial</div>
            <div style="font-size:13px; font-weight:600; margin-bottom:12px; color:var(--gray-700, #374151);">Sets Col 2 (2025 Actual)</div>
            <div id="foundationAuditBody"><div style="color:var(--gray-500); font-size:13px;">Loading…</div></div>
          </div>
        </div>
        <div id="foundationStatusBanner" style="margin-top:12px;"></div>
      </div>

      <!-- SharePoint Sources Panel -->
      <div class="sp-sources-panel" id="spSourcesPanel" style="background:white; border:1px solid var(--gray-200); border-radius:10px; padding:18px 22px; margin-bottom:20px; box-shadow:0 1px 3px rgba(0,0,0,0.04); display:none;">
        <div style="display:flex; align-items:center; gap:12px; margin-bottom:14px;">
          <div style="font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:0.08em; color:var(--gray-500); flex:1;">Yardi Sources</div>
          <span id="spFolderInfo" style="font-size:12px; color:var(--gray-500);"></span>
          <button type="button" id="spRefreshBtn" onclick="loadSharepointSources()" style="font-size:12px; padding:5px 12px; border:1px solid #ddd; background:#fff; border-radius:4px; cursor:pointer;" title="Re-list files in this entity Supporting Documents folder">↻ Refresh from SP</button>
        </div>
        <div id="spSourcesBody" style="display:flex; flex-direction:column; gap:10px;">
          <div style="color:var(--gray-500); font-size:13px;">Loading...</div>
        </div>
      </div>

      <div class="upload-section" style="margin-top:8px;">
        <div style="font-size:11px; font-weight:700; letter-spacing:0.08em; color:var(--gray-500); text-transform:uppercase; margin-bottom:8px;">From Your Computer (Fallback)</div>
        <div class="upload-dropzone" onclick="document.getElementById('fileInput').click()" style="padding:14px 18px; min-height:auto;">
          <div style="display:flex; align-items:center; gap:14px; justify-content:center;">
            <div style="font-size:24px;">&#128193;</div>
            <div style="text-align:left;">
              <div style="font-weight:600; font-size:14px;">Drop a file or click to browse</div>
              <div style="font-size:12px; color:var(--gray-500); margin-top:2px;">We'll route it to SharePoint and refresh the panel above</div>
            </div>
          </div>
        </div>
        <div id="uploadStatus" style="font-size:12px; color:var(--gray-500); margin-top:8px; min-height:16px;"></div>
        <input type="file" id="fileInput" onchange="handleFileUpload(event)" style="display:none;">
      </div>
    </div>

    <!-- Step 3: Set Assumptions (CFO defaults pre-filled, FA can override) -->
    <div class="step-content" data-step="3">
      <div class="step-header">
        <div class="step-badge">Step 3 of 5</div>
        <h1 class="step-title">Set Assumptions</h1>
        <p class="step-description">Firm defaults are pre-filled. Override anything specific to this building.</p>
      </div>
      <div class="prompt-banner">
        Values save automatically as you type. Defaults come from the CFO Portfolio Defaults page. Anything you change here applies only to this building and lands in the Assumptions tab on the dashboard after Build.
      </div>
      <div id="wizardAssumptionsForm">
        <!-- Populated by JavaScript -->
      </div>
      <div id="wizardAssumpStatus" style="margin-top:12px; font-size:12px; color:var(--gray-500); height:16px;"></div>
    </div>

    <!-- Step 4: Preview & Generate -->
    <div class="step-content" data-step="4">
      <div class="step-header">
        <div class="step-badge">Step 4 of 5</div>
        <h1 class="step-title">Preview & Generate Budget</h1>
        <p class="step-description">Review the impact of assumptions before generating</p>
      </div>
      <div class="prompt-banner">
        Review the impact — red numbers show increases from assumptions. Go back to adjust if needed.
      </div>
      <table class="preview-table" id="previewTable">
        <thead>
          <tr>
            <th>Category</th>
            <th>Yardi Raw</th>
            <th>+ Assumptions</th>
            <th>Delta</th>
          </tr>
        </thead>
        <tbody id="previewTableBody">
          <!-- Populated by JavaScript -->
        </tbody>
      </table>
    </div>

    <!-- Step 5: Success -->
    <div class="step-content" data-step="5">
      <div class="step-header">
        <div class="step-badge">Step 5 of 5</div>
        <h1 class="step-title">Budget Complete</h1>
        <p class="step-description">You're ready to fine-tune your budget</p>
      </div>
      <div class="prompt-banner">
        You're ready! Budget generated with your assumptions. Open the dashboard to fine-tune individual lines.
      </div>
      <div class="success-card">
        <div class="success-icon">✓</div>
        <div class="success-message">Budget Generated Successfully</div>
        <div class="success-details" id="successDetails">
          Budget lines created with your assumptions. Your snapshot is saved.
        </div>
      </div>
    </div>

    <!-- Action Buttons -->
    <div class="action-buttons" id="actionButtons">
      <!-- Populated by JavaScript based on step -->
    </div>

  </div>
</div>

<!-- Admin Bypass Modal -->
<div class="modal-overlay" id="adminBypassModal">
  <div class="modal">
    <div class="modal-title">⚠️ Admin Bypass Upload</div>
    <div class="modal-content">
      This upload bypasses the wizard. FA edits may be affected. Budget lines will be overwritten with new data.
      <br><br>
      Are you sure you want to continue?
    </div>
    <div class="modal-buttons">
      <button class="btn btn-secondary" onclick="hideAdminBypassModal()">Cancel</button>
      <button class="btn btn-primary" onclick="confirmAdminBypass()">I understand, continue</button>
    </div>
  </div>
</div>

<script>
// State
let currentStep = 1;
let highestStep = 1;
let selectedEntity = null;
const BUDGET_YEAR = {{ budget_year }};   // server-injected; YSL actuals year = BUDGET_YEAR - 1
let budgets = [];
let portfolio = {};
let building = {};
let sources = {};
let faUsers = [];
let faAssignments = [];
let selectedFA = '';
let mondayStatus = null;

// Parse JSON from template variables
function initializeData() {
  try {
    budgets = JSON.parse({{ budgets_json | tojson }});
    portfolio = JSON.parse({{ portfolio_json | tojson }});
    building = JSON.parse({{ building_json | tojson }});
    sources = JSON.parse({{ sources_json | tojson }});
    faUsers = JSON.parse({{ fa_users_json | tojson }});
    faAssignments = JSON.parse({{ assignments_json | tojson }});
    mondayStatus = JSON.parse({{ monday_status_json | tojson }});
    selectedEntity = '{{ entity_code }}';
  } catch (e) {
    console.error('Error parsing template data:', e);
    budgets = [];
  }
  // Initialize FA selector (now inline in the filter band — FA dir 2026-05-24).
  // The old #faSelectorWrapper was removed; only the inline <select> remains.
  if (faUsers.length > 0) {
    const sel = document.getElementById('faSelector');
    if (sel) {
      // Sort PMs/FAs alphabetically for usability across 40+ analysts.
      const sorted = faUsers.slice().sort(function (a, b) {
        return (a.name || '').localeCompare(b.name || '');
      });
      sorted.forEach(function (u) {
        const opt = document.createElement('option');
        opt.value = u.id;
        opt.textContent = u.name;
        sel.appendChild(opt);
      });
    }
  }
  renderMondaySyncStatus();
  maybeAutoSyncMonday();
}

// If the cached Monday sync is missing or older than the TTL, kick off a
// silent refresh in the background and reload on success. Failures fall
// through quietly — the banner already explains the state.
function maybeAutoSyncMonday() {
  if (!mondayStatus) return;
  const ttlMin = mondayStatus.stale_minutes || 10;
  let isStale = true;
  if (mondayStatus.last_synced_at) {
    const iso = mondayStatus.last_synced_at;
    const dt = new Date(iso.endsWith('Z') ? iso : (iso + 'Z'));
    const minsAgo = Math.max(0, (Date.now() - dt.getTime()) / 60000);
    isStale = minsAgo > ttlMin;
  }
  if (!isStale) return;
  // Avoid re-firing on rapid back-to-back loads
  try {
    const last = parseInt(sessionStorage.getItem('mondayAutoSyncFiredAt') || '0', 10);
    if (last && (Date.now() - last) < 60000) return;
    sessionStorage.setItem('mondayAutoSyncFiredAt', String(Date.now()));
  } catch (e) {}
  fetch('/api/sync-monday-now', { method: 'POST' })
    .then(r => r.json().then(data => ({ ok: r.ok, data: data })))
    .then(res => {
      if (res.ok && res.data && res.data.ok) {
        location.reload();
      }
    })
    .catch(() => { /* silent — user can still use the page */ });
}

// Render Monday.com sync timestamp + error banner
function renderMondaySyncStatus() {
  const statusEl = document.getElementById('mondaySyncStatus');
  if (statusEl) {
    if (mondayStatus && mondayStatus.last_synced_at) {
      const iso = mondayStatus.last_synced_at;
      const dt = new Date(iso.endsWith('Z') ? iso : (iso + 'Z'));
      const minsAgo = Math.max(0, Math.floor((Date.now() - dt.getTime()) / 60000));
      statusEl.textContent = minsAgo < 1
        ? 'Just synced from Monday'
        : ('Last synced: ' + minsAgo + ' min ago');
    } else {
      statusEl.textContent = 'Never synced from Monday';
    }
  }
  const banner = document.getElementById('mondaySyncBanner');
  if (banner) {
    if (mondayStatus && mondayStatus.error) {
      banner.textContent = 'Monday.com sync failed — showing the most recent cached assignments. ' + mondayStatus.error;
      banner.style.display = 'block';
    } else {
      banner.style.display = 'none';
    }
  }
}

function refreshFromMonday() {
  const btn = document.getElementById('mondayRefreshBtn');
  if (btn) { btn.disabled = true; btn.textContent = 'Refreshing...'; }
  fetch('/api/sync-monday-now', { method: 'POST' })
    .then(r => r.json().then(data => ({ ok: r.ok, data: data })))
    .then(res => {
      if (res.ok && res.data && res.data.ok) {
        location.reload();
      } else {
        const msg = (res.data && res.data.error) ? res.data.error : 'Sync failed';
        alert('Refresh failed: ' + msg);
        if (btn) { btn.disabled = false; btn.textContent = '↻ Refresh from Monday'; }
      }
    })
    .catch(err => {
      alert('Refresh failed: ' + err);
      if (btn) { btn.disabled = false; btn.textContent = '↻ Refresh from Monday'; }
    });
}

// Filter entities by selected FA
function filterByFA() {
  selectedFA = document.getElementById('faSelector').value;
  renderEntityGrid();
}

// Render entity grid
// FA dir 2026-05-23: default sort = readiness desc so the most-actionable
// buildings surface at the top of the table. FA lands → sees "Ready to
// build" rows at the top → clicks "Build now" without scanning 147 rows.
let _entitySortState = { column: "readiness", direction: "desc" };
let _stageFilter = "all";
let _faNameByEntity = {};
// Enriched per-entity data fetched from /api/budgets — has readiness tier,
// timestamps, sp_inventory, audit status, etc. Keyed by entity_code.
let _enrichedByEntity = {};

// Fetch /api/budgets and merge into _enrichedByEntity, then re-render the
// grid. Called once at page load. The template's `budgets` array stays as
// the source of building names/codes; the API call adds the readiness +
// data-status fields used to drive tiles, chips, and the hero callout.
function loadEnrichedBudgets() {
  return fetch("/api/budgets")
    .then(function (r) { return r.ok ? r.json() : []; })
    .then(function (rows) {
      const map = {};
      (rows || []).forEach(function (r) {
        if (r && r.entity_code) map[String(r.entity_code)] = r;
      });
      _enrichedByEntity = map;
      try { renderEntityGrid(); } catch (e) {}
    })
    .catch(function () { _enrichedByEntity = {}; });
}

// Build the 5 mini-tiles (B / E / Y / A / Au) for an entity row, matching
// the dashboard's color scheme. Returns an HTML string.
//
// FA dir 2026-05-24: when the building is in a fully-blocked tier (every
// source missing), render a compact "Missing: B · E · Y · A · Au" text
// label instead of 5 tiny red boxes — faster to scan and clearer about
// what the FA needs to chase down. Tiles still render when there's a
// mix (some sources in, others missing) since the visual variation is
// the point of having tiles.
function _renderEntityTiles(entityCode) {
  const e = _enrichedByEntity[String(entityCode)];
  if (!e) {
    return "<span style=\"color:var(--gray-300); font-size:11px;\">…</span>";
  }
  const ts = e.timestamps || {};
  const sp = e.sp_inventory || {};
  const au = e.audit || null;
  // Build per-source state once so we can decide tile-vs-text + compose either.
  const auOk = !!(au && au.status === "confirmed");
  const sources = [
    {letter:"B",  ok:!!ts.budget_summary, inSP:!!sp.approved_2026,        dt:ts.budget_summary},
    {letter:"E",  ok:!!e.has_expenses,    inSP:!!sp.expense_distribution, dt:ts.expense_dist},
    {letter:"Y",  ok:!!ts.ysl,            inSP:!!sp.ysl,                  dt:ts.ysl},
    {letter:"A",  ok:!!ts.open_ap,        inSP:!!sp.ap_aging,             dt:ts.open_ap},
    {letter:"Au", ok:auOk,                inSP:!!sp.audit_2025,           dt:ts.audit},
  ];
  const missing = sources.filter(function (s) { return !s.ok && !s.inSP; });
  const allMissing = missing.length === sources.length;
  if (allMissing) {
    // Compact Missing label for fully-blocked rows (NEEDS_FILES tier).
    return "<span style=\"display:inline-block; padding:3px 8px; border-radius:5px; background:#fef2f2; color:#991b1b; font-size:10px; font-weight:700; letter-spacing:0.4px;\">Missing: B · E · Y · A · Au</span>";
  }
  if (missing.length >= 3 && missing.length < sources.length) {
    // Mostly blocked — also use the text form to keep scan-speed up.
    return "<span style=\"display:inline-block; padding:3px 8px; border-radius:5px; background:#fef2f2; color:#991b1b; font-size:10px; font-weight:700; letter-spacing:0.4px;\">Missing: " + missing.map(function (s) { return s.letter; }).join(" · ") + "</span>";
  }
  // Mixed (some in, some out, or all in) — show tiles so the FA can see
  // exactly which source is which state.
  function _tile(s) {
    let cls = "miss";
    if (s.ok) cls = "ok";
    else if (s.inSP) cls = "ready";
    const dtTxt = (s.ok && s.dt)
      ? (new Date(s.dt).getMonth()+1) + "/" + (new Date(s.dt).getDate())
      : "";
    const bg = cls === "ok" ? "#def7ec" : (cls === "ready" ? "#fef3c7" : "#fef2f2");
    const fg = cls === "ok" ? "#065f46" : (cls === "ready" ? "#92400e" : "#991b1b");
    const bd = cls === "ok" ? "#a7f3d0" : (cls === "ready" ? "#fcd34d" : "#fecaca");
    return "<span style=\"display:inline-flex; flex-direction:column; align-items:center; justify-content:center; min-width:24px; height:24px; padding:1px 4px; border-radius:4px; background:" + bg + "; color:" + fg + "; border:1px solid " + bd + "; line-height:1;\">"
         + "<span style=\"font-size:10px; font-weight:700; letter-spacing:0.3px;\">" + s.letter + "</span>"
         + (dtTxt ? "<span style=\"font-size:7px; opacity:0.75; margin-top:1px; font-variant-numeric:tabular-nums;\">" + dtTxt + "</span>" : "")
         + "</span>";
  }
  return "<span style=\"display:inline-flex; gap:3px;\">" + sources.map(_tile).join("") + "</span>";
}

// FA dir 2026-05-24: compute the freshest activity timestamp across all
// ingest sources + audit + budget update. Used by the new "Waiting" column
// and the oldest-first sort within blocked tiers. Returns a Date or null.
function _entityLastActivity(b) {
  const e = _enrichedByEntity[String(b.entity_code)];
  const candidates = [];
  if (e) {
    const ts = e.timestamps || {};
    ["budget_summary", "expense_dist", "ysl", "open_ap", "audit"].forEach(function (k) {
      if (ts[k]) candidates.push(ts[k]);
    });
    if (e.updated_at) candidates.push(e.updated_at);
  }
  if (b.updated_at) candidates.push(b.updated_at);
  if (!candidates.length) return null;
  // Pick the most recent — that's "last touched".
  let max = null;
  candidates.forEach(function (iso) {
    const dt = new Date(iso);
    if (isNaN(dt.getTime())) return;
    if (!max || dt > max) max = dt;
  });
  return max;
}

// Render the days-since-last-activity for the Waiting column. Color shifts
// to amber >14, red >21 so rotten rows surface.
function _renderWaitingCell(b) {
  const last = _entityLastActivity(b);
  if (!last) {
    return "<span style=\"color:var(--gray-400); font-size:11px;\">—</span>";
  }
  const days = Math.floor((Date.now() - last.getTime()) / 86400000);
  let color = "var(--gray-600)";
  let weight = "500";
  if (days >= 21) { color = "#dc2626"; weight = "700"; }
  else if (days >= 14) { color = "#a16207"; weight = "600"; }
  const lbl = days === 0 ? "today" : (days === 1 ? "1 day" : days + " days");
  return "<span title=\"Last activity " + last.toLocaleDateString() + "\" style=\"color:" + color + "; font-weight:" + weight + "; font-size:11px; font-variant-numeric:tabular-nums;\">" + lbl + "</span>";
}

// FA dir 2026-05-24: build the contextual Next-step action button. Never
// shows a greyed-out "Waiting for files" tautology — always offers a real
// move (build, confirm audit, email FA, open SP folder, view built).
function _renderNextStep(b, faName) {
  const e = _enrichedByEntity[String(b.entity_code)] || {};
  const readiness = e.readiness || {tier: "NEEDS_FILES", next_action: "wait", next_url: "/wizard/" + b.entity_code};
  const act = readiness.next_action;
  // Build the href + visual treatment based on what the FA can actually do.
  let label, bg, fg, bd, href;
  href = readiness.next_url || ("/wizard/" + b.entity_code);
  if (act === "build") {
    label = "Build now →"; bg = "#16a34a"; fg = "#fff"; bd = "#16a34a";
  } else if (act === "audit_review") {
    label = "Confirm audit →"; bg = "#fffbeb"; fg = "#92400e"; bd = "#fcd34d";
  } else if (act === "review_built") {
    label = "View built"; bg = "#fff"; fg = "var(--blue)"; bd = "var(--gray-300)";
  } else {
    // "wait" tier — give the FA an actual move depending on what's missing.
    const tier = readiness.tier;
    if (tier === "NEEDS_AUDIT") {
      label = "Upload audit →"; bg = "#fef2f2"; fg = "#991b1b"; bd = "#fecaca";
      href = "/wizard/" + b.entity_code + "?step=2&focus=audit_2025";
    } else if (tier === "NEEDS_FILES" && faName && faName !== "—") {
      // Email the responsible FA / PM. mailto: opens their mail client.
      label = "Chase " + faName.split(" ")[0] + " →"; bg = "#fef2f2"; fg = "#991b1b"; bd = "#fecaca";
      href = "mailto:?subject=" + encodeURIComponent("Budget files needed for " + (b.building_name || b.entity_code))
           + "&body=" + encodeURIComponent("Hi " + faName.split(" ")[0] + ",\n\nCan you upload the missing source files for " + (b.building_name || b.entity_code) + " (entity " + b.entity_code + ") to SharePoint? It's currently blocking the 2027 budget build.\n\nThanks!");
    } else {
      label = "Open in wizard →"; bg = "#fff"; fg = "var(--gray-700)"; bd = "var(--gray-300)";
    }
  }
  return "<a href=\"" + href + "\" style=\"display:inline-block; font-size:11px; font-weight:700; padding:5px 11px; border-radius:6px; border:1px solid " + bd + "; background:" + bg + "; color:" + fg + "; text-decoration:none; letter-spacing:0.2px;\">" + label + "</a>";
}

function _rebuildFaNameLookup() {
  _faNameByEntity = {};
  const usersById = {};
  (faUsers || []).forEach(function (u) { usersById[String(u.id)] = u.name; });
  (faAssignments || []).forEach(function (a) {
    if (a.role && a.role !== "fa") return;
    const existing = _faNameByEntity[a.entity_code];
    const name = usersById[String(a.user_id)];
    if (!name) return;
    _faNameByEntity[a.entity_code] = existing ? (existing + ", " + name) : name;
  });
}

function setEntitySort(col) {
  if (_entitySortState.column === col) {
    _entitySortState.direction = _entitySortState.direction === "asc" ? "desc" : "asc";
  } else {
    _entitySortState.column = col;
    _entitySortState.direction = (col === "entity_code") ? "asc" : "asc";
  }
  renderEntityGrid();
}

function setStageFilter(stage) {
  _stageFilter = stage;
  // FA dir 2026-05-24: push the filter into the URL so browser back from a
  // building detail page restores the filter the FA had set. Without this,
  // clicking a building then hitting back resets to "all" — and the FA has
  // to re-filter on every dive-in. Replace if same stage to avoid history
  // pollution when the FA clicks the active chip.
  try {
    const url = new URL(window.location.href);
    if (stage === "all") {
      url.searchParams.delete("tier");
    } else {
      url.searchParams.set("tier", stage);
    }
    const currentTier = new URL(window.location.href).searchParams.get("tier") || "all";
    if (currentTier === stage) {
      window.history.replaceState({ tier: stage }, "", url.toString());
    } else {
      window.history.pushState({ tier: stage }, "", url.toString());
    }
  } catch (e) {}
  renderEntityGrid();
}

// FA dir 2026-05-24: restore filter from URL on back/forward navigation.
window.addEventListener("popstate", function () {
  try {
    const tier = new URL(window.location.href).searchParams.get("tier") || "all";
    _stageFilter = tier;
    renderEntityGrid();
  } catch (e) {}
});

function _entitySortValue(b, col) {
  if (col === "entity_code") return parseInt(b.entity_code, 10) || 0;
  if (col === "building_name") return (b.building_name || "").toLowerCase();
  if (col === "fa_name") return (_faNameByEntity[b.entity_code] || "").toLowerCase();
  if (col === "lifecycle_stage") {
    const order = ["Setup","Sources Collected","Assumptions Confirmed","Budget Built (draft)","PM Review","Approved"];
    const idx = order.indexOf(b.lifecycle_stage || "Setup");
    return idx < 0 ? 99 : idx;
  }
  if (col === "updated_at") return new Date(b.updated_at || 0).getTime();
  if (col === "readiness") {
    // Higher tier_order = more actionable. Use API-supplied tier_order
    // when present (server-side classifier); fallback to 0 for entities
    // missing from /api/budgets (e.g. brand-new rows).
    const e = _enrichedByEntity[String(b.entity_code)];
    return (e && e.readiness && typeof e.readiness.tier_order === "number")
      ? e.readiness.tier_order : 0;
  }
  if (col === "age") {
    // FA dir 2026-05-24: sort by days-since-last-activity. Default direction
    // is desc → oldest-rotten-rows surface first. Returns days as a number.
    const last = _entityLastActivity(b);
    if (!last) return -1;  // unknown — push to the bottom on desc sort
    return Math.floor((Date.now() - last.getTime()) / 86400000);
  }
  return 0;
}

function _formatRelativeTime(iso) {
  if (!iso) return "—";
  const dt = new Date(iso);
  if (isNaN(dt.getTime())) return "—";
  const now = Date.now();
  const diffMs = now - dt.getTime();
  const mins = Math.floor(diffMs / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return mins + " min ago";
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return hrs + (hrs === 1 ? " hr ago" : " hrs ago");
  const days = Math.floor(hrs / 24);
  if (days < 7) return days + (days === 1 ? " day ago" : " days ago");
  return dt.toLocaleDateString();
}

function _stageColor(stage) {
  const map = {
    "Setup":                  "var(--gray-100)",
    "Sources Collected":      "#dbeafe",
    "Assumptions Confirmed":  "#fef3c7",
    "Budget Built (draft)":   "#dcfce7",
    "PM Review":              "#fde68a",
    "Approved":               "#bbf7d0",
  };
  return map[stage] || "var(--gray-100)";
}

function renderEntityGrid() {
  // Build FA name lookup if needed (faUsers/faAssignments are loaded once at init).
  if (Object.keys(_faNameByEntity).length === 0) _rebuildFaNameLookup();

  const tbody = document.getElementById("entityTableBody");
  if (!tbody) return;
  tbody.innerHTML = "";

  // 1. Filter by FA dropdown
  let filtered = budgets;
  if (selectedFA) {
    const assigned = new Set(
      faAssignments.filter(function (a) { return String(a.user_id) === selectedFA; })
                   .map(function (a) { return a.entity_code; })
    );
    filtered = filtered.filter(function (b) { return assigned.has(b.entity_code); });
  }

  // 2. Filter by readiness tier chip (FA dir 2026-05-23). Old logic
  //    filtered on lifecycle_stage which was always "Setup" — useless.
  //    New tiers come from the server-side readiness classifier.
  //    FA dir 2026-05-24: special composite "AUDIT_REVIEW" filter unions
  //    IN_PROGRESS + NEEDS_AUDIT_EXTRACT (both are "audit waiting for FA")
  //    so the hero CTA matches its promise of 14 buildings, not just 11.
  if (_stageFilter && _stageFilter !== "all") {
    filtered = filtered.filter(function (b) {
      const e = _enrichedByEntity[String(b.entity_code)];
      const tier = (e && e.readiness && e.readiness.tier) || "NEEDS_FILES";
      if (_stageFilter === "AUDIT_REVIEW") {
        return tier === "IN_PROGRESS" || tier === "NEEDS_AUDIT_EXTRACT";
      }
      return tier === _stageFilter;
    });
  }

  // 3. Filter by search
  const searchInput = document.getElementById("entitySearch");
  const searchTerm = (searchInput ? searchInput.value : "").toLowerCase().trim();
  if (searchTerm) {
    filtered = filtered.filter(function (b) {
      const name = (b.building_name || "").toLowerCase();
      const code = (b.entity_code || "").toLowerCase();
      const fa = (_faNameByEntity[b.entity_code] || "").toLowerCase();
      return name.includes(searchTerm) || code.includes(searchTerm) || fa.includes(searchTerm);
    });
  }

  // 4. Sort
  const dir = _entitySortState.direction === "asc" ? 1 : -1;
  filtered.sort(function (a, b) {
    const va = _entitySortValue(a, _entitySortState.column);
    const vb = _entitySortValue(b, _entitySortState.column);
    if (va < vb) return -1 * dir;
    if (va > vb) return 1 * dir;
    return 0;
  });

  // 5. Update column-header arrow indicators
  document.querySelectorAll("#entityTable th[data-col]").forEach(function (th) {
    const col = th.getAttribute("data-col");
    const arrow = th.querySelector(".sort-arrow");
    if (!arrow) return;
    if (col === _entitySortState.column) {
      arrow.style.opacity = "1";
      arrow.innerHTML = _entitySortState.direction === "asc" ? "&#9650;" : "&#9660;";
    } else {
      arrow.style.opacity = "0.3";
      arrow.innerHTML = "&#9650;";
    }
  });

  // 6. FA dir 2026-05-23: render readiness-tier chips + hero callout.
  //    Chips reflect ACTUAL state (server-classified) instead of the old
  //    "Setup × 147" non-info. Hero surfaces the actionable queue.
  const stageRow = document.getElementById("stageFilterRow");
  if (stageRow) {
    let chipBase = budgets;
    if (selectedFA) {
      const assigned = new Set(
        faAssignments.filter(function (a) { return String(a.user_id) === selectedFA; })
                     .map(function (a) { return a.entity_code; })
      );
      chipBase = chipBase.filter(function (b) { return assigned.has(b.entity_code); });
    }
    // Count by readiness tier across the chipBase
    const tiers = ["BUILT","READY_TO_BUILD","IN_PROGRESS","NEEDS_AUDIT_EXTRACT","NEEDS_AUDIT","NEEDS_FILES"];
    const counts = { "all": chipBase.length };
    tiers.forEach(function (t) { counts[t] = 0; });
    chipBase.forEach(function (b) {
      const e = _enrichedByEntity[String(b.entity_code)];
      const t = (e && e.readiness && e.readiness.tier) || "NEEDS_FILES";
      if (counts[t] !== undefined) counts[t] += 1;
    });

    // FA dir 2026-05-24: cleared the explicit "FILTER BY READINESS" header
    // label in the new compact layout, so just blow away the row contents
    // before re-rendering chips. Previous code preserved any child <span>,
    // which after the first render grabbed a chip's count span as a leftover
    // (showed "147" floating to the left of the All chip).
    stageRow.innerHTML = "";

    function makeChip(label, count, key, color) {
      // FA dir 2026-05-24: when composite AUDIT_REVIEW filter is on, both
      // IN_PROGRESS and NEEDS_AUDIT_EXTRACT chips show as active.
      const isComposite = _stageFilter === "AUDIT_REVIEW";
      const isActive = (_stageFilter === key) ||
        (isComposite && (key === "IN_PROGRESS" || key === "NEEDS_AUDIT_EXTRACT"));
      const chip = document.createElement("button");
      chip.type = "button";
      chip.innerHTML = label + " <span style=\"opacity:0.7; font-weight:500;\">" + count + "</span>";
      const activeBg = color || "#5a4a3f";
      chip.style.cssText =
        "font-size:12px; font-weight:" + (isActive ? "700" : "600") +
        "; padding:6px 12px; border-radius:14px; cursor:pointer; border:1px solid " +
        (isActive ? activeBg : "var(--gray-200)") +
        "; background:" + (isActive ? activeBg : "white") +
        "; color:" + (isActive ? "white" : "var(--gray-700)") + ";";
      chip.onclick = function () { setStageFilter(key); };
      return chip;
    }
    stageRow.appendChild(makeChip("All", counts.all, "all"));
    stageRow.appendChild(makeChip("Ready to build", counts.READY_TO_BUILD, "READY_TO_BUILD", "#16a34a"));
    stageRow.appendChild(makeChip("In progress",    counts.IN_PROGRESS,    "IN_PROGRESS",    "#d97706"));
    stageRow.appendChild(makeChip("Audit ready to extract", counts.NEEDS_AUDIT_EXTRACT, "NEEDS_AUDIT_EXTRACT", "#0369a1"));
    stageRow.appendChild(makeChip("Waiting for audit", counts.NEEDS_AUDIT, "NEEDS_AUDIT"));
    stageRow.appendChild(makeChip("Waiting for files", counts.NEEDS_FILES, "NEEDS_FILES"));
    stageRow.appendChild(makeChip("Built", counts.BUILT, "BUILT", "#065f46"));

    // Hero callout — FA dir 2026-05-24: state-aware. Pushes toward the
    // binding constraint, not just the loudest count.
    //   N ready                  → "Build all N →" CTA (most actionable)
    //   0 ready + M extract/inprog → push toward audit queue
    //   0 ready + 0 audit + K need-files → push toward chasing PMs
    //   all built / nothing actionable → no hero
    const hero = document.getElementById("readinessHero");
    if (hero) {
      const ready = counts.READY_TO_BUILD || 0;
      const inProg = (counts.IN_PROGRESS || 0) + (counts.NEEDS_AUDIT_EXTRACT || 0);
      const needFiles = counts.NEEDS_FILES || 0;
      const needAudit = counts.NEEDS_AUDIT || 0;
      if (ready > 0) {
        hero.style.display = "block";
        hero.innerHTML =
          "<div style=\"background:linear-gradient(90deg,#ecfdf5 0%,#f0fdf4 100%); border:1px solid #6ee7b7; border-radius:10px; padding:14px 18px; display:flex; align-items:center; gap:14px;\">"
        + "<span style=\"font-size:22px;\">🚀</span>"
        + "<div style=\"flex:1;\"><strong style=\"color:#065f46; font-size:14px;\">" + ready + " building" + (ready === 1 ? "" : "s") + " ready to build right now</strong>"
        + "<div style=\"color:#047857; font-size:11px; margin-top:2px;\">All files staged, audit confirmed. " + (inProg > 0 ? (inProg + " more in progress." ) : "") + "</div></div>"
        + "<button onclick=\"buildAllReady()\" style=\"background:#16a34a; color:#fff; border:none; padding:8px 16px; border-radius:6px; font-weight:700; font-size:13px; cursor:pointer;\">Build all " + ready + " →</button>"
        + "</div>";
      } else if (inProg > 0) {
        // 0 ready, but audit review can unlock builds today — push there.
        hero.style.display = "block";
        hero.innerHTML =
          "<div style=\"background:#fff7ed; border:1px solid #fdba74; border-left:5px solid #ea580c; border-radius:0 10px 10px 0; padding:14px 18px; display:flex; align-items:center; gap:14px;\">"
        + "<span style=\"font-size:22px;\">🔓</span>"
        + "<div style=\"flex:1;\"><strong style=\"color:#9a3412; font-size:14px;\">Closest to ready: " + inProg + " building" + (inProg === 1 ? "" : "s") + " in audit review</strong>"
        + "<div style=\"color:#7c2d12; font-size:11px; margin-top:2px;\">Confirm those and " + inProg + " unlock for build today. Most leverage right now.</div></div>"
        + "<button onclick=\"setStageFilter('AUDIT_REVIEW')\" style=\"background:#ea580c; color:#fff; border:none; padding:8px 16px; border-radius:7px; font-weight:700; font-size:12px; cursor:pointer;\">Open audit queue →</button>"
        + "</div>";
      } else if (needAudit > 0) {
        // Stuck waiting for auditor delivery — surface the count + filter cta.
        hero.style.display = "block";
        hero.innerHTML =
          "<div style=\"background:#fef2f2; border:1px solid #fecaca; border-left:5px solid #dc2626; border-radius:0 10px 10px 0; padding:12px 16px; display:flex; align-items:center; gap:12px;\">"
        + "<span style=\"font-size:18px;\">⏳</span>"
        + "<div style=\"flex:1;\"><strong style=\"color:#991b1b; font-size:13px;\">" + needAudit + " building" + (needAudit === 1 ? "" : "s") + " waiting on audit PDF</strong>"
        + "<div style=\"color:#991b1b; font-size:11px; margin-top:1px;\">Vendor-blocked. Sort by Waiting to find the most rotten and chase auditors.</div></div>"
        + "<button onclick=\"setStageFilter('NEEDS_AUDIT')\" style=\"background:#fff; color:#991b1b; border:1px solid #dc2626; padding:6px 12px; border-radius:6px; font-weight:700; font-size:12px; cursor:pointer;\">Show audit queue →</button>"
        + "</div>";
      } else if (needFiles > 0) {
        // Last-resort: only file-collection blockers remain.
        hero.style.display = "block";
        hero.innerHTML =
          "<div style=\"background:#fef2f2; border:1px solid #fecaca; border-radius:10px; padding:12px 16px; display:flex; align-items:center; gap:12px;\">"
        + "<span style=\"font-size:18px;\">📂</span>"
        + "<div style=\"flex:1;\"><strong style=\"color:#991b1b; font-size:13px;\">" + needFiles + " building" + (needFiles === 1 ? "" : "s") + " missing source files</strong>"
        + "<div style=\"color:#991b1b; font-size:11px; margin-top:1px;\">Chase PMs to upload to SharePoint. Click \"Needs files\" to see the list.</div></div>"
        + "</div>";
      } else {
        hero.style.display = "none";
        hero.innerHTML = "";
      }
    }
  }

  // FA dir 2026-05-24 #9: portfolio summary in title — live counts inline.
  const summaryEl = document.getElementById("portfolioSummary");
  if (summaryEl) {
    const total = budgets.length;
    const built = (function () {
      let n = 0;
      Object.values(_enrichedByEntity).forEach(function (e) {
        if (e && e.readiness && e.readiness.tier === "BUILT") n += 1;
      });
      return n;
    })();
    const ready = (function () {
      let n = 0;
      Object.values(_enrichedByEntity).forEach(function (e) {
        if (e && e.readiness && e.readiness.tier === "READY_TO_BUILD") n += 1;
      });
      return n;
    })();
    summaryEl.textContent = "· " + total + " buildings · " + built + " built · " + ready + " ready";
  }

  // 7. Update header count caption
  const countEl = document.getElementById("faEntityCount");
  if (countEl) {
    const total = selectedFA
      ? faAssignments.filter(function (a) { return String(a.user_id) === selectedFA && (a.role === "fa" || !a.role); }).length
      : budgets.length;
    const showing = filtered.length;
    if (searchTerm || _stageFilter !== "all") {
      countEl.textContent = showing + " of " + total + " shown";
    } else if (selectedFA) {
      countEl.textContent = showing + " building" + (showing !== 1 ? "s" : "") + " assigned";
    } else {
      countEl.textContent = total + " total buildings";
    }
  }

  // 8. Render rows. Empty-state row if no matches.
  if (filtered.length === 0) {
    const msg = searchTerm
      ? "No entities match \"" + searchTerm + "\""
      : (_stageFilter !== "all" ? ("No entities in " + _stageFilter + " stage")
         : (selectedFA ? "No buildings assigned to this analyst." : "No entities found."));
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 6;
    td.style.cssText = "padding:32px; color:var(--gray-500); font-size:13px; text-align:center;";
    td.textContent = msg;
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  filtered.forEach(function (b, idx) {
    const tr = document.createElement("tr");
    const isSelected = (selectedEntity === b.entity_code);
    tr.style.cssText = "transition:background 0.1s; border-bottom:1px solid var(--gray-100);" +
                       (idx % 2 === 1 ? " background:#fbfaf6;" : "") +
                       (isSelected ? " background:#f5efe7;" : "");

    const faName = _faNameByEntity[b.entity_code] || "—";
    const enriched = _enrichedByEntity[String(b.entity_code)] || {};
    const readiness = enriched.readiness || {tier: "NEEDS_FILES", tier_label: "Setup", next_action: "wait", next_url: "/wizard/" + b.entity_code};

    // Building name — clickable to select entity in wizard (legacy behavior)
    const nameTd = document.createElement("td");
    nameTd.style.cssText = "padding:10px 14px; font-size:13px; color:var(--gray-900); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; font-weight:600; cursor:pointer;";
    nameTd.textContent = b.building_name || b.entity_code;
    nameTd.onclick = function () { selectEntity(b.entity_code, b.building_name || b.entity_code); };
    nameTd.onmouseenter = function () { tr.style.background = "#f4f1eb"; };
    nameTd.onmouseleave = function () { tr.style.background = isSelected ? "#f5efe7" : (idx % 2 === 1 ? "#fbfaf6" : ""); };
    tr.appendChild(nameTd);

    const cell = function (text, css) {
      const td = document.createElement("td");
      td.style.cssText = "padding:10px 14px; font-size:13px; color:var(--gray-900); overflow:hidden; text-overflow:ellipsis; white-space:nowrap;" + (css || "");
      td.textContent = text;
      return td;
    };
    tr.appendChild(cell(b.entity_code, " font-family:ui-monospace,monospace; color:var(--gray-700);"));
    // FA cell — name clickable to filter the table by that FA.
    const faTd = document.createElement("td");
    faTd.style.cssText = "padding:10px 14px; font-size:12px; color:var(--gray-700);";
    if (faName !== "—") {
      const faLink = document.createElement("a");
      faLink.textContent = faName;
      faLink.href = "#";
      faLink.style.cssText = "color:var(--blue, #2563eb); text-decoration:none; font-weight:600; cursor:pointer;";
      faLink.title = "Filter to " + faName + "'s buildings";
      faLink.onclick = function (e) {
        e.preventDefault();
        e.stopPropagation();
        // Find the user id from the rebuilt lookup and apply.
        const matchUser = (faUsers || []).find(function (u) { return u.name === faName; });
        if (matchUser) {
          const sel = document.getElementById("faSelector");
          if (sel) { sel.value = String(matchUser.id); }
          selectedFA = String(matchUser.id);
          renderEntityGrid();
        }
      };
      faTd.appendChild(faLink);
    } else {
      faTd.textContent = "—";
    }
    tr.appendChild(faTd);

    // Data-status tiles (B / E / Y / A / Au) — mirrors dashboard.
    // Switches automatically to "Missing: X · Y · Z" text for blocked tiers.
    const tilesTd = document.createElement("td");
    tilesTd.style.cssText = "padding:10px 14px;";
    tilesTd.innerHTML = _renderEntityTiles(b.entity_code);
    tr.appendChild(tilesTd);

    // Waiting column — days since last activity. Red text >21 days.
    const waitTd = document.createElement("td");
    waitTd.style.cssText = "padding:10px 14px;";
    waitTd.innerHTML = _renderWaitingCell(b);
    tr.appendChild(waitTd);

    // Contextual Next-step action — never greyed-out tautology.
    const actTd = document.createElement("td");
    actTd.style.cssText = "padding:10px 14px; text-align:right;";
    actTd.innerHTML = _renderNextStep(b, faName);
    tr.appendChild(actTd);

    tbody.appendChild(tr);
  });
}

// FA dir 2026-05-23: bulk-build action — hits the admin endpoint that
// auto-walks every READY_TO_BUILD entity. Confirmation gate first; the
// server returns a per-entity summary which we surface in an alert.
function buildAllReady() {
  // Count up the targets from enriched data
  let readyCount = 0;
  Object.values(_enrichedByEntity).forEach(function (e) {
    if (e && e.readiness && e.readiness.tier === "READY_TO_BUILD") readyCount += 1;
  });
  if (readyCount === 0) {
    alert("No buildings are currently ready to build.");
    return;
  }
  if (!confirm("Build all " + readyCount + " ready building" + (readyCount === 1 ? "" : "s") + " now?\\n\\nEach takes ~30-60 seconds. The page will stay open while it runs.")) return;
  const hero = document.getElementById("readinessHero");
  if (hero) hero.innerHTML = "<div style=\"background:#fffbeb; border:1px solid #fcd34d; border-radius:10px; padding:14px 18px; font-size:13px; color:#92400e;\">⏳ Building " + readyCount + " budgets… this may take a few minutes. Do not close the tab.</div>";
  fetch("/api/admin/build-all-ready", { method: "POST", headers: {"Content-Type": "application/json"}, body: "{}" })
    .then(function (r) { return r.json(); })
    .then(function (data) {
      const ok = (data.results || []).filter(function (r) { return r.ok; }).length;
      const fail = (data.results || []).filter(function (r) { return !r.ok; }).length;
      alert("Built " + ok + " of " + readyCount + " buildings."
          + (fail > 0 ? "\\n" + fail + " failed — see dashboard for details." : ""));
      // Refresh the enriched data to reflect new BUILT state
      loadEnrichedBudgets();
    })
    .catch(function (err) {
      alert("Bulk build failed: " + err);
      loadEnrichedBudgets();
    });
}

// Select entity
function selectEntity(code, name) {
  selectedEntity = code;
  document.getElementById('railEntityName').textContent = name;
  renderEntityGrid();
  completeStep(1);
  // Kick off SharePoint detection for Step 2 as soon as entity is picked.
  try { loadSharepointSources(); } catch (e) {}
  try { loadFoundationStatus(); } catch (e) {}
  try { loadWizardSelections(); } catch (e) {}
  // L3: refresh title + breadcrumb immediately so deep-links land on the
  // right chrome before any async loads return.
  try { _updateWizardChrome(typeof currentStep === 'number' ? currentStep : 2); } catch (e) {}
}

// Render upload checklist
// FA file selections (staged but not yet built) for current entity.
// _wizardSelections = what the FA clicked (Budget.wizard_selections_json).
// _ingestState = actual DB-table presence (same source of truth the FA
// Dashboard uses). The wizard renders state from _ingestState so the two
// pages can't disagree. See /api/wizard/<ec>/selections endpoint.
let _wizardSelections = {};
let _ingestState = {};

function loadWizardSelections() {
  const ent = selectedEntity;
  if (!ent) return;
  fetch("/api/wizard/" + ent + "/selections")
    .then(function (r) { return r.json(); })
    .then(function (data) {
      _wizardSelections = data.selections || {};
      _ingestState = data.ingest_state || {};
      // Re-render any panels that show selection state
      try { renderSharepointSources(); } catch (e) {}
    })
    .catch(function () { _wizardSelections = {}; _ingestState = {}; });
}

// (Removed: 2026 approved budget panel — now rendered via shared FROM SHAREPOINT panel)

// SharePoint sources state for current entity (Step 2)
let _spSources = null;

function loadSharepointSources() {
  const ent = selectedEntity;
  if (!ent) return;
  // Kick off the foundation status load in parallel — they\'re independent.
  try { loadFoundationStatus(); } catch (e) {}
  const panel = document.getElementById("spSourcesPanel");
  const body = document.getElementById("spSourcesBody");
  const info = document.getElementById("spFolderInfo");
  const btn = document.getElementById("spRefreshBtn");
  if (!panel || !body) return;
  panel.style.display = "block";
  body.innerHTML = "<div style=\"color:var(--gray-500); font-size:13px;\">Checking SharePoint...</div>";
  if (btn) { btn.disabled = true; btn.textContent = "Checking..."; }
  fetch("/api/wizard/" + ent + "/sharepoint-sources")
    .then(function (r) { return r.json(); })
    .then(function (data) {
      _spSources = data;
      renderSharepointSources();
      // Re-render foundation panel now that filename details are available.
      try { renderFoundationPanel(); } catch (e) {}
      if (info) {
        if (data.folder_exists) {
          const total = (data.by_source_type.ysl||[]).length + (data.by_source_type.expense_distribution||[]).length + (data.by_source_type.ap_aging||[]).length + (data.by_source_type.maint_proof||[]).length;
          info.textContent = total + " matched · " + (data.by_source_type.unmatched||[]).length + " other";
        } else {
          info.textContent = "Folder not yet created";
        }
      }
    })
    .catch(function (err) {
      body.innerHTML = "<div style=\"color:#b45309; font-size:13px;\">SharePoint lookup failed: " + err + "</div>";
    })
    .finally(function () {
      if (btn) { btn.disabled = false; btn.textContent = "↻ Refresh from SP"; }
    });
}

function renderSharepointSources() {
  const body = document.getElementById("spSourcesBody");
  if (!body || !_spSources) return;
  if (!_spSources.folder_exists) {
    body.innerHTML = "<div style=\"color:var(--gray-500); font-size:13px;\">No SharePoint folder yet for this entity. Files you upload below will be saved to a new folder.</div>";
    return;
  }
  const slots = [
    { key: "ysl",                  label: "YSL" },
    { key: "expense_distribution", label: "Expense Distribution" },
    { key: "ap_aging",             label: "AP Aging" },
    { key: "maint_proof",          label: "Maintenance Proof" }
  ];
  // FA dir 2026-05-22 (A2): honest 3-state per source. The previous logic
  // collapsed "file found in SP" and "ingested into budget" into one green
  // ✓ — which is why the dashboard could show ✗YSL even though wizard
  // step 2 looked fully checked. New states:
  //   ingested   = file detected AND already in _wizardSelections → green
  //   ingesting  = auto-ingest fired this session, awaiting server → blue dot
  //   ready      = file detected, not yet ingested → amber (will auto-fire)
  //   missing    = no file in SP folder → existing "not in folder" copy
  // FA dir 2026-05-23: state determination now reads the SAME truth as the
  // FA dashboard — the actual ingest tables, via _ingestState. Previously
  // we trusted _wizardSelections alone (what the FA clicked) which could
  // diverge from real DB state when a parse silently failed. Now:
  //   ingested = data actually in DB tables (matches dashboard green)
  //   ingesting = auto-ingest fired this session, awaiting server
  //   ready = file in SP, not yet in DB → will auto-ingest
  //   missing = no file in SP
  function _stateFor(slotKey, hasFile) {
    const ing = _ingestState && _ingestState[slotKey];
    if (ing && ing.ingested) return 'ingested';
    if (!hasFile) return 'missing';
    const ent = selectedEntity;
    if (ent && sessionStorage.getItem('cb_auto_' + ent + '_' + slotKey) === 'fired') {
      return 'ingesting';
    }
    return 'ready';
  }
  function _ingestedAtFor(slotKey) {
    // Prefer the real ingest-table timestamp; fall back to selection time.
    try {
      const ing = _ingestState && _ingestState[slotKey];
      if (ing && ing.at) {
        const d = new Date(ing.at);
        return (d.getMonth()+1) + '/' + d.getDate();
      }
      const sel = (_wizardSelections || {})[slotKey];
      if (sel && sel.selected_at) {
        const d = new Date(sel.selected_at);
        return (d.getMonth()+1) + '/' + d.getDate();
      }
    } catch (e) {}
    return '';
  }
  function _ingestDetailFor(slotKey) {
    // Optional info shown alongside the state pill — line/invoice counts
    // give the FA visual reassurance that data actually flowed.
    try {
      const ing = _ingestState && _ingestState[slotKey];
      if (!ing || !ing.ingested) return '';
      if (slotKey === 'expense_distribution' && ing.invoices) return ing.invoices + ' invoices';
      if (slotKey === 'ap_aging' && ing.invoices) return ing.invoices + ' invoices';
      if (slotKey === 'approved_2026' && ing.rows) return ing.rows + ' rows';
    } catch (e) {}
    return '';
  }
  let html = "";
  slots.forEach(function (slot) {
    const files = (_spSources.by_source_type[slot.key] || []);
    const hasAny = files.length > 0;
    const state = _stateFor(slot.key, hasAny);
    // Header pill: state-aware label + color
    let headerColor = "#b45309", headerIcon = "↑", headerNote = "not in folder — use upload below";
    if (state === 'ingested') {
      headerColor = "#15803d"; headerIcon = "✓";
      const dt = _ingestedAtFor(slot.key);
      const detail = _ingestDetailFor(slot.key);
      // "ingested 5/22 · 67 invoices" — gives FA a visual receipt that
      // real data flowed, not just that a click was recorded.
      let parts = ["ingested"];
      if (dt) parts.push(dt);
      headerNote = parts.join(" ") + (detail ? (" · " + detail) : "");
    } else if (state === 'ingesting') {
      headerColor = "#1d4ed8"; headerIcon = "⟳";
      headerNote = "auto-ingesting…";
    } else if (state === 'ready') {
      headerColor = "#92400e"; headerIcon = "📥";
      headerNote = "ready — will auto-ingest";
    }
    html += "<div style=\"border:1px solid var(--gray-200); border-radius:8px; padding:12px 14px;\">";
    html += "<div style=\"display:flex; align-items:center; gap:10px; margin-bottom:" + (hasAny ? "8px" : "0") + ";\">";
    html += "<span style=\"color:" + headerColor + "; font-weight:700;\">" + headerIcon + "</span>";
    html += "<span style=\"font-weight:600;\">" + slot.label + "</span>";
    html += "<span style=\"flex:1\"></span>";
    html += "<span style=\"font-size:12px; color:" + headerColor + "; font-weight:600;\">" + headerNote + "</span>";
    html += "</div>";
    if (hasAny) {
      files.forEach(function (f) {
        html += "<div style=\"display:flex; align-items:center; gap:10px; padding:8px 0; border-top:1px solid var(--gray-100);\">";
        html += "<span style=\"font-family:ui-monospace,monospace; font-size:12px; flex:1; color:var(--text-200,#374151); overflow-wrap:anywhere;\">" + escapeHtml(f.name) + "</span>";
        html += "<span style=\"font-size:11px; color:var(--gray-500); white-space:nowrap;\">" + (f.size ? Math.round(f.size/1024) + " KB" : "") + "</span>";
        if (f.web_url) {
          html += "<a href=\"" + f.web_url + "\" target=\"_blank\" rel=\"noopener\" style=\"font-size:12px; color:var(--blue); text-decoration:none;\">Open in SP ↗</a>";
        }
        // Action button copy varies by state:
        //   ingested  → "⟳ Re-ingest"  (manual override)
        //   ingesting → disabled "Working…"
        //   ready     → "Ingest now" (manual fallback if auto-ingest can't fire)
        let btnLabel = "Ingest now", btnDisabled = false, btnStyle = "border:1px solid var(--blue); color:var(--blue);";
        if (state === 'ingested') { btnLabel = "⟳ Re-ingest"; btnStyle = "border:1px solid var(--gray-300); color:var(--gray-700);"; }
        else if (state === 'ingesting') { btnLabel = "Working…"; btnDisabled = true; btnStyle = "border:1px solid var(--gray-300); color:var(--gray-500); opacity:0.6;"; }
        html += "<button type=\"button\" data-action=\"select-yardi\"" + (btnDisabled ? " disabled" : "") +
                " data-source-type=\"" + escapeHtmlAttr(slot.key) + "\"" +
                " data-item-id=\"" + escapeHtmlAttr(f.item_id) + "\"" +
                " data-filename=\"" + escapeHtmlAttr(f.name) + "\"" +
                " style=\"font-size:12px; padding:5px 10px; background:white; border-radius:4px; cursor:" + (btnDisabled ? "not-allowed" : "pointer") + "; font-weight:600; " + btnStyle + "\">" + btnLabel + "</button>";
        html += "</div>";
      });
    }
    html += "</div>";
  });
  // Unmatched files (informational)
  const unmatched = _spSources.by_source_type.unmatched || [];
  if (unmatched.length > 0) {
    html += "<div style=\"font-size:11px; color:var(--gray-500); margin-top:6px;\">Other files in folder (not auto-classified): ";
    html += unmatched.map(function (f) { return escapeHtml(f.name); }).join(", ");
    html += "</div>";
  }
  body.innerHTML = html;
  body.querySelectorAll('button[data-action="select-yardi"]').forEach(function (btn) {
    btn.addEventListener('click', function () {
      useSharepointFile(
        btn.getAttribute('data-source-type'),
        btn.getAttribute('data-item-id'),
        btn.getAttribute('data-filename')
      );
    });
  });
  // Kick off auto-ingest AFTER rendering — small delay so the FA actually
  // sees the "📥 Ready" state for a beat before it flips to "⟳ Auto-ingesting".
  try { setTimeout(_maybeAutoIngestSources, 350); } catch (e) {}
}

// FA dir 2026-05-22 (A2): auto-ingest controller. For each Yardi source
// slot, if (a) we have exactly ONE matching SP file (ambiguous matches get
// skipped — FA must pick manually), (b) the FA hasn't already ingested it
// (no entry in _wizardSelections), and (c) we haven't already attempted
// this session (sessionStorage guard), kick off /use-sp-source silently.
// Failures don't auto-retry — the "Ingest now" button stays available so
// the FA can manually trigger if auto-ingest hits a parse error.
function _maybeAutoIngestSources() {
  if (!_spSources || !_spSources.by_source_type) return;
  const ent = selectedEntity;
  if (!ent) return;
  ['ysl', 'expense_distribution', 'ap_aging', 'maint_proof'].forEach(function (srcType) {
    if (_wizardSelections && _wizardSelections[srcType]) return;
    const files = _spSources.by_source_type[srcType] || [];
    if (files.length !== 1) return;
    const key = 'cb_auto_' + ent + '_' + srcType;
    if (sessionStorage.getItem(key)) return;
    sessionStorage.setItem(key, 'fired');
    const f = files[0];
    // Silent ingest: same endpoint, no alert() popups on success.
    useSharepointFile(srcType, f.item_id, f.name, true);
  });
}

// ─── Phase E: Foundation panel (2026 Approved Budget + 2025 Audit) ─────────
let _foundationStatus = null;

function loadFoundationStatus() {
  const ent = selectedEntity;
  if (!ent) return Promise.resolve();
  return fetch("/api/wizard/" + ent + "/foundation-status")
    .then(function (r) { return r.json(); })
    .then(function (data) {
      _foundationStatus = data;
      renderFoundationPanel();
      applyYardiGate();
      return data;
    })
    .catch(function (err) {
      console.error("foundation-status load failed:", err);
    });
}

function renderFoundationPanel() {
  const panel = document.getElementById("foundationPanel");
  if (!panel) return;
  panel.style.display = "block";
  // Render based on foundation_status alone — _spSources is used only for filename
  // detail and is optional. Avoids a race where SP scan is still loading but
  // foundation-status has returned.
  if (!_foundationStatus) return;

  // ─── Approved Budget card ──
  const apprBody = document.getElementById("foundationApprovedBody");
  const fs = _foundationStatus;
  const sp = (_spSources && _spSources.by_source_type) || {};
  const apprFiles = sp.approved_2026 || [];
  let apprHtml = "";
  if (fs.approved_budget === "imported") {
    apprHtml += "<div style=\"background:#f0fdf4; border:1px solid #bbf7d0; border-radius:6px; padding:10px 12px; margin-bottom:10px;\">";
    apprHtml += "<div style=\"color:#15803d; font-weight:700; font-size:13px;\">\u2713 Imported \u00b7 " + fs.approved_budget_summary_rows + " category rows</div>";
    if (apprFiles.length) {
      apprHtml += "<div style=\"font-family:ui-monospace,monospace; font-size:11px; color:var(--gray-600,#4b5563); margin-top:4px;\">" + escapeHtml(apprFiles[0].name) + "</div>";
    }
    apprHtml += "</div>";
    if (apprFiles.length) {
      apprHtml += "<button data-action=\"foundation-approved\" data-item-id=\"" + escapeHtmlAttr(apprFiles[0].item_id) + "\" data-filename=\"" + escapeHtmlAttr(apprFiles[0].name) + "\" style=\"font-size:12px; padding:6px 12px; border:1px solid #ddd; background:white; border-radius:4px; cursor:pointer;\">\u21bb Re-process</button>";
    }
  } else if (fs.approved_budget === "acknowledged_missing") {
    apprHtml += "<div style=\"background:#f0fdf4; border:1px solid #bbf7d0; border-radius:6px; padding:10px 12px;\">";
    apprHtml += "<div style=\"color:#15803d; font-weight:700; font-size:13px;\">\u2713 Acknowledged \u00b7 No prior budget</div>";
    apprHtml += "<div style=\"font-size:11px; color:var(--gray-600,#4b5563); margin-top:4px;\">Audit mapping will use default Century categories.</div>";
    apprHtml += "</div>";
    // FA dir 2026-05-22: mismatch detector. The acknowledged_missing flag
    // was set by an FA, but if SP actually has an approved_2026 file the
    // build will skip it and budget_summary_rows stays empty (see entity
    // 710 \u2014 0 rows despite the file being present). Offer a one-click fix.
    if (apprFiles.length > 0) {
      apprHtml += "<div style=\"margin-top:10px; background:#fef3c7; border:1px solid #f59e0b; border-radius:6px; padding:10px 12px;\">";
      apprHtml += "<div style=\"font-weight:700; color:#92400e; font-size:13px;\">\u26a0 Mismatch detected</div>";
      apprHtml += "<div style=\"font-size:12px; color:#78350f; margin-top:4px;\">";
      apprHtml += "We found a 2026 budget file in SharePoint (";
      apprHtml += "<span style=\"font-family:ui-monospace,monospace; font-size:11px;\">" + escapeHtml(apprFiles[0].name) + "</span>";
      apprHtml += ") even though this entity is flagged \"no prior budget.\" The file will be skipped at build time unless you clear the flag.";
      apprHtml += "</div>";
      apprHtml += "<button data-action=\"foundation-unack-noprior\" style=\"margin-top:8px; font-size:12px; padding:6px 12px; border:1px solid #b45309; background:#b45309; color:#fff; border-radius:4px; cursor:pointer; font-weight:600;\">Clear flag &amp; use this file</button>";
      apprHtml += "</div>";
    }
  } else if (fs.approved_budget === "in_sp_not_imported") {
    apprHtml += "<div style=\"background:#f9fafb; border:1px solid #e5e7eb; border-radius:6px; padding:10px 12px; margin-bottom:10px;\">";
    apprHtml += "<div style=\"font-family:ui-monospace,monospace; font-size:12px; color:var(--gray-700,#374151);\">\ud83d\udcc4 " + escapeHtml(apprFiles[0] ? apprFiles[0].name : "(file)") + "</div>";
    apprHtml += "<div style=\"font-size:11px; color:var(--gray-500); margin-top:4px;\">Found in SharePoint \u00b7 ready to import</div>";
    apprHtml += "</div>";
    apprHtml += "<button data-action=\"foundation-approved\" data-item-id=\"" + escapeHtmlAttr(apprFiles[0] ? apprFiles[0].item_id : "") + "\" data-filename=\"" + escapeHtmlAttr(apprFiles[0] ? apprFiles[0].name : "") + "\" style=\"font-size:12px; padding:8px 14px; border:1px solid #2563eb; background:#2563eb; color:white; border-radius:4px; cursor:pointer; font-weight:600;\">Process this file</button>";
  } else {
    // missing
    apprHtml += "<div style=\"background:#fef3c7; border:1px solid #fde68a; border-radius:6px; padding:10px 12px; margin-bottom:10px;\">";
    apprHtml += "<div style=\"color:#92400e; font-weight:600; font-size:13px;\">\u26a0 Not found in SharePoint</div>";
    apprHtml += "<div style=\"font-size:11px; color:#92400e; margin-top:4px;\">No 2026 approved budget XLSX detected for this entity.</div>";
    apprHtml += "</div>";
    apprHtml += "<button data-action=\"foundation-ack-noprior\" style=\"font-size:12px; padding:8px 14px; border:1px solid #b45309; background:white; color:#b45309; border-radius:4px; cursor:pointer; font-weight:600;\">Acknowledge: no prior budget</button>";
  }
  apprBody.innerHTML = apprHtml;

  // ─── Audit card ──
  const auditBody = document.getElementById("foundationAuditBody");
  const auditFiles = sp.audit_2025 || [];
  // Audit click is gated by approved_budget being in a complete state
  const apprDone = (fs.approved_budget === "imported" || fs.approved_budget === "acknowledged_missing");
  let auditHtml = "";
  if (fs.audit === "confirmed") {
    auditHtml += "<div style=\"background:#f0fdf4; border:1px solid #bbf7d0; border-radius:6px; padding:10px 12px; margin-bottom:10px;\">";
    auditHtml += "<div style=\"color:#15803d; font-weight:700; font-size:13px;\">\u2713 Confirmed</div>";
    auditHtml += "<div style=\"font-size:11px; color:var(--gray-600,#4b5563); margin-top:4px;\">Foundation locked in. Mapping signed off.</div>";
    auditHtml += "</div>";
    if (fs.review_url) {
      auditHtml += "<a href=\"" + fs.review_url + "\" target=\"_blank\" rel=\"noopener\" style=\"font-size:12px; padding:6px 12px; border:1px solid #ddd; background:white; border-radius:4px; cursor:pointer; text-decoration:none; color:var(--gray-700,#374151); display:inline-block;\">\u21d7 View mapping</a>";
    }
  } else if (fs.audit === "extracted") {
    auditHtml += "<div style=\"background:#fef3c7; border:1px solid #fde68a; border-radius:6px; padding:10px 12px; margin-bottom:10px;\">";
    auditHtml += "<div style=\"color:#b45309; font-weight:700; font-size:13px;\">\u21d7 Extracted, awaiting mapping confirm</div>";
    auditHtml += "<div style=\"font-size:11px; color:#92400e; margin-top:4px;\">Open the review page to assign auditor profile and confirm.</div>";
    auditHtml += "</div>";
    auditHtml += "<a href=\"" + (fs.review_url || "#") + "\" target=\"_blank\" rel=\"noopener\" style=\"font-size:12px; padding:8px 14px; border:1px solid #2563eb; background:#2563eb; color:white; border-radius:4px; cursor:pointer; font-weight:600; text-decoration:none; display:inline-block;\">Review &amp; Confirm Mapping \u2192</a>";
  } else if (fs.audit === "in_sp") {
    auditHtml += "<div style=\"background:#f9fafb; border:1px solid #e5e7eb; border-radius:6px; padding:10px 12px; margin-bottom:10px;\">";
    auditHtml += "<div style=\"font-family:ui-monospace,monospace; font-size:12px; color:var(--gray-700,#374151);\">\ud83d\udcc4 " + escapeHtml(auditFiles[0] ? auditFiles[0].name : "(file)") + "</div>";
    auditHtml += "<div style=\"font-size:11px; color:var(--gray-500); margin-top:4px;\">Found in SharePoint \u00b7 click to extract via Claude (~30-60s)</div>";
    auditHtml += "</div>";
    if (apprDone) {
      auditHtml += "<button data-action=\"foundation-audit\" data-item-id=\"" + escapeHtmlAttr(auditFiles[0] ? auditFiles[0].item_id : "") + "\" data-filename=\"" + escapeHtmlAttr(auditFiles[0] ? auditFiles[0].name : "") + "\" style=\"font-size:12px; padding:8px 14px; border:1px solid #2563eb; background:#2563eb; color:white; border-radius:4px; cursor:pointer; font-weight:600;\">Process this file</button>";
    } else {
      auditHtml += "<button disabled style=\"font-size:12px; padding:8px 14px; border:1px solid #d1d5db; background:#f3f4f6; color:#9ca3af; border-radius:4px; cursor:not-allowed; font-weight:600;\">Process this file</button>";
      auditHtml += "<div style=\"font-size:11px; color:var(--gray-500); margin-top:6px;\">\ud83d\udd12 Process the 2026 Approved Budget first.</div>";
    }
  } else {
    // missing
    auditHtml += "<div style=\"background:#fef3c7; border:1px solid #fde68a; border-radius:6px; padding:10px 12px;\">";
    auditHtml += "<div style=\"color:#92400e; font-weight:600; font-size:13px;\">\u26a0 Not found in SharePoint</div>";
    auditHtml += "<div style=\"font-size:11px; color:#92400e; margin-top:4px;\">Drop a PDF below or wait for the audit-sync to copy it from the master folder.</div>";
    auditHtml += "</div>";
  }
  auditBody.innerHTML = auditHtml;

  // ─── Status banner ──
  const banner = document.getElementById("foundationStatusBanner");
  if (banner) {
    if (fs.foundation_confirmed_at) {
      banner.innerHTML = '<div style="background:#f0fdf4; border:1px solid #bbf7d0; border-radius:8px; padding:14px 18px; display:flex; align-items:center; gap:14px;"><div style="font-size:20px;">\u2713</div><div style="flex:1;"><div style="font-weight:700; color:#15803d;">Foundation confirmed</div><div style="font-size:12px; color:var(--gray-600,#4b5563); margin-top:2px;">Yardi sources, assumptions, and Build Budget are now unlocked. You can come back any time before the cycle starts.</div></div></div>';
    } else if (fs.blocking_reason) {
      banner.innerHTML = '<div style="background:#fef3c7; border:1px solid #fde68a; border-radius:8px; padding:12px 16px; font-size:13px; color:#92400e;"><strong>Foundation pending \u00b7</strong> ' + escapeHtml(fs.blocking_reason) + '</div>';
    } else {
      banner.innerHTML = "";
    }
  }

  // Attach event handlers via delegation (no inline handlers — apostrophe escapes are tricky)
  panel.querySelectorAll('button[data-action="foundation-approved"]').forEach(function (btn) {
    btn.addEventListener("click", function () {
      useSharepointFile("approved_2026", btn.getAttribute("data-item-id"), btn.getAttribute("data-filename"));
    });
  });
  panel.querySelectorAll('button[data-action="foundation-audit"]').forEach(function (btn) {
    btn.addEventListener("click", function () {
      useSharepointFile("audit_2025", btn.getAttribute("data-item-id"), btn.getAttribute("data-filename"));
    });
  });
  panel.querySelectorAll('button[data-action="foundation-ack-noprior"]').forEach(function (btn) {
    btn.addEventListener("click", acknowledgeNoPriorBudget);
  });
  // FA dir 2026-05-22: clear-flag handler for the mismatch banner.
  panel.querySelectorAll('button[data-action="foundation-unack-noprior"]').forEach(function (btn) {
    btn.addEventListener("click", clearNoPriorBudgetFlag);
  });
}

function clearNoPriorBudgetFlag() {
  if (!selectedEntity) return;
  if (!confirm("Clear the no-prior-budget flag for this entity? The 2026 Approved Budget file in SharePoint will then be used during the build.")) return;
  fetch("/api/wizard/" + selectedEntity + "/acknowledge-no-prior-budget", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({acknowledged: false}),
  })
    .then(function (r) { return r.json(); })
    .then(function (data) {
      if (data.ok) {
        // Re-render the foundation panel so the banner disappears and the
        // approved_2026 file shows up in its "ready to import" state.
        loadFoundationStatus();
      } else {
        alert("Could not clear flag: " + (data.error || "unknown"));
      }
    })
    .catch(function (err) { alert("Request failed: " + err); });
}

function acknowledgeNoPriorBudget() {
  if (!selectedEntity) return;
  if (!confirm("Acknowledge that no 2026 Approved Budget exists for this entity? The audit mapping will use default Century categories.")) return;
  fetch("/api/wizard/" + selectedEntity + "/acknowledge-no-prior-budget", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({acknowledged: true}),
  })
    .then(function (r) { return r.json(); })
    .then(function (data) {
      if (data.ok) {
        loadFoundationStatus();
      } else {
        alert("Acknowledgment failed: " + (data.error || "unknown"));
      }
    });
}

function applyYardiGate() {
  // Visually lock the Yardi panel until Foundation is confirmed.
  const sp = document.getElementById("spSourcesPanel");
  if (!sp || !_foundationStatus) return;
  if (_foundationStatus.foundation_confirmed_at) {
    sp.style.opacity = "1";
    sp.style.pointerEvents = "auto";
    // Remove any lock overlay
    const overlay = document.getElementById("yardiLockOverlay");
    if (overlay) overlay.remove();
  } else {
    sp.style.opacity = "0.55";
    sp.style.pointerEvents = "none";
    if (!document.getElementById("yardiLockOverlay")) {
      const ov = document.createElement("div");
      ov.id = "yardiLockOverlay";
      ov.style.cssText = "background:#f9fafb; border:1px dashed #d1d5db; border-radius:8px; padding:10px 14px; margin-top:8px; font-size:12px; color:#6b7280; pointer-events:none;";
      ov.innerHTML = "\ud83d\udd12 Yardi sources unlock once Foundation is confirmed above.";
      sp.parentElement.insertBefore(ov, sp);
    }
  }
}

function escapeHtml(s) {
  if (!s) return "";
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/\"/g, "&quot;");
}

function escapeHtmlAttr(s) {
  return String(s || "").replace(/'/g, "&apos;").replace(/"/g, "&quot;");
}

function useSharepointFile(sourceType, itemId, filename, auto) {
  // `auto`: when true, suppress alert() popups for success and failure \u2014
  // used by _maybeAutoIngestSources() so the FA doesn't get hit with
  // confirm() prompts every time wizard step 2 loads. Errors still surface
  // in the console + via the button reverting to "Ingest now".
  const ent = selectedEntity;
  if (!ent) return;
  // Find the clicked button and disable it with a working indicator.
  const btn = document.querySelector('button[data-action="select-yardi"][data-item-id="' + itemId.replace(/"/g, '\\\"') + '"]');
  let originalText = "Ingest now";
  if (btn) {
    originalText = btn.textContent;
    btn.disabled = true;
    btn.textContent = "Working...";
    btn.style.opacity = "0.6";
  }
  // Look up the file's SharePoint web_url from the cached SP scan so we can
  // pass it through to the backend (used for the audit review page's
  // "Open audit PDF" link, which deep-links into SharePoint instead of
  // streaming bytes through this app).
  let webUrl = "";
  try {
    if (_spSources && _spSources.by_source_type && _spSources.by_source_type[sourceType]) {
      const match = _spSources.by_source_type[sourceType].find(f => f.item_id === itemId);
      if (match && match.web_url) webUrl = match.web_url;
    }
  } catch (e) {}
  fetch("/api/wizard/" + ent + "/use-sp-source", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ source_type: sourceType, item_id: itemId, filename: filename || "", web_url: webUrl })
  })
    .then(function (r) { return r.json().then(function (j) { return { status: r.status, body: j }; }); })
    .then(function (resp) {
      const data = resp.body;
      if (data.ok) {
        if (data.selections) { _wizardSelections = data.selections; }
        loadFoundationStatus();
        if (data.parse_result && !auto) {
          const pr = data.parse_result;
          let msg;
          if (pr.source_type === "audit_2025") {
            msg = "\u2713 Audit extracted: " + (pr.revenue_lines || 0) + " revenue + " + (pr.expense_lines || 0) + " expense lines.\n\nNext: open the review page to assign auditor profile + confirm mapping:\n" + pr.review_url;
            if (confirm(msg + "\n\nOpen review page now?")) {
              window.open(pr.review_url, "_blank");
            }
          } else {
            msg = "\u2713 Imported " + (pr.rows_imported || 0) + " rows from " + (pr.filename || "file") + ".";
            alert(msg);
          }
        }
        // For auto-ingests, console-log success quietly. Re-render the
        // sources panel so the row flips from "auto-ingesting" \u2192 "ingested".
        if (auto && data.parse_result) {
          try { console.info("[A2] Auto-ingested " + sourceType + ": ", data.parse_result); } catch (e) {}
        }
        loadSharepointSources();
      } else {
        const err = data.parse_error || data.error || ("HTTP " + resp.status);
        if (!auto) {
          alert("Click failed: " + err);
        } else {
          // Auto-ingest failed \u2014 log + leave the session marker so we don't
          // retry in a loop. FA can click "Ingest now" manually to retry.
          try { console.warn("[A2] Auto-ingest failed for " + sourceType + ": " + err); } catch (e) {}
        }
        if (btn) {
          btn.disabled = false;
          btn.textContent = originalText;
          btn.style.opacity = "1";
        }
        // Re-render so the row flips back to "Ready" state with manual button.
        try { renderSharepointSources(); } catch (e) {}
      }
    })
    .catch(function (err) {
      if (!auto) {
        alert("Request failed: " + err);
      } else {
        try { console.warn("[A2] Auto-ingest network failure for " + sourceType + ": " + err); } catch (e) {}
      }
      if (btn) {
        btn.disabled = false;
        btn.textContent = originalText;
        btn.style.opacity = "1";
      }
      try { renderSharepointSources(); } catch (e) {}
    });
}

// Render upload checklist
function renderUploadChecklist() { /* no-op since the checklist UI was removed in D6; SP panel is the source of truth */ }

// Wizard staged assumptions — populated by renderWizardAssumptionsForm,
// updated by every onchange handler before POSTing.
let wizardStagedAssumptions = {};
let _wizardAssumpSaveTimer = null;

// Render the wizard\'s editable assumptions form. CFO defaults come from
// `portfolio` (server-rendered). Staged FA edits are loaded from the wizard
// selections endpoint and merged on top. Auto-saves on change.
function renderWizardAssumptionsForm() {
  const host = document.getElementById('wizardAssumptionsForm');
  if (!host) return;
  host.innerHTML = '<div style="padding:20px; color:var(--gray-500);">Loading assumptions...</div>';

  // Pull any staged values from selections (overrides over CFO defaults)
  fetch('/api/wizard/' + selectedEntity + '/selections')
    .then(r => r.json())
    .then(data => {
      const staged = (data && data.selections && data.selections.assumptions) || {};
      wizardStagedAssumptions = staged;
      // Deep-merge: CFO defaults under, staged on top
      const merged = JSON.parse(JSON.stringify(portfolio || {}));
      Object.keys(staged).forEach(k => {
        if (typeof staged[k] === 'object' && staged[k] !== null && typeof merged[k] === 'object' && merged[k] !== null) {
          merged[k] = Object.assign({}, merged[k], staged[k]);
        } else {
          merged[k] = staged[k];
        }
      });
      _renderWizardAssumpHTML(host, merged);
    })
    .catch(err => {
      console.error('Failed to load staged assumptions, using CFO defaults only:', err);
      wizardStagedAssumptions = {};
      _renderWizardAssumpHTML(host, JSON.parse(JSON.stringify(portfolio || {})));
    });
}

function _renderWizardAssumpHTML(host, a) {
  const pt = a.payroll_tax || {};
  const ub = a.union_benefits || {};
  const wc = a.workers_comp || {};
  const wi = a.wage_increase || {};
  const ir = a.insurance_renewal || {};
  const en = a.energy || {};
  const ws = a.water_sewer || {};
  // Budget Period (Phase F1: drives YTD/forecast formulas across the whole budget)
  // Stored as "MM/YYYY" e.g. "04/2026" = actuals through April of the prior year.
  const bp = a.budget_period || '';
  let bpMonth = 0; // 0 = unset
  if (bp && bp.indexOf('/') > 0) {
    const mm = parseInt(bp.split('/')[0], 10);
    if (!isNaN(mm) && mm >= 1 && mm <= 12) bpMonth = mm;
  }

  // Inject scoped style once
  if (!document.getElementById('wiz-asm-style')) {
    const st = document.createElement('style');
    st.id = 'wiz-asm-style';
    st.textContent =
      '.wiz-asm { padding: 4px 0 12px; font-variant-numeric: tabular-nums; }' +
      '.wiz-asm .wa-section { margin-bottom: 24px; }' +
      '.wiz-asm .wa-section-title { font-size: 11px; font-weight: 700; letter-spacing: 0.12em; text-transform: uppercase; color: var(--gray-700); margin-bottom: 10px; }' +
      '.wiz-asm .wa-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 14px; align-items: start; }' +
      '.wiz-asm .wa-card { background: #fff; border-radius: 10px; border: 1px solid var(--gray-200); padding: 16px 20px 14px; }' +
      '.wiz-asm .wa-card-title { font-size: 11px; font-weight: 700; letter-spacing: 0.09em; text-transform: uppercase; color: var(--gray-700); margin: 0 0 10px; }' +
      '.wiz-asm .wa-row { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 5px 0; }' +
      '.wiz-asm .wa-row label { font-size: 13px; color: var(--gray-700); font-weight: 400; flex: 1 1 auto; }' +
      '.wiz-asm .wa-input-wrap { display: inline-flex; align-items: center; gap: 6px; }' +
      '.wiz-asm .wa-unit { font-size: 11px; color: var(--gray-500); min-width: 8px; }' +
      '.wiz-asm .wa-input { width: 100px; padding: 6px 10px; border: 1px solid var(--gray-200); border-radius: 6px; font-size: 13px; text-align: right; background: #fff; font-variant-numeric: tabular-nums; -moz-appearance: textfield; }' +
      '.wiz-asm .wa-input::-webkit-outer-spin-button, .wiz-asm .wa-input::-webkit-inner-spin-button { -webkit-appearance: none; margin: 0; }' +
      '.wiz-asm .wa-input:focus { outline: none; border-color: var(--brown-700, #5a4a3f); box-shadow: 0 0 0 3px rgba(90, 74, 63, 0.12); }' +
      '.wiz-asm .wa-sub { font-size: 10px; font-weight: 700; letter-spacing: 0.10em; text-transform: uppercase; color: var(--gray-500); padding: 8px 0 2px; }' +
      '.wiz-asm .wa-card .wa-sub:first-child { padding-top: 0; }';
    document.head.appendChild(st);
  }

  function pctVal(v) { return (v || v === 0) ? (v * 100).toFixed(2) : '0'; }
  function numVal(v) { return (v || v === 0) ? v : 0; }

  function pctF(section, key, val) {
    return '<div class="wa-input-wrap"><input class="wa-input" type="number" step="any" value="' + pctVal(val) +
      '" data-section="' + section + '" data-key="' + key + '" data-kind="pct"><span class="wa-unit">%</span></div>';
  }
  function numF(section, key, val, unit) {
    return '<div class="wa-input-wrap"><input class="wa-input" type="number" step="any" value="' + numVal(val) +
      '" data-section="' + section + '" data-key="' + key + '" data-kind="num"><span class="wa-unit">' + (unit || '') + '</span></div>';
  }
  function txtF(section, key, val) {
    return '<div class="wa-input-wrap"><input class="wa-input" type="text" value="' + (val || '') +
      '" data-section="' + section + '" data-key="' + key + '" data-kind="txt"><span class="wa-unit"></span></div>';
  }
  function row(label, inp) { return '<div class="wa-row"><label>' + label + '</label>' + inp + '</div>'; }
  function sub(label) { return '<div class="wa-sub">' + label + '</div>'; }
  function card(title, body) { return '<div class="wa-card"><h3 class="wa-card-title">' + title + '</h3>' + body + '</div>'; }
  function section(title, cards) { return '<div class="wa-section"><div class="wa-section-title">' + title + '</div><div class="wa-grid">' + cards + '</div></div>'; }

  const payrollTax = card('Payroll Tax Rates',
    row('FICA', pctF('payroll_tax','FICA', pt.FICA)) +
    row('SUI', pctF('payroll_tax','SUI', pt.SUI)) +
    row('FUI', pctF('payroll_tax','FUI', pt.FUI)) +
    row('MTA', pctF('payroll_tax','MTA', pt.MTA)) +
    row('NYS Disability', pctF('payroll_tax','NYS_Disability', pt.NYS_Disability)) +
    row('PFL', pctF('payroll_tax','PFL', pt.PFL))
  );
  const union = card('Union Benefits · 32BJ',
    row('Welfare · $/mo/man', numF('union_benefits','welfare_monthly', ub.welfare_monthly, '$')) +
    row('Pension · $/wk/man', numF('union_benefits','pension_weekly', ub.pension_weekly, '$')) +
    row('Supp Retirement · $/wk', numF('union_benefits','supp_retirement_weekly', ub.supp_retirement_weekly, '$')) +
    row('Legal · $/mo', numF('union_benefits','legal_monthly', ub.legal_monthly, '$')) +
    row('Training · $/mo', numF('union_benefits','training_monthly', ub.training_monthly, '$')) +
    row('Profit Sharing · $/qtr', numF('union_benefits','profit_sharing_quarterly', ub.profit_sharing_quarterly, '$'))
  );
  const wcWi = card('Workers Comp & Wage Increase',
    sub('Workers Comp') +
    row('Workers Comp', pctF('workers_comp','percent', wc.percent)) +
    sub('Wage Increase') +
    row('Wage Increase', pctF('wage_increase','percent', wi.percent)) +
    row('Effective Week', txtF('wage_increase','effective_week', wi.effective_week || 'Wk 16')) +
    row('Pre-Increase Weeks', numF('wage_increase','pre_increase_weeks', wi.pre_increase_weeks, '')) +
    row('Post-Increase Weeks', numF('wage_increase','post_increase_weeks', wi.post_increase_weeks, ''))
  );
  const ins = card('Insurance Renewal',
    row('Renewal Increase', pctF('insurance_renewal','increase_percent', ir.increase_percent)) +
    row('Effective Date', txtF('insurance_renewal','effective_date', ir.effective_date || 'Mar 2027')) +
    row('Pre-Renewal Months', numF('insurance_renewal','pre_renewal_months', ir.pre_renewal_months, '')) +
    row('Post-Renewal Months', numF('insurance_renewal','post_renewal_months', ir.post_renewal_months, ''))
  );
  const energy = card('Energy Rates',
    sub('Gas') +
    row('ESCO Rate · $/Therm', numF('energy','gas_esco_rate', en.gas_esco_rate, '$')) +
    row('Rate Increase', pctF('energy','gas_rate_increase', en.gas_rate_increase)) +
    sub('Electric') +
    row('ESCO Rate · $/KWH', numF('energy','electric_esco_rate', en.electric_esco_rate, '$')) +
    row('Rate Increase', pctF('energy','electric_rate_increase', en.electric_rate_increase)) +
    sub('Oil') +
    row('Price · $/gallon', numF('energy','oil_price_per_gallon', en.oil_price_per_gallon, '$')) +
    row('Rate Increase', pctF('energy','oil_rate_increase', en.oil_rate_increase))
  );
  const water = card('Water & Sewer',
    row('Rate Increase', pctF('water_sewer','rate_increase', ws.rate_increase))
  );

  // Budget Period card — single dropdown for actuals-through month.
  // Drives YTD_MONTHS / REMAINING_MONTHS everywhere (forecast formula, estimate label, Excel export).
  const MONTH_NAMES = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  let bpOpts = '<option value="0"' + (bpMonth === 0 ? ' selected' : '') + '>— Select —</option>';
  for (let i = 1; i <= 12; i++) {
    bpOpts += '<option value="' + i + '"' + (bpMonth === i ? ' selected' : '') + '>' + MONTH_NAMES[i - 1] + '</option>';
  }
  const periodCard =
    '<div class="wa-card" style="border-color:' + (bpMonth === 0 ? '#dc2626' : 'var(--gray-200)') + ';">' +
      '<h3 class="wa-card-title">Budget Period <span style="color:#dc2626;font-weight:700;">*</span></h3>' +
      '<div class="wa-row"><label>Actuals through (last completed YSL month)</label>' +
        '<div class="wa-input-wrap"><select class="wa-input" id="wizBudgetPeriodSelect" data-kind="period" style="width:120px;text-align:left;padding-left:10px;">' +
          bpOpts +
        '</select></div>' +
      '</div>' +
      '<div style="font-size:12px;color:var(--gray-500);margin-top:6px;">' +
        (bpMonth === 0
          ? 'Required. The period your YSL/YTD actuals cover. Drives the forecast formula for all expense and income lines.'
          : 'Actuals: Jan–' + MONTH_NAMES[bpMonth - 1] + ' · Estimate: ' + (bpMonth < 12 ? MONTH_NAMES[bpMonth] + '–Dec' : 'none (full year actuals)')) +
      '</div>' +
    '</div>';

  host.innerHTML = '<div class="wiz-asm">' +
    section('Budget Period', periodCard) +
    section('Payroll', payrollTax + union + wcWi) +
    section('Operating', ins + energy + water) +
    '</div>';

  // Wire period dropdown
  const bpSel = document.getElementById('wizBudgetPeriodSelect');
  if (bpSel) {
    bpSel.addEventListener('change', () => {
      const mm = parseInt(bpSel.value, 10) || 0;
      _stageWizardPeriod(mm);
    });
  }

  // Wire onchange handlers to auto-save staged values
  host.querySelectorAll('.wa-input').forEach(inp => {
    inp.addEventListener('change', () => {
      const sectionKey = inp.dataset.section;
      const fieldKey = inp.dataset.key;
      const kind = inp.dataset.kind;
      let value;
      if (kind === 'pct') {
        value = (parseFloat(inp.value) || 0) / 100;
      } else if (kind === 'num') {
        value = parseFloat(inp.value) || 0;
      } else {
        value = inp.value;
      }
      _stageWizardAssumption(sectionKey, fieldKey, value);
    });
  });
}

// Save a top-level (non-nested) wizard assumption value, e.g. budget_period.
// The submission endpoint deep-merges; non-dict values land at the top level
// of the staged assumptions object as expected.
function _stageWizardPeriod(monthNum) {
  const status = document.getElementById('wizardAssumpStatus');
  if (status) status.textContent = 'Saving...';
  // Compose MM/YYYY where YYYY is the prior calendar year (YSL covers prior year actuals).
  // Empty string when unset so dashboard can show the warning banner.
  let value = '';
  if (monthNum && monthNum >= 1 && monthNum <= 12) {
    const mm = String(monthNum).padStart(2, '0');
    // Year portion: BUDGET_YEAR - 1 (YSL actuals year). Read code only parses the
    // month, but storing the right year makes audit/log entries readable.
    const yyyy = BUDGET_YEAR - 1;
    value = mm + '/' + yyyy;
  }
  fetch('/api/wizard/' + selectedEntity + '/selections/assumptions', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({budget_period: value})
  })
    .then(r => r.json())
    .then(data => {
      if (data && data.ok) {
        wizardStagedAssumptions = data.assumptions || {};
        if (status) status.textContent = 'Saved';
        // Re-render the form so the helper text under the dropdown updates
        renderWizardAssumptionsForm();
        setTimeout(() => { if (status) status.textContent = ''; }, 1500);
      } else {
        if (status) status.textContent = 'Save failed';
      }
    })
    .catch(() => { if (status) status.textContent = 'Save failed'; });
}

function _stageWizardAssumption(section, field, value) {
  // Bail when section/field aren't real strings — happens when an event
  // fires on an element without data-section/data-key attributes (e.g.,
  // the period dropdown, which has its own dedicated handler). Without
  // this guard, the payload becomes {"undefined": {"undefined": value}}
  // and pollutes the staged assumptions JSON forever.
  if (!section || section === 'undefined' || !field || field === 'undefined') {
    return;
  }
  const status = document.getElementById('wizardAssumpStatus');
  if (status) status.textContent = 'Saving...';
  clearTimeout(_wizardAssumpSaveTimer);
  _wizardAssumpSaveTimer = setTimeout(() => {
    const payload = {};
    payload[section] = {};
    payload[section][field] = value;
    fetch('/api/wizard/' + selectedEntity + '/selections/assumptions', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload)
    })
      .then(r => r.json())
      .then(data => {
        if (data && data.ok) {
          wizardStagedAssumptions = data.assumptions || {};
          if (status) status.textContent = 'Saved';
          setTimeout(() => { if (status) status.textContent = ''; }, 1500);
        } else {
          if (status) status.textContent = 'Save failed';
        }
      })
      .catch(() => { if (status) status.textContent = 'Save failed'; });
  }, 500);
}

// Render preview table — fetches live data from API
function renderPreviewTable() {
  const tbody = document.getElementById('previewTableBody');
  tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;padding:20px;color:var(--gray-500);">Loading preview...</td></tr>';

  if (!selectedEntity) return;

  fetch(`/api/wizard/${selectedEntity}/preview`)
    .then(r => r.json())
    .then(data => {
      tbody.innerHTML = '';
      if (!data.success || !data.preview) {
        tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;color:var(--red);">Could not load preview. Upload YSL first.</td></tr>';
        return;
      }
      data.preview.forEach(cat => {
        const delta = cat.delta || 0;
        const deltaPercent = cat.delta_pct || 0;
        const deltaClass = delta > 0 ? 'preview-increase' : (delta < 0 ? 'preview-decrease' : 'preview-neutral');
        const sign = delta >= 0 ? '+' : '';

        const row = document.createElement('tr');
        row.innerHTML = `
          <td class="preview-category">${cat.category}</td>
          <td class="preview-value">$${Math.round(cat.raw).toLocaleString()}</td>
          <td class="preview-value">$${Math.round(cat.adjusted).toLocaleString()}</td>
          <td class="preview-value ${deltaClass}">${sign}$${Math.round(Math.abs(delta)).toLocaleString()} (${deltaPercent}%)</td>
        `;
        tbody.appendChild(row);
      });
    })
    .catch(err => {
      tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;color:var(--red);">Error loading preview</td></tr>';
      console.error('Preview fetch error:', err);
    });
}

// Show step
// FA dir 2026-05-22 (L3 nav clarity): keep the browser tab title + step
// header in sync with the current step and entity, and surface a one-click
// "← FA Dashboard" breadcrumb so the FA can jump back without using the
// browser back button (which on the wizard sometimes lands them mid-step).
function _updateWizardChrome(stepNum) {
  const stepNames = {
    1: 'Select Entity',
    2: 'Foundation',
    3: 'Set Assumptions',
    4: 'Preview & Generate',
    5: 'Budget Complete',
  };
  const stepName = stepNames[stepNum] || '';
  const railEnt = document.getElementById('railEntityName');
  const rawName = railEnt ? (railEnt.textContent || '').trim() : '';
  const entName = (rawName && rawName !== 'No Entity Selected' && rawName !== '—') ? rawName : '';
  const entCode = (typeof selectedEntity === 'string' && selectedEntity) ? selectedEntity : '';
  // Document <title> — shows up in the browser tab + back-history. The old
  // static "Budget Wizard - Century Management" gave the FA no way to tell
  // which entity or step a tab/history entry corresponded to.
  if (entName && stepName) {
    document.title = 'Step ' + stepNum + ': ' + stepName + ' — ' + entName + ' (' + entCode + ') · Century Budget';
  } else if (stepName) {
    document.title = 'Step ' + stepNum + ': ' + stepName + ' · Century Budget';
  } else {
    document.title = 'Budget Wizard - Century Management';
  }
  // Breadcrumb: insert once per step-header (idempotent). Sits above the
  // step badge so it's the first thing the eye lands on.
  document.querySelectorAll('.step-content .step-header').forEach(function (hdr) {
    if (hdr.querySelector('.wiz-crumb')) return;
    const crumb = document.createElement('div');
    crumb.className = 'wiz-crumb';
    crumb.style.cssText = 'font-size:12px; color:var(--gray-500); margin-bottom:8px;';
    crumb.innerHTML =
      '<a href="/dashboard" style="color:var(--blue); text-decoration:none; font-weight:600;">← Back to FA Dashboard</a>';
    hdr.insertBefore(crumb, hdr.firstChild);
  });
  // Append entity context onto the visible step's h1. Idempotent — strip
  // any prior " — ..." suffix before re-applying, so step transitions
  // don't double-stack names.
  const activeH1 = document.querySelector('.step-content.active .step-title');
  if (activeH1) {
    const base = stepNames[stepNum] || activeH1.textContent.split('—')[0].trim();
    if (entName && stepNum >= 2) {
      activeH1.textContent = base + ' — ' + entName + ' (' + entCode + ')';
    } else {
      activeH1.textContent = base;
    }
  }
}

function showStep(stepNum) {
  if (stepNum > highestStep && stepNum !== currentStep + 1) {
    alert('You must complete previous steps first');
    return;
  }

  // Phase E gate: Steps 3+ require Foundation confirmed.
  if (stepNum >= 3 && _foundationStatus && !_foundationStatus.foundation_confirmed_at) {
    alert("Foundation must be confirmed before continuing.\n\n" +
          (_foundationStatus.blocking_reason || "Process the 2026 Approved Budget and the 2025 Audit, then confirm the audit mapping."));
    // Stay at Step 2
    stepNum = 2;
  }

  currentStep = stepNum;

  // Hide all steps
  document.querySelectorAll('.step-content').forEach(el => el.classList.remove('active'));

  // Show current step (target .step-content specifically, not rail items)
  const content = document.querySelector(`.step-content[data-step="${stepNum}"]`);
  if (content) content.classList.add('active');

  // Update rail
  updateRail();

  // Render step-specific content
  if (stepNum === 2) {
    // Refresh Step 2 state when navigating in
    if (selectedEntity) {
      try { loadSharepointSources(); } catch (e) {}
      try { loadFoundationStatus(); } catch (e) {}
    }
  }
  if (stepNum === 3) renderWizardAssumptionsForm();
  if (stepNum === 4) renderPreviewTable();

  // Update action buttons
  renderActionButtons();

  // Sync browser title + step-header chrome with current step + entity.
  try { _updateWizardChrome(stepNum); } catch (e) {}
}

// Complete step and advance
function completeStep(stepNum) {
  if (stepNum >= highestStep) {
    highestStep = stepNum + 1;
  }

  // Persist step progress to server
  if (selectedEntity) {
    fetch(`/api/wizard/${selectedEntity}/step`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({step: stepNum})
    }).catch(err => console.error('Step save error:', err));
  }

  // Phase E gate: completeStep(2) advancing to Step 3 requires Foundation confirmed.
  if (stepNum === 2 && _foundationStatus && !_foundationStatus.foundation_confirmed_at) {
    alert("Foundation must be confirmed before continuing.\n\n" +
          (_foundationStatus.blocking_reason || "Process the 2026 Approved Budget and confirm the 2025 Audit mapping."));
    return;
  }

  if (stepNum < 5) {
    showStep(stepNum + 1);
  }
}

// Update rail UI
function updateRail() {
  // Phase E gate: when Foundation not confirmed, force-lock Steps 3-5
  const foundationGate = (_foundationStatus && !_foundationStatus.foundation_confirmed_at);
  document.querySelectorAll('.rail-step').forEach(el => {
    const step = parseInt(el.dataset.step);
    el.classList.remove('active', 'locked');

    if (step === currentStep) {
      el.classList.add('active');
    } else if (step > highestStep) {
      el.classList.add('locked');
    }
    // Foundation gate overrides — Steps 3-5 always locked until Foundation confirmed
    if (foundationGate && step >= 3) {
      el.classList.add('locked');
    }

    const circle = el.querySelector('.step-circle');
    circle.classList.remove('active', 'done', 'locked');

    if (step < highestStep) {
      circle.classList.add('done');
      circle.textContent = '✓';
    } else if (step === currentStep) {
      circle.classList.add('active');
      circle.textContent = step;
    } else {
      circle.classList.add('locked');
      circle.textContent = step;
    }
  });
}

// Render action buttons based on current step
function renderActionButtons() {
  const container = document.getElementById('actionButtons');
  container.innerHTML = '';

  if (currentStep === 1) {
    const btn = document.createElement('button');
    btn.className = 'btn btn-primary';
    btn.textContent = 'Continue →';
    btn.disabled = !selectedEntity;
    btn.onclick = () => completeStep(1);
    container.appendChild(btn);
  } else if (currentStep === 2) {
    const btn1 = document.createElement('button');
    btn1.className = 'btn btn-secondary';
    btn1.textContent = '← Back';
    btn1.onclick = () => showStep(1);
    container.appendChild(btn1);

    const btn2 = document.createElement('button');
    btn2.className = 'btn btn-primary';
    btn2.textContent = 'Continue →';
    btn2.onclick = () => completeStep(2);
    container.appendChild(btn2);
  } else if (currentStep === 3) {
    const btn1 = document.createElement('button');
    btn1.className = 'btn btn-secondary';
    btn1.textContent = '← Back';
    btn1.onclick = () => showStep(2);
    container.appendChild(btn1);

    const btn3 = document.createElement('button');
    btn3.className = 'btn btn-primary';
    btn3.textContent = 'Continue →';
    btn3.onclick = () => completeStep(3);
    container.appendChild(btn3);
  } else if (currentStep === 4) {
    const btn1 = document.createElement('button');
    btn1.className = 'btn btn-secondary';
    btn1.textContent = '← Adjust assumptions';
    btn1.onclick = () => showStep(3);
    container.appendChild(btn1);

    const btn2 = document.createElement('button');
    btn2.className = 'btn btn-primary';
    btn2.textContent = 'Generate budget →';
    btn2.onclick = () => generateBudget();
    container.appendChild(btn2);
  } else if (currentStep === 5) {
    // Two CTAs: open the FA Dashboard for line edits, or send straight to
    // PM review. Both use the existing /api/budgets/<entity>/status endpoint
    // for the PM transition, so behavior matches the dashboard's "Send to PM"
    // button at workflow.py:5217 (status: draft → pm_pending).
    const btn1 = document.createElement('button');
    btn1.className = 'btn btn-secondary';
    btn1.textContent = '← Open Dashboard';
    btn1.onclick = () => openDashboard();
    container.appendChild(btn1);

    const btn2 = document.createElement('button');
    btn2.className = 'btn btn-primary';
    btn2.textContent = 'Send to PM →';
    btn2.onclick = () => sendToPM();
    container.appendChild(btn2);
  }
}

// Send the budget to PM review — mirrors the FA Dashboard's "Send to PM"
// button. POSTs to /api/budgets/<entity>/status with status="pm_pending".
function sendToPM() {
  if (!selectedEntity) { alert('No entity selected'); return; }
  if (!confirm('Send this budget to PM for review? You can still re-open it from the FA Dashboard later.')) return;
  fetch(`/api/budgets/${selectedEntity}/status`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ status: 'pm_pending' })
  })
  .then(r => r.json())
  .then(data => {
    if (data && (data.success || data.status === 'pm_pending')) {
      const sd = document.getElementById('successDetails');
      if (sd) sd.textContent = 'Sent to PM. The PM will see this in their portal at /pm/' + selectedEntity + '.';
      // Replace the action buttons with a single "Open PM Portal" CTA.
      const container = document.getElementById('actionButtons');
      if (container) {
        container.innerHTML = '';
        const btn = document.createElement('button');
        btn.className = 'btn btn-primary btn-full';
        btn.textContent = 'Open PM Portal →';
        btn.onclick = () => { window.location.href = '/pm/' + selectedEntity; };
        container.appendChild(btn);
      }
    } else {
      alert('Could not change status: ' + ((data && data.error) || 'unknown error'));
    }
  })
  .catch(err => { console.error('Send to PM error:', err); alert('Send to PM error: ' + err.message); });
}

// Handle file upload
function handleFileUpload(event) {
  const file = event.target.files[0];
  if (!file) return;
  if (!selectedEntity) {
    alert("Pick an entity first.");
    return;
  }
  const status = document.getElementById("uploadStatus");
  if (status) status.textContent = "Uploading " + file.name + " to SharePoint...";

  const formData = new FormData();
  formData.append("file", file);

  fetch("/api/wizard/" + selectedEntity + "/upload-to-sp", {
    method: "POST",
    body: formData,
  })
    .then(function (r) { return r.json().then(function (j) { return {status: r.status, body: j}; }); })
    .then(function (resp) {
      const data = resp.body;
      if (data.ok) {
        if (status) {
          status.textContent = "\u2713 Uploaded to SharePoint as " + (data.classified_as || "unmatched")
                              + ". Refreshing panel...";
          status.style.color = "#15803d";
        }
        // Refresh the FROM SHAREPOINT panel so the new file appears.
        loadSharepointSources();
        loadFoundationStatus();
        // Reset clear after a moment
        setTimeout(function () {
          if (status) {
            status.textContent = (data.note || "") + " Click \"Select for build\" above when ready.";
            status.style.color = "var(--gray-500)";
          }
        }, 1500);
      } else {
        if (status) {
          status.textContent = "Upload failed: " + (data.error || "unknown error");
          status.style.color = "#b45309";
        }
      }
      // Reset file input so re-selecting same file fires change again
      event.target.value = "";
    })
    .catch(function (err) {
      if (status) {
        status.textContent = "Upload error: " + err.message;
        status.style.color = "#b45309";
      }
      event.target.value = "";
    });
}

// Generate budget — POSTs to wizard generate endpoint, then advances to the
// success/Complete step (Step 5). The previous version called completeStep(5)
// which never advanced the UI (its body is `if (stepNum < 5) showStep(...)`),
// so the click looked like a no-op even on success. Also reads the correct
// server response field (`lines_updated`, not `lines_generated`).
function generateBudget() {
  // Phase F1: hard-gate budget generation on the period being set.
  // Without it, the forecast formula silently defaults to 2-month YTD / 10-month
  // estimate and produces wrong numbers across every line.
  const stagedPeriod = (wizardStagedAssumptions && wizardStagedAssumptions.budget_period) || '';
  let stagedMonth = 0;
  if (stagedPeriod && stagedPeriod.indexOf('/') > 0) {
    const mm = parseInt(stagedPeriod.split('/')[0], 10);
    if (!isNaN(mm) && mm >= 1 && mm <= 12) stagedMonth = mm;
  }
  if (!stagedMonth) {
    alert('Please set the Budget Period in Step 3 (Set Assumptions) before generating.\n\nThe period drives the YTD/forecast formula for every line in the budget.');
    showStep(3);
    return;
  }
  try {
    fetch(`/api/wizard/${selectedEntity}/generate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' }
    })
    .then(response => response.json())
    .then(data => {
      if (data.success === false || data.error) {
        alert('Error generating budget: ' + (data.error || 'unknown error'));
        return;
      }
      const linesUpdated = data.lines_updated || 0;
      const totalLines = data.total_lines || 0;
      document.getElementById('successDetails').textContent =
        `${linesUpdated} of ${totalLines} GL lines adjusted by your assumptions. Snapshot saved at ${new Date().toLocaleString()}.`;
      // Persist completion + advance the UI to the success step.
      if (selectedEntity) {
        fetch(`/api/wizard/${selectedEntity}/step`, {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({step: 6})
        }).catch(err => console.error('Step save error:', err));
      }
      if (highestStep < 6) highestStep = 6;
      showStep(5);
    })
    .catch(error => {
      console.error('Generate error:', error);
      alert('Error generating budget: ' + error.message);
    });
  } catch (e) {
    alert('Error: ' + e.message);
  }
}

// Open dashboard
function openDashboard() {
  window.location.href = `/dashboard/${selectedEntity}`;
}

// Admin bypass modal
function showAdminBypassModal() {
  document.getElementById('adminBypassModal').classList.add('active');
}

function hideAdminBypassModal() {
  document.getElementById('adminBypassModal').classList.remove('active');
}

function confirmAdminBypass() {
  hideAdminBypassModal();
  document.getElementById('fileInput').click();
}

// Initialize
document.addEventListener('DOMContentLoaded', () => {
  initializeData();
  // FA dir 2026-05-24: restore readiness-tier filter from URL on initial
  // load so /wizard?tier=audit_extract_ready (the deep-link form back-nav
  // produces) shows the filtered grid, not the default "all" view.
  try {
    const initialTier = new URLSearchParams(window.location.search).get("tier");
    if (initialTier) _stageFilter = initialTier;
  } catch (e) {}
  renderEntityGrid();
  // FA dir 2026-05-23: hydrate the enriched per-entity data (readiness +
  // tiles) from /api/budgets. The grid renders once with placeholder
  // tiles, then re-renders when enriched data lands a fraction of a
  // second later. Non-blocking.
  try { loadEnrichedBudgets(); } catch (e) {}
  updateRail();
  renderActionButtons();
  // Auto-select if /wizard/<entity_code> or ?entity=<code> URL form was used.
  // initializeData sets `selectedEntity` from the template var but does not
  // trigger the entity-pick flow that advances to Step 2. Without this,
  // deep-links (e.g. the post-audit-confirm redirect) land on Step 1 and
  // require a manual click before the wizard moves forward.
  if (selectedEntity) {
    const ent = (budgets || []).find(b => b.entity_code === selectedEntity);
    if (ent) selectEntity(selectedEntity, ent.building_name);
  }
  // Bug #4 deep-link: ?step=N — after entity is selected, jump to step N.
  // Capped to highestStep so the FA can't skip ahead of their progress.
  // Wrapped in setTimeout(0) so it runs after selectEntity's async loads
  // settle and `_foundationStatus` is populated (showStep reads it).
  try {
    const params = new URLSearchParams(window.location.search);
    const stepParam = parseInt(params.get('step') || '', 10);
    if (selectedEntity && stepParam >= 1 && stepParam <= 5) {
      setTimeout(() => {
        if (typeof showStep === 'function') showStep(stepParam);
      }, 250);
    }
  } catch (e) { /* noop */ }
});
</script>

</body>
</html>
"""
