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

<!-- Header -->
<header>
  <div class="header-left">
    <a href="/dashboard" class="header-logo" style="text-decoration:none;color:white;">← Century Budget</a>
    <nav class="header-nav">
      <a href="/dashboard" class="nav-item">Home</a>
      <a class="nav-item active" onclick="showStep(1)">Wizard</a>
      <a class="nav-item locked" onclick="alert(\'Complete the wizard first\')">Dashboard</a>
    </nav>
  </div>
  <div class="header-right">
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

      <!-- FA Selector -->
      <div class="fa-selector-wrapper" id="faSelectorWrapper" style="display:none;">
        <div class="fa-selector-label">Financial Analyst</div>
        <div class="fa-selector-row" style="display:flex; align-items:center; gap:12px;">
          <select id="faSelector" class="fa-select" onchange="filterByFA()" style="flex:1; max-width:none;">
            <option value="">All Entities</option>
          </select>
          <span class="fa-entity-count" id="faEntityCount"></span>
          <span style="flex:1"></span>
          <span class="monday-sync-status" id="mondaySyncStatus" style="font-size:12px; color:#666;">Last synced: —</span>
          <button type="button" id="mondayRefreshBtn" onclick="refreshFromMonday()" style="font-size:12px; padding:5px 12px; border:1px solid #ddd; background:#fff; border-radius:4px; cursor:pointer;" title="Force a fresh pull from Monday.com Building Master List">↻ Refresh from Monday</button>
        </div>
      </div>

      <div class="prompt-banner" id="entityPrompt">
        Each entity has its own budget timeline. Select one to get started.
      </div>
      <!-- Lifecycle stage filter chips -->
      <div class="stage-filter-row" id="stageFilterRow" style="display:flex; gap:8px; flex-wrap:wrap; align-items:center; margin-bottom:14px;">
        <span style="font-size:10px; font-weight:700; color:var(--gray-500); letter-spacing:0.08em; margin-right:6px;">FILTER BY STAGE</span>
        <!-- Populated by JavaScript -->
      </div>
      <div class="entity-search-bar">
        <svg class="search-icon" viewBox="0 0 20 20" fill="currentColor" width="18" height="18"><path fill-rule="evenodd" d="M8 4a4 4 0 100 8 4 4 0 000-8zM2 8a6 6 0 1110.89 3.476l4.817 4.817a1 1 0 01-1.414 1.414l-4.816-4.816A6 6 0 012 8z" clip-rule="evenodd"/></svg>
        <input type="text" id="entitySearch" class="entity-search-input" placeholder="Search by name or entity code..." oninput="renderEntityGrid()">
      </div>
      <div class="entity-table-wrap" style="background:white; border:1px solid var(--gray-200); border-radius:10px; overflow:hidden; box-shadow:0 1px 3px rgba(0,0,0,0.04); margin-bottom:32px;">
        <table class="entity-table" id="entityTable" style="width:100%; border-collapse:collapse; table-layout:fixed;">
          <colgroup>
            <col style="width:38%"/>
            <col style="width:8%"/>
            <col style="width:18%"/>
            <col style="width:14%"/>
            <col style="width:14%"/>
            <col style="width:8%"/>
          </colgroup>
          <thead>
            <tr style="background:var(--gray-50); border-bottom:1px solid var(--gray-200);">
              <th data-col="building_name" onclick="setEntitySort(\'building_name\')" style="padding:10px 14px; text-align:left; font-size:11px; font-weight:700; color:var(--gray-700); letter-spacing:0.04em; cursor:pointer; user-select:none;">Building <span class="sort-arrow" data-arrow="building_name" style="opacity:0.3;">&#9650;</span></th>
              <th data-col="entity_code" onclick="setEntitySort(\'entity_code\')" style="padding:10px 14px; text-align:left; font-size:11px; font-weight:700; color:var(--gray-700); letter-spacing:0.04em; cursor:pointer; user-select:none;">Entity <span class="sort-arrow" data-arrow="entity_code" style="opacity:1;">&#9650;</span></th>
              <th data-col="fa_name" onclick="setEntitySort(\'fa_name\')" style="padding:10px 14px; text-align:left; font-size:11px; font-weight:700; color:var(--gray-700); letter-spacing:0.04em; cursor:pointer; user-select:none;">FA <span class="sort-arrow" data-arrow="fa_name" style="opacity:0.3;">&#9650;</span></th>
              <th data-col="lifecycle_stage" onclick="setEntitySort(\'lifecycle_stage\')" style="padding:10px 14px; text-align:left; font-size:11px; font-weight:700; color:var(--gray-700); letter-spacing:0.04em; cursor:pointer; user-select:none;">Stage <span class="sort-arrow" data-arrow="lifecycle_stage" style="opacity:0.3;">&#9650;</span></th>
              <th data-col="updated_at" onclick="setEntitySort(\'updated_at\')" style="padding:10px 14px; text-align:left; font-size:11px; font-weight:700; color:var(--gray-700); letter-spacing:0.04em; cursor:pointer; user-select:none;">Last activity <span class="sort-arrow" data-arrow="updated_at" style="opacity:0.3;">&#9650;</span></th>
              <th style="padding:10px 14px; text-align:right; font-size:11px; font-weight:700; color:var(--gray-700);"></th>
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
            <div style="font-size:24px;">\ud83d\udcc1</div>
            <div style="text-align:left;">
              <div style="font-weight:600; font-size:14px;">Drop a file or click to browse</div>
              <div style="font-size:12px; color:var(--gray-500); margin-top:2px;">We\'ll route it to SharePoint and refresh the panel above</div>
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
        <p class="step-description">You\'re ready to fine-tune your budget</p>
      </div>
      <div class="prompt-banner">
        You\'re ready! Budget generated with your assumptions. Open the dashboard to fine-tune individual lines.
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
  // Initialize FA selector if we have FA users
  if (faUsers.length > 0) {
    const wrapper = document.getElementById('faSelectorWrapper');
    wrapper.style.display = 'block';
    const sel = document.getElementById('faSelector');
    faUsers.forEach(u => {
      const opt = document.createElement('option');
      opt.value = u.id;
      opt.textContent = u.name;
      sel.appendChild(opt);
    });
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
let _entitySortState = { column: "entity_code", direction: "asc" };
let _stageFilter = "all";
let _faNameByEntity = {};

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
  renderEntityGrid();
}

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

  // 2. Filter by stage chip
  if (_stageFilter && _stageFilter !== "all") {
    filtered = filtered.filter(function (b) { return (b.lifecycle_stage || "Setup") === _stageFilter; });
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

  // 6. Render stage filter chips with live counts (based on FA + search filters, NOT stage filter)
  const stageRow = document.getElementById("stageFilterRow");
  if (stageRow) {
    // Compute counts on the FA-filtered set (so chips reflect what is reachable)
    let chipBase = budgets;
    if (selectedFA) {
      const assigned = new Set(
        faAssignments.filter(function (a) { return String(a.user_id) === selectedFA; })
                     .map(function (a) { return a.entity_code; })
      );
      chipBase = chipBase.filter(function (b) { return assigned.has(b.entity_code); });
    }
    const stages = ["Setup","Sources Collected","Assumptions Confirmed","Budget Built (draft)","PM Review","Approved"];
    const counts = { "all": chipBase.length };
    stages.forEach(function (s) { counts[s] = 0; });
    chipBase.forEach(function (b) { const s = b.lifecycle_stage || "Setup"; if (counts[s] !== undefined) counts[s] += 1; });

    // Keep the leading label, replace any prior chips.
    const headerLabel = stageRow.querySelector("span");
    stageRow.innerHTML = "";
    if (headerLabel) stageRow.appendChild(headerLabel);

    function makeChip(label, count, key) {
      const isActive = _stageFilter === key;
      const chip = document.createElement("button");
      chip.type = "button";
      chip.textContent = label + " " + count;
      chip.style.cssText =
        "font-size:12px; font-weight:" + (isActive ? "700" : "600") +
        "; padding:6px 12px; border-radius:14px; cursor:pointer; border:1px solid " +
        (isActive ? "var(--brown, #5a4a3f)" : "var(--gray-200)") +
        "; background:" + (isActive ? "var(--brown, #5a4a3f)" : "white") +
        "; color:" + (isActive ? "white" : "var(--gray-700)") + ";";
      chip.onclick = function () { setStageFilter(key); };
      return chip;
    }
    stageRow.appendChild(makeChip("All", counts.all, "all"));
    stages.forEach(function (s) {
      stageRow.appendChild(makeChip(s, counts[s], s));
    });
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
    tr.style.cssText = "cursor:pointer; transition:background 0.1s; border-bottom:1px solid var(--gray-100);" +
                       (idx % 2 === 1 ? " background:#fbfaf6;" : "") +
                       (isSelected ? " background:#f5efe7;" : "");
    tr.onmouseenter = function () { if (!isSelected) tr.style.background = "#f4f1eb"; };
    tr.onmouseleave = function () {
      tr.style.background = isSelected ? "#f5efe7" : (idx % 2 === 1 ? "#fbfaf6" : "");
    };
    tr.onclick = function () { selectEntity(b.entity_code, b.building_name || b.entity_code); };

    const stage = b.lifecycle_stage || "Setup";
    const stageBg = _stageColor(stage);
    const faName = _faNameByEntity[b.entity_code] || "—";

    const cell = function (text, css) {
      const td = document.createElement("td");
      td.style.cssText = "padding:10px 14px; font-size:13px; color:var(--gray-900); overflow:hidden; text-overflow:ellipsis; white-space:nowrap;" + (css || "");
      td.textContent = text;
      return td;
    };

    tr.appendChild(cell(b.building_name || b.entity_code, " font-weight:600;"));
    tr.appendChild(cell(b.entity_code, " font-family:ui-monospace,SFMono-Regular,monospace; color:var(--gray-700);"));
    tr.appendChild(cell(faName, " color:var(--gray-700);"));

    const stageTd = document.createElement("td");
    stageTd.style.cssText = "padding:10px 14px;";
    stageTd.innerHTML = "<span style=\"display:inline-block; padding:3px 10px; border-radius:11px; font-size:11px; font-weight:600; background:" + stageBg + "; color:var(--gray-700);\">" + stage + "</span>";
    tr.appendChild(stageTd);

    tr.appendChild(cell(_formatRelativeTime(b.updated_at), " color:var(--gray-500); font-size:12px;"));

    const arrowTd = document.createElement("td");
    arrowTd.style.cssText = "padding:10px 14px; text-align:right; color:var(--gray-300); font-size:14px;";
    arrowTd.textContent = "→";
    tr.appendChild(arrowTd);

    tbody.appendChild(tr);
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
}

// Render upload checklist
// FA file selections (staged but not yet built) for current entity
let _wizardSelections = {};

function loadWizardSelections() {
  const ent = selectedEntity;
  if (!ent) return;
  fetch("/api/wizard/" + ent + "/selections")
    .then(function (r) { return r.json(); })
    .then(function (data) {
      _wizardSelections = data.selections || {};
      // Re-render any panels that show selection state
      try { renderSharepointSources(); } catch (e) {}
    })
    .catch(function () { _wizardSelections = {}; });
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
  let html = "";
  slots.forEach(function (slot) {
    const files = (_spSources.by_source_type[slot.key] || []);
    const hasAny = files.length > 0;
    const headerColor = hasAny ? "#15803d" : "#b45309";
    const headerIcon = hasAny ? "✓" : "↑";
    const headerNote = hasAny ? "found in SharePoint" : "not in folder — use upload below";
    html += "<div style=\"border:1px solid var(--gray-200); border-radius:8px; padding:12px 14px;\">";
    html += "<div style=\"display:flex; align-items:center; gap:10px; margin-bottom:" + (hasAny ? "8px" : "0") + ";\">";
    html += "<span style=\"color:" + headerColor + "; font-weight:700;\">" + headerIcon + "</span>";
    html += "<span style=\"font-weight:600;\">" + slot.label + "</span>";
    html += "<span style=\"flex:1\"></span>";
    html += "<span style=\"font-size:12px; color:" + headerColor + ";\">" + headerNote + "</span>";
    html += "</div>";
    if (hasAny) {
      files.forEach(function (f) {
        html += "<div style=\"display:flex; align-items:center; gap:10px; padding:8px 0; border-top:1px solid var(--gray-100);\">";
        html += "<span style=\"font-family:ui-monospace,monospace; font-size:12px; flex:1; color:var(--text-200,#374151); overflow-wrap:anywhere;\">" + escapeHtml(f.name) + "</span>";
        html += "<span style=\"font-size:11px; color:var(--gray-500); white-space:nowrap;\">" + (f.size ? Math.round(f.size/1024) + " KB" : "") + "</span>";
        if (f.web_url) {
          html += "<a href=\"" + f.web_url + "\" target=\"_blank\" rel=\"noopener\" style=\"font-size:12px; color:var(--blue); text-decoration:none;\">Open in SP ↗</a>";
        }
        html += "<button type=\"button\" data-action=\"select-yardi\" data-source-type=\"" + escapeHtmlAttr(slot.key) + "\" data-item-id=\"" + escapeHtmlAttr(f.item_id) + "\" data-filename=\"" + escapeHtmlAttr(f.name) + "\" style=\"font-size:12px; padding:5px 10px; border:1px solid var(--blue); background:white; color:var(--blue); border-radius:4px; cursor:pointer; font-weight:600;\">Select for build</button>";
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

function useSharepointFile(sourceType, itemId, filename) {
  const ent = selectedEntity;
  if (!ent) return;
  // Find the clicked button and disable it with a working indicator.
  const btn = document.querySelector('button[data-action="select-yardi"][data-item-id="' + itemId.replace(/"/g, '\\\"') + '"]');
  let originalText = "Select for build";
  if (btn) {
    originalText = btn.textContent;
    btn.disabled = true;
    btn.textContent = "Working...";
    btn.style.opacity = "0.6";
  }
  fetch("/api/wizard/" + ent + "/use-sp-source", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ source_type: sourceType, item_id: itemId, filename: filename || "" })
  })
    .then(function (r) { return r.json().then(function (j) { return { status: r.status, body: j }; }); })
    .then(function (resp) {
      const data = resp.body;
      if (data.ok) {
        if (data.selections) { _wizardSelections = data.selections; }
        loadFoundationStatus();
        if (data.parse_result) {
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
        loadSharepointSources();
      } else {
        const err = data.parse_error || data.error || ("HTTP " + resp.status);
        alert("Click failed: " + err);
        if (btn) {
          btn.disabled = false;
          btn.textContent = originalText;
          btn.style.opacity = "1";
        }
      }
    })
    .catch(function (err) {
      alert("Request failed: " + err);
      if (btn) {
        btn.disabled = false;
        btn.textContent = originalText;
        btn.style.opacity = "1";
      }
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

  host.innerHTML = '<div class="wiz-asm">' +
    section('Payroll', payrollTax + union + wcWi) +
    section('Operating', ins + energy + water) +
    '</div>';

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

function _stageWizardAssumption(section, field, value) {
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
    // Fetch fresh source status for selected entity
    if (selectedEntity) {
      fetch('/api/wizard/' + selectedEntity + '/status')
        .then(r => r.json())
        .then(data => {
          if (data.sources) sources = data.sources;
          renderUploadChecklist();
        })
        .catch(() => renderUploadChecklist());
    } else {
      renderUploadChecklist();
    }
  }
  if (stepNum === 3) renderWizardAssumptionsForm();
  if (stepNum === 4) renderPreviewTable();

  // Update action buttons
  renderActionButtons();
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
    const btn = document.createElement('button');
    btn.className = 'btn btn-primary btn-full';
    btn.textContent = 'Open Dashboard →';
    btn.onclick = () => openDashboard();
    container.appendChild(btn);
  }
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

// Generate budget
function generateBudget() {
  try {
    fetch(`/api/wizard/${selectedEntity}/generate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' }
    })
    .then(response => response.json())
    .then(data => {
      if (data.error) {
        alert('Error generating budget: ' + data.error);
      } else {
        document.getElementById('successDetails').textContent =
          `${data.lines_generated || 0} budget lines generated. Snapshot saved at ${new Date().toLocaleString()}.`;
        completeStep(5);
      }
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
  renderEntityGrid();
  updateRail();
  renderActionButtons();
});
</script>

</body>
</html>
"""
