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
          <span>Upload Sources</span>
        </li>

        <li class="rail-phase">Assumptions</li>
        <li class="rail-step" onclick="showStep(3)" data-step="3">
          <div class="step-circle" data-step="3">3</div>
          <span>Portfolio Review</span>
        </li>
        <li class="rail-step" onclick="showStep(4)" data-step="4">
          <div class="step-circle" data-step="4">4</div>
          <span>Building Overrides</span>
        </li>

        <li class="rail-phase">Generate</li>
        <li class="rail-step" onclick="showStep(5)" data-step="5">
          <div class="step-circle" data-step="5">5</div>
          <span>Preview & Generate</span>
        </li>
        <li class="rail-step" onclick="showStep(6)" data-step="6">
          <div class="step-circle" data-step="6">6</div>
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
        <div class="step-badge">Step 1 of 6</div>
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
      <div class="entity-search-bar">
        <svg class="search-icon" viewBox="0 0 20 20" fill="currentColor" width="18" height="18"><path fill-rule="evenodd" d="M8 4a4 4 0 100 8 4 4 0 000-8zM2 8a6 6 0 1110.89 3.476l4.817 4.817a1 1 0 01-1.414 1.414l-4.816-4.816A6 6 0 012 8z" clip-rule="evenodd"/></svg>
        <input type="text" id="entitySearch" class="entity-search-input" placeholder="Search by name or entity code..." oninput="renderEntityGrid()">
      </div>
      <div class="entity-grid" id="entityGrid">
        <!-- Populated by JavaScript from budgets_json -->
      </div>
    </div>

    <!-- Step 2: Upload Sources -->
    <div class="step-content" data-step="2">
      <div class="step-header">
        <div class="step-badge">Step 2 of 6</div>
        <h1 class="step-title">Upload Yardi Sources</h1>
        <p class="step-description">Upload the required YSL and optional enrichment files</p>
      </div>
      <div class="prompt-banner">
        Step 2 collects the source files. Nothing is committed to your budget until you click "Build Budget" in Step 5. Files staged in SharePoint will be detected here — you confirm each one explicitly.
      </div>

      <!-- SharePoint Sources Panel -->
      <div class="sp-sources-panel" id="spSourcesPanel" style="background:white; border:1px solid var(--gray-200); border-radius:10px; padding:18px 22px; margin-bottom:20px; box-shadow:0 1px 3px rgba(0,0,0,0.04); display:none;">
        <div style="display:flex; align-items:center; gap:12px; margin-bottom:14px;">
          <div style="font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:0.08em; color:var(--gray-500); flex:1;">From SharePoint</div>
          <span id="spFolderInfo" style="font-size:12px; color:var(--gray-500);"></span>
          <button type="button" id="spRefreshBtn" onclick="loadSharepointSources()" style="font-size:12px; padding:5px 12px; border:1px solid #ddd; background:#fff; border-radius:4px; cursor:pointer;" title="Re-list files in this entity Supporting Documents folder">↻ Refresh from SP</button>
        </div>
        <div id="spSourcesBody" style="display:flex; flex-direction:column; gap:10px;">
          <div style="color:var(--gray-500); font-size:13px;">Loading...</div>
        </div>
      </div>

      <div class="upload-section">
        <ul class="checklist" id="uploadChecklist">
          <!-- Populated by JavaScript -->
        </ul>
        <div class="upload-dropzone" onclick="document.getElementById('fileInput').click()">
          <div class="upload-dropzone-icon">📁</div>
          <div class="upload-dropzone-text">Drop files here or click to browse</div>
          <div class="upload-dropzone-hint">YSL, Expense Distribution, AP Aging, Maintenance Proof, Audited Financials</div>
        </div>
        <input type="file" id="fileInput" onchange="handleFileUpload(event)">
      </div>
    </div>

    <!-- Step 3: Review Portfolio Assumptions -->
    <div class="step-content" data-step="3">
      <div class="step-header">
        <div class="step-badge">Step 3 of 6</div>
        <h1 class="step-title">Review Portfolio Assumptions</h1>
        <p class="step-description">These are your admin-set defaults</p>
      </div>
      <div class="prompt-banner">
        Review only — these are set by your admin. If something looks wrong, flag it before continuing.
      </div>
      <div class="version-chip" id="versionChip">v1 · Updated Today</div>
      <div class="assumptions-grid" id="portfolioAssumptions">
        <!-- Populated by JavaScript -->
      </div>
    </div>

    <!-- Step 4: Set Building Assumptions -->
    <div class="step-content" data-step="4">
      <div class="step-header">
        <div class="step-badge">Step 4 of 6</div>
        <h1 class="step-title">Set Building Assumptions</h1>
        <p class="step-description">Override defaults for this specific entity</p>
      </div>
      <div class="prompt-banner">
        Only fill in overrides — blank fields inherit the portfolio default shown in gray.
      </div>
      <div class="building-assumptions-form" id="buildingAssumptionsForm">
        <!-- Populated by JavaScript -->
      </div>
    </div>

    <!-- Step 5: Preview & Generate -->
    <div class="step-content" data-step="5">
      <div class="step-header">
        <div class="step-badge">Step 5 of 6</div>
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

    <!-- Step 6: Success -->
    <div class="step-content" data-step="6">
      <div class="step-header">
        <div class="step-badge">Step 6 of 6</div>
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
function renderEntityGrid() {
  const grid = document.getElementById('entityGrid');
  grid.innerHTML = '';

  // Filter by FA if one is selected
  let filteredBudgets = budgets;
  if (selectedFA) {
    const assignedEntities = new Set(
      faAssignments.filter(a => String(a.user_id) === selectedFA).map(a => a.entity_code)
    );
    filteredBudgets = budgets.filter(b => assignedEntities.has(b.entity_code));
  }

  // Filter by search term
  const searchInput = document.getElementById('entitySearch');
  const searchTerm = (searchInput ? searchInput.value : '').toLowerCase().trim();
  if (searchTerm) {
    filteredBudgets = filteredBudgets.filter(b => {
      const name = (b.building_name || '').toLowerCase();
      const code = (b.entity_code || '').toLowerCase();
      return name.includes(searchTerm) || code.includes(searchTerm);
    });
  }

  // Update entity count
  const countEl = document.getElementById('faEntityCount');
  if (countEl) {
    const total = selectedFA
      ? faAssignments.filter(a => String(a.user_id) === selectedFA).length
      : budgets.length;
    const showing = filteredBudgets.length;
    if (searchTerm) {
      countEl.textContent = showing + ' of ' + total + ' shown';
    } else if (selectedFA) {
      countEl.textContent = showing + ' building' + (showing !== 1 ? 's' : '') + ' assigned';
    } else {
      countEl.textContent = total + ' total buildings';
    }
  }

  // Update prompt
  const prompt = document.getElementById('entityPrompt');
  if (prompt) {
    if (selectedFA && filteredBudgets.length === 0) {
      prompt.textContent = 'No buildings assigned to this analyst yet. Assignments are managed in Monday.com.';
    } else if (selectedFA) {
      const faName = faUsers.find(u => String(u.id) === selectedFA);
      prompt.textContent = 'Showing buildings assigned to ' + (faName ? faName.name : 'this analyst') + '. Select one to get started.';
    } else {
      prompt.textContent = 'Each entity has its own budget timeline. Select one to get started.';
    }
  }

  if (filteredBudgets.length === 0) {
    const msg = searchTerm
      ? 'No entities match "' + searchTerm + '"'
      : (selectedFA ? 'No buildings assigned to this analyst.' : 'No entities found.');
    grid.innerHTML = '<p style="color:var(--gray-500);padding:20px;">' + msg + '</p>';
    return;
  }

  filteredBudgets.forEach(budget => {
    // Map DB status to display status
    let displayStatus;
    const ws = budget.wizard_step || 0;
    if (budget.wizard_completed_at) {
      displayStatus = { class: 'status-complete', label: 'Complete' };
    } else if (budget.has_lines && ws >= 2) {
      displayStatus = { class: 'status-has-edits', label: 'Step ' + ws + ' of 6' };
    } else if (budget.has_lines) {
      displayStatus = { class: 'status-in-progress', label: 'Has Data' };
    } else {
      displayStatus = { class: 'status-fresh', label: 'Fresh' };
    }

    const code = budget.entity_code;
    const name = budget.building_name || code;

    const card = document.createElement('div');
    card.className = 'entity-card' + (selectedEntity === code ? ' selected' : '');
    card.onclick = () => selectEntity(code, name);

    card.innerHTML = `
      <div class="entity-status">
        <span class="status-dot ${displayStatus.class}"></span>
        ${displayStatus.label}
      </div>
      <div class="entity-name">${name}</div>
      <div class="entity-address">Entity ${code}</div>
    `;

    grid.appendChild(card);
  });
}

// Select entity
function selectEntity(code, name) {
  selectedEntity = code;
  document.getElementById('railEntityName').textContent = name;
  renderEntityGrid();
  completeStep(1);
}

// Render upload checklist
// SharePoint sources state for current entity (Step 2)
let _spSources = null;

function loadSharepointSources() {
  const ent = selectedEntity;
  if (!ent) return;
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
        html += "<button type=\"button\" onclick=\"useSharepointFile(\\'" + slot.key + "\\',\\'" + f.item_id + "\\')\" style=\"font-size:12px; padding:5px 10px; border:1px solid var(--blue); background:var(--blue); color:white; border-radius:4px; cursor:pointer;\">Use this file</button>";
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
}

function escapeHtml(s) {
  if (!s) return "";
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/\"/g, "&quot;");
}

function useSharepointFile(sourceType, itemId) {
  const ent = selectedEntity;
  if (!ent) return;
  if (!confirm("Confirm: use this SharePoint file as the " + sourceType + " source for entity " + ent + "?\n\nNothing will be committed to the budget until you click Build Budget in Step 5.")) return;
  fetch("/api/wizard/" + ent + "/use-sp-source", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ source_type: sourceType, item_id: itemId })
  })
    .then(function (r) { return r.json(); })
    .then(function (data) {
      if (data.success) {
        alert("File staged: " + data.filename + " (" + Math.round((data.size_bytes||0)/1024) + " KB)\n\nBackend pipeline wiring still pending — next step.");
      } else {
        alert("Failed: " + (data.error || "unknown"));
      }
    })
    .catch(function (err) { alert("Request failed: " + err); });
}

// Render upload checklist
function renderUploadChecklist() {
  const checklist = document.getElementById('uploadChecklist');
  checklist.innerHTML = '';

  const ent = selectedEntity || '';
  const sourceTypes = [
    { key: 'ysl', name: 'YSL (Year Statement Ledger)', required: true, href: '/building/' + ent },
    { key: 'expense_distribution', name: 'Expense Distribution', required: false, href: '/building/' + ent },
    { key: 'ap_aging', name: 'AP Aging', required: false, href: '/building/' + ent },
    { key: 'maint_proof', name: 'Maintenance Proof', required: false, href: '/building/' + ent },
    { key: 'audited_financials', name: 'Audited Financials', required: false, href: '/audited-financials' }
  ];

  sourceTypes.forEach(source => {
    const info = sources[source.key] || {};
    const isUploaded = info.uploaded || false;
    const lastDate = info.last_uploaded ? new Date(info.last_uploaded).toLocaleDateString() : null;

    // Audited financials have a richer status progression
    const auditStatus = info.audit_status || null;
    const confirmedDate = info.confirmed_at ? new Date(info.confirmed_at).toLocaleDateString() : null;
    const isAudit = source.key === 'audited_financials';

    // Determine visual state
    let itemClass = 'pending';
    let icon = '';
    let statusText = 'Waiting for upload';
    let badge = '';

    if (isAudit && auditStatus) {
      const statusMap = {
        uploaded:  { cls: 'audit-uploaded',  icon: '↑', text: 'Uploaded' + (lastDate ? ' on ' + lastDate : ''), badge: 'Uploaded', badgeCls: 'badge-uploaded' },
        extracted: { cls: 'audit-extracted', icon: '⟳', text: 'Data extracted — awaiting mapping', badge: 'Extracted', badgeCls: 'badge-extracted' },
        mapped:    { cls: 'audit-mapped',    icon: '≡', text: 'Mapped to GL accounts — awaiting review', badge: 'Mapped', badgeCls: 'badge-mapped' },
        confirmed: { cls: 'confirmed',       icon: '✓', text: 'Confirmed' + (confirmedDate ? ' on ' + confirmedDate : ''), badge: 'Reviewed', badgeCls: 'badge-confirmed' }
      };
      const s = statusMap[auditStatus] || statusMap.uploaded;
      itemClass = s.cls;
      icon = s.icon;
      statusText = s.text;
      badge = '<span class="checklist-badge ' + s.badgeCls + '">' + s.badge + '</span>';
    } else if (isUploaded) {
      itemClass = '';
      icon = '✓';
      statusText = 'Uploaded' + (lastDate ? ' on ' + lastDate : '');
    }

    const li = document.createElement('li');
    li.className = 'checklist-item ' + itemClass;
    li.style.cursor = 'pointer';
    li.onclick = () => { window.open(source.href, '_blank'); };

    li.innerHTML = `
      <div class="checklist-icon">${icon}</div>
      <div class="checklist-content">
        <div class="checklist-name">
          ${source.name}
          ${source.required ? '<span class="checklist-required">Required</span>' : ''}
          ${badge}
        </div>
        <div class="checklist-description">${statusText}</div>
      </div>
      <div class="checklist-link">→</div>
    `;

    checklist.appendChild(li);
  });
}

// Render portfolio assumptions
function renderPortfolioAssumptions() {
  const container = document.getElementById('portfolioAssumptions');
  container.innerHTML = '';

  const assumptions = [
    { label: 'Payroll Tax Rate', key: 'payroll_tax' },
    { label: 'Union Benefits ($/employee)', key: 'union_benefits' },
    { label: 'Workers Comp Rate', key: 'workers_comp' },
    { label: 'Wage Increase (%)', key: 'wage_increase' }
  ];

  assumptions.forEach(assumption => {
    const value = portfolio[assumption.key] || 'Not set';
    const row = document.createElement('div');
    row.className = 'assumption-row';
    row.innerHTML = `
      <div class="assumption-label">${assumption.label}</div>
      <div class="assumption-value">${value}</div>
    `;
    container.appendChild(row);
  });
}

// Render building assumptions form
function renderBuildingAssumptionsForm() {
  const form = document.getElementById('buildingAssumptionsForm');
  form.innerHTML = '';

  const overrides = [
    { label: 'Insurance Renewal %', key: 'insurance_renewal', default: '5.2%' },
    { label: 'Energy Escalation %', key: 'energy_escalation', default: '3.1%' },
    { label: 'Water / Sewer %', key: 'water_sewer', default: '2.5%' }
  ];

  overrides.forEach(override => {
    const value = building[override.key] || '';
    const row = document.createElement('div');
    row.className = 'form-row';
    row.innerHTML = `
      <div class="form-label">${override.label}</div>
      <div class="form-default">${override.default}</div>
      <input type="text" class="form-input" placeholder="Leave blank to use default" value="${value}" data-key="${override.key}">
    `;
    form.appendChild(row);
  });
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
  if (stepNum === 3) renderPortfolioAssumptions();
  if (stepNum === 4) renderBuildingAssumptionsForm();
  if (stepNum === 5) renderPreviewTable();

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

  if (stepNum < 6) {
    showStep(stepNum + 1);
  }
}

// Update rail UI
function updateRail() {
  document.querySelectorAll('.rail-step').forEach(el => {
    const step = parseInt(el.dataset.step);
    el.classList.remove('active', 'locked');

    if (step === currentStep) {
      el.classList.add('active');
    } else if (step > highestStep) {
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

    const btn2 = document.createElement('button');
    btn2.className = 'btn btn-secondary';
    btn2.textContent = 'Flag an issue';
    btn2.onclick = () => flagAssumption();
    container.appendChild(btn2);

    const btn3 = document.createElement('button');
    btn3.className = 'btn btn-primary';
    btn3.textContent = 'Looks good, continue →';
    btn3.onclick = () => completeStep(3);
    container.appendChild(btn3);
  } else if (currentStep === 4) {
    const btn1 = document.createElement('button');
    btn1.className = 'btn btn-secondary';
    btn1.textContent = '← Back';
    btn1.onclick = () => showStep(3);
    container.appendChild(btn1);

    const btn2 = document.createElement('button');
    btn2.className = 'btn btn-secondary';
    btn2.textContent = 'Use all defaults';
    btn2.onclick = () => completeStep(4);
    container.appendChild(btn2);

    const btn3 = document.createElement('button');
    btn3.className = 'btn btn-primary';
    btn3.textContent = 'Save & continue →';
    btn3.onclick = () => saveBuildingAssumptions();
    container.appendChild(btn3);
  } else if (currentStep === 5) {
    const btn1 = document.createElement('button');
    btn1.className = 'btn btn-secondary';
    btn1.textContent = '← Adjust assumptions';
    btn1.onclick = () => showStep(4);
    container.appendChild(btn1);

    const btn2 = document.createElement('button');
    btn2.className = 'btn btn-primary';
    btn2.textContent = 'Generate budget →';
    btn2.onclick = () => generateBudget();
    container.appendChild(btn2);
  } else if (currentStep === 6) {
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

  const formData = new FormData();
  formData.append('file', file);
  formData.append('entity_code', selectedEntity);

  try {
    fetch('/api/process', {
      method: 'POST',
      body: formData
    })
    .then(response => response.json())
    .then(data => {
      if (data.error) {
        alert('Upload failed: ' + data.error);
      } else {
        renderUploadChecklist();
        alert('File uploaded successfully');
      }
    })
    .catch(error => {
      console.error('Upload error:', error);
      alert('Upload failed: ' + error.message);
    });
  } catch (e) {
    alert('Error uploading file: ' + e.message);
  }
}

// Flag assumption
function flagAssumption() {
  const note = prompt('What looks wrong? (optional)');
  if (note !== null) {
    try {
      fetch(`/api/wizard/${selectedEntity}/flag`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ note: note })
      })
      .then(() => {
        alert('Flag saved. An admin will review it.');
        completeStep(3);
      })
      .catch(error => {
        console.error('Flag error:', error);
        alert('Error saving flag: ' + error.message);
      });
    } catch (e) {
      alert('Error: ' + e.message);
    }
  }
}

// Save building assumptions
function saveBuildingAssumptions() {
  const formInputs = document.querySelectorAll('.form-input');
  const overrides = {};

  formInputs.forEach(input => {
    if (input.value) {
      overrides[input.dataset.key] = input.value;
    }
  });

  try {
    fetch(`/api/wizard/${selectedEntity}/assumptions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ overrides: overrides })
    })
    .then(response => response.json())
    .then(data => {
      if (data.error) {
        alert('Error saving assumptions: ' + data.error);
      } else {
        completeStep(4);
      }
    })
    .catch(error => {
      console.error('Save error:', error);
      alert('Error saving assumptions: ' + error.message);
    });
  } catch (e) {
    alert('Error: ' + e.message);
  }
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
