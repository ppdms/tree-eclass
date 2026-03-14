// tree-eclass Manager - Client-side JavaScript

// Custom Modal Dialog
function showModal(title, message, alertMode = false) {
	return new Promise((resolve) => {
		const modal = document.getElementById('customModal');
		const modalTitle = document.getElementById('modalTitle');
		const modalMessage = document.getElementById('modalMessage');
		const modalCancel = document.getElementById('modalCancel');
		const modalConfirm = document.getElementById('modalConfirm');

		modalTitle.textContent = title;
		modalMessage.textContent = message;

		// In alert mode, hide cancel button and only show OK
		if (alertMode) {
			modalCancel.style.display = 'none';
			modalConfirm.textContent = 'OK';
		} else {
			modalCancel.style.display = 'inline-block';
			modalConfirm.textContent = 'Confirm';
		}

		modal.style.display = 'flex';

		const handleConfirm = () => {
			modal.style.display = 'none';
			cleanup();
			resolve(true);
		};

		const handleCancel = () => {
			modal.style.display = 'none';
			cleanup();
			resolve(false);
		};

		const handleEscape = (e) => {
			if (e.key === 'Escape') {
				handleCancel();
			}
		};

		const cleanup = () => {
			modalConfirm.removeEventListener('click', handleConfirm);
			modalCancel.removeEventListener('click', handleCancel);
			document.removeEventListener('keydown', handleEscape);
		};

		modalConfirm.addEventListener('click', handleConfirm);
		modalCancel.addEventListener('click', handleCancel);
		document.addEventListener('keydown', handleEscape);

		// Focus the confirm button
		modalConfirm.focus();
	});
}

// Auto-refresh functionality
let autoRefreshEnabled = false;
let refreshInterval = null;

function toggleAutoRefresh() {
	autoRefreshEnabled = !autoRefreshEnabled;

	if (autoRefreshEnabled) {
		refreshInterval = setInterval(() => {
			location.reload();
		}, 30000); // Refresh every 30 seconds
		console.log('Auto-refresh enabled');
	} else {
		if (refreshInterval) {
			clearInterval(refreshInterval);
			refreshInterval = null;
		}
		console.log('Auto-refresh disabled');
	}
}

// Fetch recent logs via API
async function fetchRecentLogs() {
	try {
		const response = await fetch('/api/check-status');
		const status = await response.json();
		return status;
	} catch (error) {
		console.error('Error fetching status:', error);
		return null;
	}
}

// Fetch stats via API
async function fetchStats() {
	try {
		const response = await fetch('/api/stats');
		const stats = await response.json();
		return stats;
	} catch (error) {
		console.error('Error fetching stats:', error);
		return null;
	}
}

// Tree navigation helpers
function toggleTreeNode(element) {
	const children = element.nextElementSibling;
	if (children && children.classList.contains('tree-children')) {
		children.style.display = children.style.display === 'none' ? 'block' : 'none';
	}
}

// Form validation
function validateCourseForm(form) {
	const courseId = form.querySelector('#course_id').value;
	const name = form.querySelector('#name').value;
	const webdavFolder = form.querySelector('#webdav_folder').value;

	if (!courseId || !name || !webdavFolder) {
		alert('Please fill in all fields');
		return false;
	}

	if (isNaN(courseId) || courseId <= 0) {
		alert('Course ID must be a positive number');
		return false;
	}

	return true;
}

// Confirmation dialogs
function confirmDelete(itemName) {
	return confirm(`Are you sure you want to delete ${itemName}? This action cannot be undone.`);
}

// Format timestamp
function formatTimestamp(timestamp) {
	const date = new Date(timestamp);
	return date.toLocaleString();
}

// Show notification
function showNotification(message, type = 'info') {
	const notification = document.createElement('div');
	notification.className = `notification notification-${type}`;
	notification.textContent = message;
	notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        padding: 1rem 1.5rem;
        background: ${type === 'success' ? '#16a34a' : type === 'error' ? '#dc2626' : '#2563eb'};
        color: white;
        border-radius: 0.5rem;
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
        z-index: 1000;
        animation: slideIn 0.3s ease-out;
    `;

	document.body.appendChild(notification);

	setTimeout(() => {
		notification.style.animation = 'slideOut 0.3s ease-out';
		setTimeout(() => notification.remove(), 300);
	}, 3000);
}

// Add CSS animations
const style = document.createElement('style');
style.textContent = `
    @keyframes slideIn {
        from {
            transform: translateX(100%);
            opacity: 0;
        }
        to {
            transform: translateX(0);
            opacity: 1;
        }
    }
    
    @keyframes slideOut {
        from {
            transform: translateX(0);
            opacity: 1;
        }
        to {
            transform: translateX(100%);
            opacity: 0;
        }
    }
`;
document.head.appendChild(style);

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
	console.log('tree-eclass Manager loaded');

	// Add form validation if forms exist
	const courseForm = document.querySelector('form[action="/courses/add"]');
	if (courseForm) {
		courseForm.addEventListener('submit', (e) => {
			if (!validateCourseForm(courseForm)) {
				e.preventDefault();
			}
		});
	}
});

// Build a tree structure from flat file changes
function buildDiffTree(changes) {
	const root = { name: '', children: {}, files: [], type: 'directory', changeType: null };

	changes.forEach(change => {
		const parts = change.file_path.split('/').filter(p => p);
		let current = root;

		// Navigate/create directory structure
		for (let i = 0; i < parts.length; i++) {
			const part = parts[i];
			const isLast = i === parts.length - 1;

			if (isLast) {
				// It's a file or final directory
				if (change.change_type.includes('directory')) {
					// It's a directory change
					if (!current.children[part]) {
						current.children[part] = {
							name: part,
							children: {},
							files: [],
							type: 'directory',
							changeType: change.change_type
						};
					} else {
						current.children[part].changeType = change.change_type;
					}
				} else {
					// It's a file
					current.files.push({
						name: change.display_name || part,
						path: change.file_path,
						changeType: change.change_type,
						redirectUrl: change.redirect_url || null,
						diffWebdavPath: change.diff_webdav_path || null,
						type: 'file'
					});
				}
			} else {
				// Intermediate directory
				if (!current.children[part]) {
					current.children[part] = {
						name: part,
						children: {},
						files: [],
						type: 'directory',
						changeType: null // Will be inferred
					};
				}
				current = current.children[part];
			}
		}
	});

	// Infer change types for parent directories
	function inferChangeTypes(node) {
		let hasAdded = false;
		let hasDeleted = false;
		let hasModified = false;

		// Check files
		node.files.forEach(file => {
			if (file.changeType.includes('added')) hasAdded = true;
			if (file.changeType.includes('deleted')) hasDeleted = true;
			if (file.changeType.includes('modified')) hasModified = true;
		});

		// Check children
		Object.values(node.children).forEach(child => {
			const childType = inferChangeTypes(child);
			if (childType.includes('added')) hasAdded = true;
			if (childType.includes('deleted')) hasDeleted = true;
			if (childType.includes('modified')) hasModified = true;
		});

		// Set change type if not already set
		if (!node.changeType) {
			if (hasAdded && hasDeleted) {
				node.changeType = 'mixed';
			} else if (hasAdded) {
				node.changeType = 'added_directory';
			} else if (hasDeleted) {
				node.changeType = 'deleted_directory';
			} else if (hasModified) {
				node.changeType = 'modified_directory';
			}
		}

		return node.changeType || 'mixed';
	}

	Object.values(root.children).forEach(child => inferChangeTypes(child));

	return root;
}

// Render diff tree as HTML
function renderDiffTree(node, level = 0, webdavFolder = '') {
	let html = '';
	const indent = level * 20;

	// Sort: directories first, then files
	const sortedDirs = Object.values(node.children).sort((a, b) => a.name.localeCompare(b.name));
	const sortedFiles = node.files.sort((a, b) => a.name.localeCompare(b.name));

	// Render directories
	sortedDirs.forEach(dir => {
		const changeClass = dir.changeType || 'unchanged';
		const icon = getChangeIcon(dir.changeType);
		const symbol = getChangeSymbol(dir.changeType);

		html += `
			<div class="diff-tree-node diff-tree-directory ${changeClass}" style="margin-left: ${indent}px;">
				<span class="diff-tree-icon">${icon}</span>
				<span class="diff-tree-symbol">${symbol}</span>
				<span class="diff-tree-name">${dir.name}</span>
			</div>
		`;

		// Recursively render children
		html += renderDiffTree(dir, level + 1, webdavFolder);
	});

	// Render files
	sortedFiles.forEach(file => {
		const changeClass = file.changeType || 'unchanged';
		const icon = getChangeIcon(file.changeType, true);
		const symbol = getChangeSymbol(file.changeType);
		const isAdded = file.changeType === 'added_file' || file.changeType === 'modified_file';
		let nameHtml;
		if (file.redirectUrl && isAdded) {
			// External link (e.g. SharePoint recording) — link directly to the source
			nameHtml = `<a href="${file.redirectUrl}" target="_blank" title="External link (SharePoint / Stream)">${file.name} 🔗</a>`;
		} else if (isAdded && webdavFolder && file.path) {
			nameHtml = `<a href="/files${webdavFolder}/${file.path}" target="_blank">${file.name}</a>`;
		} else {
			nameHtml = file.name;
		}
		const diffBtn = (file.changeType === 'modified_file' && file.diffWebdavPath)
			? ` <a href="/files${escHtml(file.diffWebdavPath)}" target="_blank" class="diff-pdf-btn" title="Open visual diff PDF">📊 Diff</a>`
			: '';

		html += `
			<div class="diff-tree-node diff-tree-file ${changeClass}" style="margin-left: ${indent}px;">
				<span class="diff-tree-icon">${icon}</span>
				<span class="diff-tree-symbol">${symbol}</span>
				<span class="diff-tree-name">${nameHtml}${diffBtn}</span>
			</div>
		`;
	});

	return html;
}

// Get appropriate icon for change type
function getChangeIcon(changeType, isFile = false) {
	if (!changeType || changeType === 'unchanged') {
		return isFile ? '📄' : '📁';
	}

	if (changeType.includes('added')) {
		return isFile ? '📄' : '📁';
	} else if (changeType.includes('deleted')) {
		return isFile ? '📄' : '📁';
	} else if (changeType.includes('modified')) {
		return '📄';
	} else if (changeType === 'mixed') {
		return '📁';
	}

	return isFile ? '📄' : '📁';
}

// Get symbol for change type
function getChangeSymbol(changeType) {
	if (!changeType || changeType === 'unchanged') return '';

	if (changeType.includes('added')) {
		return '+';
	} else if (changeType.includes('deleted')) {
		return '−';
	} else if (changeType.includes('modified')) {
		return '~';
	} else if (changeType === 'mixed') {
		return '±';
	}

	return '';
}

// Initialize diff tree view
function initDiffTreeView(changes, webdavFolder = '') {
	const container = document.getElementById('diff-tree-container');
	if (!container || !changes || changes.length === 0) return;

	const tree = buildDiffTree(changes);
	const html = renderDiffTree(tree, 0, webdavFolder);
	container.innerHTML = html;
}

// Initialize diff tree view in a specific container element
function initDiffTreeAt(changes, containerId, webdavFolder = '') {
	const container = document.getElementById(containerId);
	if (!container) return;
	if (!changes || changes.length === 0) {
		container.innerHTML = '<em style="color:var(--text-secondary);font-size:0.8125rem">No changes</em>';
		return;
	}
	const tree = buildDiffTree(changes);
	const html = renderDiffTree(tree, 0, webdavFolder);
	container.innerHTML = html;
}

// Export functions for use in templates
window.treeEclass = {
	toggleAutoRefresh,
	fetchRecentLogs,
	fetchStats,
	toggleTreeNode,
	validateCourseForm,
	confirmDelete,
	formatTimestamp,
	showNotification,
	buildDiffTree,
	renderDiffTree,
	initDiffTreeView,
	initDiffTreeAt,
};

// ===== File Version / Deleted Files UI =====

function _insertExpandPanel(btn, html) {
	// Toggle: if already open, remove and return
	const existing = btn.parentElement.querySelector('.version-panel');
	if (existing) {
		existing.remove();
		btn.classList.remove('active');
		return false;
	}
	const panel = document.createElement('div');
	panel.className = 'version-panel';
	panel.innerHTML = html;
	btn.insertAdjacentElement('afterend', panel);
	btn.classList.add('active');
	return true;
}

async function toggleFileVersions(btn, courseId, filePath) {
	const existing = btn.parentElement.querySelector('.version-panel');
	if (existing) {
		existing.remove();
		btn.classList.remove('active');
		return;
	}
	btn.disabled = true;
	try {
		const resp = await fetch(`/api/courses/${courseId}/file-versions?file_path=${encodeURIComponent(filePath)}`);
		const data = await resp.json();
		const versions = data.versions || [];
		if (versions.length === 0) {
			_insertExpandPanel(btn, '<em style="color:var(--text-secondary)">No archived versions found.</em>');
			return;
		}
		const rows = versions.map(v => {
			const ts = v.timestamp ? new Date(v.timestamp).toLocaleString() : '?';
			const diffBtn = v.diff_webdav_path
				? ` <a href="/files${escHtml(v.diff_webdav_path)}" target="_blank" class="diff-pdf-btn" title="Open visual diff PDF">📊 Diff</a>`
				: '';
			if (v.redirect_url) {
				return `<div class="version-entry">🔗 <a href="${escHtml(v.redirect_url)}" target="_blank">Old external link</a> <span class="version-ts">${escHtml(ts)}</span></div>`;
			} else if (v.version_webdav_path) {
				return `<div class="version-entry">📄 <a href="/files${escHtml(v.version_webdav_path)}" target="_blank">Version from ${escHtml(ts)}</a>${diffBtn}</div>`;
			}
			return `<div class="version-entry">📄 ${escHtml(ts)} (no file)</div>`;
		}).join('');
		_insertExpandPanel(btn, `<div class="version-panel-header">Old versions:</div>${rows}`);
	} catch (e) {
		_insertExpandPanel(btn, '<em style="color:var(--danger-color)">Failed to load versions.</em>');
	} finally {
		btn.disabled = false;
	}
}

async function toggleDeletedFiles(btn, courseId, folder) {
	const existing = btn.parentElement.querySelector('.version-panel');
	if (existing) {
		existing.remove();
		btn.classList.remove('active');
		return;
	}
	btn.disabled = true;
	try {
		const url = `/api/courses/${courseId}/deleted-files?folder=${encodeURIComponent(folder)}`;
		const resp = await fetch(url);
		const data = await resp.json();
		const deleted = data.deleted || [];
		if (deleted.length === 0) {
			_insertExpandPanel(btn, '<em style="color:var(--text-secondary)">No deleted files found.</em>');
			return;
		}
		const rows = deleted.map(v => {
			const name = v.display_name || v.file_path.split('/').pop();
			const ts = v.timestamp ? new Date(v.timestamp).toLocaleString() : '?';
			const subpath = v.file_path;
			if (v.redirect_url) {
				return `<div class="version-entry">🔗 <a href="${escHtml(v.redirect_url)}" target="_blank">${escHtml(name)}</a> <span class="version-ts">(deleted ${escHtml(ts)})</span> <span class="version-path">${escHtml(subpath)}</span></div>`;
			} else if (v.version_webdav_path) {
				return `<div class="version-entry">📄 <a href="/files${escHtml(v.version_webdav_path)}" target="_blank">${escHtml(name)}</a> <span class="version-ts">(deleted ${escHtml(ts)})</span> <span class="version-path">${escHtml(subpath)}</span></div>`;
			}
			return `<div class="version-entry">📄 ${escHtml(name)} <span class="version-ts">(deleted ${escHtml(ts)}, no archived copy)</span> <span class="version-path">${escHtml(subpath)}</span></div>`;
		}).join('');
		_insertExpandPanel(btn, `<div class="version-panel-header">Deleted files:</div>${rows}`);
	} catch (e) {
		_insertExpandPanel(btn, '<em style="color:var(--danger-color)">Failed to load deleted files.</em>');
	} finally {
		btn.disabled = false;
	}
}

function escHtml(str) {
	return String(str)
		.replace(/&/g, '&amp;')
		.replace(/</g, '&lt;')
		.replace(/>/g, '&gt;')
		.replace(/"/g, '&quot;');
}

// ===== Study Level UI =====

const STUDY_LEVEL_ICONS = ['\u25cb', '\u25d4', '\u25d1', '\u25d5', '\u25cf', '\u2014']; // ○ ◔ ◑ ◕ ● —
const STUDY_LEVEL_COLORS = ['#94a3b8', '#ef4444', '#f97316', '#eab308', '#16a34a', '#cbd5e1'];
const STUDY_LEVEL_TITLES = [
	'Not studied — click to advance',
	'Glanced — click to advance',
	'Familiar — click to advance',
	'Studied — click to advance',
	'Mastered — click to advance',
	'Ignored (excluded from progress) — Shift+click to unignore',
];

async function applyStudyLevel(btn, nextLevel) {
	const courseId = btn.dataset.courseId;
	const localPath = btn.dataset.localPath;
	btn.disabled = true;
	try {
		const resp = await fetch(`/api/courses/${courseId}/files/study-level`, {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ file_path: localPath, level: nextLevel }),
		});
		if (resp.ok) {
			btn.dataset.level = nextLevel;
			btn.textContent = STUDY_LEVEL_ICONS[nextLevel];
			btn.style.color = STUDY_LEVEL_COLORS[nextLevel];
			btn.title = STUDY_LEVEL_TITLES[nextLevel];
			btn.classList.toggle('study-ignored', nextLevel === 5);
			// Update picker option highlights
			const picker = btn.closest('.study-wrap')?.querySelector('.study-picker');
			if (picker) {
				picker.querySelectorAll('.sp-opt').forEach(o => {
					o.classList.toggle('sp-opt--active', parseInt(o.dataset.level) === nextLevel);
				});
			}
			// If mastered (level 4) on the inbox page, fade the whole row out
			if (nextLevel === 4) {
				const row = btn.closest('tr');
				if (row && row.closest('#inbox-table')) {
					row.style.transition = 'opacity 0.6s';
					row.style.opacity = '0.35';
					setTimeout(() => row.remove(), 700);
				}
			}
		} else {
			window.treeEclass.showNotification('Failed to update level.', 'error');
		}
	} catch (e) {
		window.treeEclass.showNotification('Error: ' + e.message, 'error');
	} finally {
		btn.disabled = false;
	}
}

async function cycleStudyLevel(btn, event) {
	const currentLevel = parseInt(btn.dataset.level, 10);
	let nextLevel;
	if (event && event.shiftKey) {
		nextLevel = currentLevel === 5 ? 0 : 5;
	} else {
		nextLevel = currentLevel === 5 ? 1 : (currentLevel + 1) % 5;
	}
	await applyStudyLevel(btn, nextLevel);
}

async function setStudyLevel(pickerOpt, level) {
	const btn = pickerOpt.closest('.study-wrap').querySelector('.study-level-btn');
	await applyStudyLevel(btn, level);
}
