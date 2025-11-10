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
		const response = await fetch('/api/logs/recent?limit=5');
		const logs = await response.json();
		return logs;
	} catch (error) {
		console.error('Error fetching logs:', error);
		return [];
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
	const downloadFolder = form.querySelector('#download_folder').value;

	if (!courseId || !name || !downloadFolder) {
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
						name: part,
						changeType: change.change_type,
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
function renderDiffTree(node, level = 0) {
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
		html += renderDiffTree(dir, level + 1);
	});

	// Render files
	sortedFiles.forEach(file => {
		const changeClass = file.changeType || 'unchanged';
		const icon = getChangeIcon(file.changeType, true);
		const symbol = getChangeSymbol(file.changeType);

		html += `
			<div class="diff-tree-node diff-tree-file ${changeClass}" style="margin-left: ${indent}px;">
				<span class="diff-tree-icon">${icon}</span>
				<span class="diff-tree-symbol">${symbol}</span>
				<span class="diff-tree-name">${file.name}</span>
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
function initDiffTreeView(changes) {
	const container = document.getElementById('diff-tree-container');
	if (!container || !changes || changes.length === 0) return;

	const tree = buildDiffTree(changes);
	const html = renderDiffTree(tree);
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
	initDiffTreeView
};
