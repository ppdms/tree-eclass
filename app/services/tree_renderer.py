"""
Shared tree rendering logic for both email and web interface.
Builds hierarchical tree structures from flat file change lists.
"""
from typing import List, Dict, Any


def build_tree_structure(changes: List[Dict[str, str]]) -> Dict[str, Any]:
    """
    Build a hierarchical tree structure from a flat list of changes.
    
    Args:
        changes: List of dicts with 'file_path' and 'change_type' keys
        
    Returns:
        Nested dict representing the tree structure
    """
    root = {'_name': '', '_children': {}, '_files': [], '_type': 'directory', '_change_type': None}
    
    for change in changes:
        path = change.get('file_path', change.get('path', ''))
        change_type = change.get('change_type', 'unknown')
        
        parts = [p for p in path.split('/') if p]
        current = root
        
        # Navigate/create directory structure
        for i, part in enumerate(parts):
            is_last = i == len(parts) - 1
            
            if is_last:
                # It's a file or final directory
                if 'directory' in change_type:
                    # It's a directory change
                    if part not in current['_children']:
                        current['_children'][part] = {
                            '_name': part,
                            '_children': {},
                            '_files': [],
                            '_type': 'directory',
                            '_change_type': change_type
                        }
                    else:
                        current['_children'][part]['_change_type'] = change_type
                else:
                    # It's a file
                    current['_files'].append({
                        '_name': part,
                        '_change_type': change_type,
                        '_type': 'file'
                    })
            else:
                # Intermediate directory
                if part not in current['_children']:
                    current['_children'][part] = {
                        '_name': part,
                        '_children': {},
                        '_files': [],
                        '_type': 'directory',
                        '_change_type': None
                    }
                current = current['_children'][part]
    
    # Infer change types for parent directories
    _infer_change_types(root)
    
    return root


def _infer_change_types(node: Dict[str, Any]) -> str:
    """
    Recursively infer change types for directories based on their contents.
    
    Returns:
        The inferred change type for this node
    """
    has_added = False
    has_deleted = False
    has_modified = False
    
    # Check files
    for file in node.get('_files', []):
        change_type = file.get('_change_type', '')
        if 'added' in change_type:
            has_added = True
        if 'deleted' in change_type:
            has_deleted = True
        if 'modified' in change_type:
            has_modified = True
    
    # Check children
    for child in node.get('_children', {}).values():
        child_type = _infer_change_types(child)
        if 'added' in child_type:
            has_added = True
        if 'deleted' in child_type:
            has_deleted = True
        if 'modified' in child_type:
            has_modified = True
    
    # Set change type if not already set
    if not node.get('_change_type'):
        if has_added and has_deleted:
            node['_change_type'] = 'mixed'
        elif has_added:
            node['_change_type'] = 'added_directory'
        elif has_deleted:
            node['_change_type'] = 'deleted_directory'
        elif has_modified:
            node['_change_type'] = 'modified_directory'
        else:
            node['_change_type'] = 'unchanged'
    
    return node.get('_change_type', 'unchanged')


def get_change_symbol(change_type: str) -> str:
    """Get the symbol for a change type."""
    if not change_type or change_type == 'unchanged':
        return '•'
    
    if 'added' in change_type:
        return '+'
    elif 'deleted' in change_type:
        return '−'
    elif 'modified' in change_type:
        return '~'
    elif change_type == 'mixed':
        return '±'
    
    return '•'


def get_change_icon(change_type: str, is_file: bool = False) -> str:
    """Get the emoji icon for a change type."""
    if is_file:
        return '📄'
    return '📁'


def get_change_color(change_type: str) -> str:
    """Return a hex color for a given change type for inline email styling."""
    if not change_type or change_type == 'unchanged':
        return '#64748b'  # muted gray
    if 'added' in change_type:
        return '#16a34a'  # green
    if 'deleted' in change_type:
        return '#dc2626'  # red
    if 'modified' in change_type:
        return '#ea580c'  # orange
    if change_type == 'mixed':
        return '#2563eb'  # blue
    return '#64748b'


def render_tree_html(node: Dict[str, Any], indent_level: int = 0, for_email: bool = False) -> str:
    """
    Recursively render tree structure to HTML.
    
    Args:
        node: Tree node to render
        indent_level: Current indentation level
        for_email: If True, use inline styles suitable for email
        
    Returns:
        HTML string
    """
    html = ""
    indent_px = indent_level * 20
    
    # Get children and files
    children = node.get('_children', {})
    files = node.get('_files', [])
    
    # Sort directories and files
    sorted_dirs = sorted(children.items(), key=lambda x: x[0])
    sorted_files = sorted(files, key=lambda x: x['_name'])
    
    # Render directories
    for name, child in sorted_dirs:
        change_type = child.get('_change_type', 'unchanged')
        icon = get_change_icon(change_type, False)
        symbol = get_change_symbol(change_type)
        
        if for_email:
            # Email style with inline CSS (use inline colors so email clients without external CSS still show colors)
            color = get_change_color(change_type)
            html += f'<li class="tree-item {change_type}" style="margin-left: {indent_px}px; padding: 4px 0; display: flex; align-items: center; gap: 8px;">'
            html += f'<span class="symbol" style="font-weight: bold; min-width: 16px; color: {color};">{symbol}</span>'
            html += f'<span class="icon" style="font-size: 16px; color: {color};">{icon}&nbsp;</span>'
            html += f'<span class="path" style="flex: 1; color: {color};">{name}</span>'
            html += '</li>\n'
        else:
            # Web app style (uses external CSS)
            html += f'<div class="diff-tree-node diff-tree-directory {change_type}" style="margin-left: {indent_px}px;">'
            html += f'<span class="diff-tree-icon">{icon}</span>'
            html += f'<span class="diff-tree-symbol">{symbol}</span>'
            html += f'<span class="diff-tree-name">{name}</span>'
            html += '</div>\n'
        
        # Recursively render children
        html += render_tree_html(child, indent_level + 1, for_email)
    
    # Render files
    for file in sorted_files:
        change_type = file.get('_change_type', 'unchanged')
        icon = get_change_icon(change_type, True)
        symbol = get_change_symbol(change_type)
        name = file.get('_name', '')
        
        if for_email:
            # Email style with inline CSS (use inline colors so email clients without external CSS still show colors)
            color = get_change_color(change_type)
            html += f'<li class="tree-item {change_type}" style="margin-left: {indent_px}px; padding: 4px 0; display: flex; align-items: center; gap: 8px;">'
            html += f'<span class="symbol" style="font-weight: bold; min-width: 16px; color: {color};">{symbol}</span>'
            html += f'<span class="icon" style="font-size: 16px; color: {color};">{icon}&nbsp;</span>'
            html += f'<span class="path" style="flex: 1; color: {color};">{name}</span>'
            html += '</li>\n'
        else:
            # Web app style (uses external CSS)
            html += f'<div class="diff-tree-node diff-tree-file {change_type}" style="margin-left: {indent_px}px;">'
            html += f'<span class="diff-tree-icon">{icon}</span>'
            html += f'<span class="diff-tree-symbol">{symbol}</span>'
            html += f'<span class="diff-tree-name">{name}</span>'
            html += '</div>\n'
    
    return html


def render_tree_for_email(changes: List[Dict[str, str]]) -> str:
    """
    Build and render a tree structure suitable for email.
    
    Args:
        changes: List of changes with 'file_path' and 'change_type'
        
    Returns:
        Complete HTML tree as a string
    """
    tree = build_tree_structure(changes)
    
    # Start with ul wrapper for email
    html = '<ul class="tree-list" style="list-style: none; padding: 0; margin: 16px 0; font-family: \'Roboto Mono\', monospace; font-size: 13px; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; padding: 16px;">\n'
    
    # Render each top-level item
    for name, child in sorted(tree.get('_children', {}).items()):
        change_type = child.get('_change_type', 'unchanged')
        icon = get_change_icon(change_type, False)
        symbol = get_change_symbol(change_type)
        
        color = get_change_color(change_type)
        html += f'<li class="tree-item {change_type}" style="padding: 4px 0; display: flex; align-items: center; gap: 8px;">'
        html += f'<span class="symbol" style="font-weight: bold; min-width: 16px; color: {color};">{symbol}</span>'
        html += f'<span class="icon" style="font-size: 16px; color: {color};">{icon}&nbsp;</span>'
        html += f'<span class="path" style="flex: 1; color: {color};">{name}</span>'
        html += '</li>\n'
        
        # Render children
        html += render_tree_html(child, 1, for_email=True)
    
    # Render top-level files
    for file in sorted(tree.get('_files', []), key=lambda x: x['_name']):
        change_type = file.get('_change_type', 'unchanged')
        icon = get_change_icon(change_type, True)
        symbol = get_change_symbol(change_type)
        name = file.get('_name', '')
        
        color = get_change_color(change_type)
        html += f'<li class="tree-item {change_type}" style="padding: 4px 0; display: flex; align-items: center; gap: 8px;">'
        html += f'<span class="symbol" style="font-weight: bold; min-width: 16px; color: {color};">{symbol}</span>'
        html += f'<span class="icon" style="font-size: 16px; color: {color};">{icon}&nbsp;</span>'
        html += f'<span class="path" style="flex: 1; color: {color};">{name}</span>'
        html += '</li>\n'
    
    html += '</ul>\n'
    
    return html
