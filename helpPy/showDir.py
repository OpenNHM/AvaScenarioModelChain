import os
from typing import Optional

flowPyDir = '/media/christoph/Daten/Cairos/ModelChainTesting/FolderStructure/'

def print_directory_structure(
    root: str,
    *,
    pretty: bool = True,          # toggle nice tree glyphs
    show_files: bool = True,      # show files as well as folders
    show_hidden: bool = False,    # include dotfiles/folders
    max_depth: Optional[int] = None,  # limit depth (None = unlimited)
    sort_names: bool = True       # sort entries case-insensitively
) -> None:
    """
    Print folders (and optionally files) under `root`.

    Args:
        root: Path to start from.
        pretty: If True, draw a tree using box-drawing glyphs. If False, simple indentation.
        show_files: Include files (not just directories).
        show_hidden: Include names starting with '.'.
        max_depth: Maximum depth to traverse (0 = only root). None = unlimited.
        sort_names: Sort directory entries by name (case-insensitive).
    """
    root = os.path.abspath(root)
    if not os.path.isdir(root):
        print(f"[!] Not a directory: {root}")
        return

    def _list_entries(path: str):
        try:
            with os.scandir(path) as it:
                entries = [
                    e for e in it
                    if show_hidden or not e.name.startswith('.')
                ]
        except PermissionError:
            return []
        if not show_files:
            entries = [e for e in entries if e.is_dir(follow_symlinks=False)]
        if sort_names:
            entries.sort(key=lambda e: e.name.lower())
        return entries

    def _print_plain(path: str, depth: int):
        indent = " " * 4 * depth
        name = os.path.basename(path) or path
        print(f"{indent}{name}/")
        if max_depth is not None and depth >= max_depth:
            return
        for entry in _list_entries(path):
            if entry.is_dir(follow_symlinks=False):
                _print_plain(entry.path, depth + 1)
            else:
                print(f"{' ' * 4 * (depth + 1)}{entry.name}")







    def _print_tree(path: str, prefix: str, depth: int):
        # print the directory itself
        label = os.path.basename(path) or path
        print(f"{prefix}{label}/")
        if max_depth is not None and depth >= max_depth:
            return

        entries = _list_entries(path)
        count = len(entries)
        for idx, entry in enumerate(entries):
            is_last = (idx == count - 1)
            branch = "└── " if is_last else "├── "
            child_prefix = prefix + ("    " if is_last else "│   ")
            name = entry.name + ("/" if entry.is_dir(follow_symlinks=False) else "")
            print(prefix + branch + name)
            if entry.is_dir(follow_symlinks=False):
                _print_tree(entry.path, child_prefix, depth + 1)

    if pretty:
        _print_tree(root, prefix="", depth=0)
    else:
        _print_plain(root, depth=0)


# Run it
print(flowPyDir)
print_directory_structure(
    flowPyDir,
    pretty=True,        # set to False for simple indentation
    show_files=True,    # set to False to show only folders
    show_hidden=False,  # set to True to include dotfiles
    max_depth=None      # e.g. set to 2 to cap depth
)
