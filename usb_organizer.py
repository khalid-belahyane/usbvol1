r"""
USB Drive Organizer for Windows - Pro Version
=============================================
- Scans a drive/folder for ALL files (ignoring system folders)
- Detects true duplicates ultra-fast (checks file size first, hashes only identical sizes using multi-threading)
- Optional CLI arguments (--auto-delete, --dry-run)
- Smart organization (Date-based for media, Alphabetical for others)
- Cleans up empty directories after organizing
- Generates a full report of what was done

Usage:
  python usb_organizer.py [path] [options]
  
Examples:
  python usb_organizer.py E:\
  python usb_organizer.py E:\ --auto-delete
  python usb_organizer.py C:\Users\user\Downloads --dry-run

Requirements:
  Python 3.7+  (no extra packages needed)
"""

import os
import sys
import hashlib
import shutil
import argparse
import time
import threading
import queue
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import concurrent.futures

# Butler/AI imports
try:
    import watchdog.observers
    import watchdog.events
    import google.generativeai as genai
    import fitz  # PyMuPDF
    from docx import Document as DocxDocument
except ImportError as e:
    # We allow imports to fail so the basic tool still works without extra libs
    pass

# ─────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────

# File-type categories: folder name → list of extensions
CATEGORIES = {
    "Images":     [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".tiff", ".svg", ".ico", ".heic", ".raw"],
    "Videos":     [".mp4", ".mov", ".avi", ".mkv", ".wmv", ".flv", ".webm", ".m4v", ".mpeg", ".mpg"],
    "Audio":      [".mp3", ".wav", ".aac", ".flac", ".ogg", ".wma", ".m4a", ".aiff"],
    "Documents":  [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".odt", ".ods", ".odp"],
    "Text":       [".txt", ".md", ".rtf", ".csv", ".log", ".xml", ".json", ".yaml", ".yml", ".ini", ".cfg"],
    "Code":       [".py", ".js", ".ts", ".html", ".css", ".java", ".c", ".cpp", ".cs", ".go", ".rb", ".php", ".sh", ".bat", ".ps1"],
    "Archives":   [".zip", ".rar", ".7z", ".tar", ".gz", ".bz2", ".xz"],
    "Executables":[".exe", ".msi", ".dll", ".apk", ".app"],
    "Fonts":      [".ttf", ".otf", ".woff", ".woff2"],
}

# Folders to ignore during scanning to protect the system and avoid errors
IGNORE_DIRS = {
    "System Volume Information", 
    "$RECYCLE.BIN", 
    ".git", 
    ".svn", 
    "Windows", 
    "Program Files",
    "Program Files (x86)",
    "node_modules",
    "__pycache__",
    ".gemini"
}

# ─────────────────────────────────────────────
#  AI INTELLIGENCE LAYER
# ─────────────────────────────────────────────

class AIClassifier:
    def __init__(self, api_key: str):
        if not api_key:
            self.model = None
            return
        try:
            genai.configure(api_key=api_key)
            self.model = genai.GenerativeModel('gemini-1.5-flash')
        except Exception as e:
            print(f"  ⚠  Failed to initialize Gemini: {e}")
            self.model = None

    def extract_text(self, path: Path, max_chars: int = 2000) -> str:
        """Extract text from various file formats for AI analysis."""
        ext = path.suffix.lower()
        text = ""
        try:
            if ext in [".txt", ".md", ".json", ".yaml", ".py", ".js", ".ts", ".html", ".css"]:
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    text = f.read(max_chars)
            elif ext == ".pdf":
                doc = fitz.open(path)
                for page in doc:
                    text += page.get_text()
                    if len(text) >= max_chars: break
                doc.close()
            elif ext == ".docx":
                doc = DocxDocument(path)
                text = "\n".join([p.text for p in doc.paragraphs])[:max_chars]
        except Exception as e:
            # Silently fail, we'll just fall back to generic categorization
            pass
        return text.strip()

    def get_topic(self, path: Path) -> str:
        """Consult AI to determine the topic of the file content."""
        if not self.model:
            return "General"

        content = self.extract_text(path)
        if not content:
            # Try to guess from filename if content is missing
            content = f"Filename: {path.name}"

        prompt = (
            f"Given the following content from a file named '{path.name}', "
            "identify the single most relevant 'Topic' (one word, e.g., Finance, AI, Travel, Health, Work). "
            "If it is a code file, identify the project type (e.g., WebApp, Script, Game).\n\n"
            f"Content snippet:\n{content[:1500]}\n\n"
            "Return ONLY the category word."
        )

        try:
            response = self.model.generate_content(prompt)
            topic = response.text.strip().split('\n')[0].strip().replace('.', '').title()
            return topic if len(topic) < 20 else "General"
        except Exception:
            return "General"

# ─────────────────────────────────────────────
#  SMART BUTLER (REAL-TIME MONITOR)
# ─────────────────────────────────────────────

class ButlerHandler(watchdog.events.FileSystemEventHandler):
    def __init__(self, root: Path, classifier: AIClassifier, grace_period: int = 10):
        self.root = root
        self.classifier = classifier
        self.grace_period = grace_period
        self.queue = queue.Queue()
        self.pending_files = {} # path -> last_time
        
        # Start the processing thread
        self.process_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.process_thread.start()

    def on_created(self, event):
        if not event.is_directory:
            self._track_file(Path(event.src_path))

    def on_moved(self, event):
        if not event.is_directory:
            self._track_file(Path(event.dest_path))

    def _track_file(self, path: Path):
        # Ignore files already in category folders and system files
        if any(ignored in path.parts for ignored in IGNORE_DIRS):
            return
        
        # Don't track temp/download partial files
        if path.suffix.lower() in [".crdownload", ".tmp", ".part"]:
            return

        self.pending_files[path] = time.time()

    def _worker_loop(self):
        while True:
            time.sleep(2)
            now = time.time()
            to_process = []
            
            for path, last_seen in list(self.pending_files.items()):
                if now - last_seen >= self.grace_period:
                    to_process.append(path)
                    del self.pending_files[path]

            for path in to_process:
                if path.exists():
                    self._process_file(path)

    def _process_file(self, path: Path):
        print(f"\n🎩  Butler checking fresh arrival: {path.name}")
        
        # 1. Basic category
        cat = get_category(path.suffix)
        
        # 2. AI Intelligence (Topic)
        topic = "General"
        if cat in ["Documents", "Text", "Code"]:
            topic = self.classifier.get_topic(path)
            print(f"    └─ AI Analysis: {topic}")
        
        # 3. Determine Destination
        # Logic: Category / Topic / Year (if media)
        if cat in ["Images", "Videos", "Audio"]:
            mtime = path.stat().st_mtime
            date = datetime.fromtimestamp(mtime)
            subfolder = self.root / cat / str(date.year) / date.strftime("%m_%B")
        else:
            subfolder = self.root / cat / topic

        dest = subfolder / path.name
        if path.parent == subfolder:
            return

        subfolder.mkdir(parents=True, exist_ok=True)
        final_dest = get_unique_path(dest)
        
        try:
            shutil.move(str(path), str(final_dest))
            print(f"    ✓ Automatically placed in {final_dest.relative_to(self.root)}")
        except Exception as e:
            print(f"    ⚠  Failed to move: {e}")

# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────

def get_category(ext: str) -> str:
    """Return the folder category for a file extension."""
    ext = ext.lower()
    for cat, exts in CATEGORIES.items():
        if ext in exts:
            return cat
    return "Other"

def human_size(size_bytes: int | float) -> str:
    """Return a human-readable file size."""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"

def get_unique_path(dest: Path) -> Path:
    """If dest already exists, append _1, _2, … until the path is free."""
    if not dest.exists():
        return dest
    stem, suffix = dest.stem, dest.suffix
    counter = 1
    while True:
        candidate = dest.with_name(f"{stem}_{counter}{suffix}")
        if not candidate.exists():
            return candidate
        counter += 1

def file_hash(path: Path, chunk_size: int = 65536) -> str | None:
    """Return SHA-256 hash of file contents."""
    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            while chunk := f.read(chunk_size):
                h.update(chunk)
        return h.hexdigest()
    except (PermissionError, OSError) as e:
        return None  # Skip unreadable files

def pick_drive(args_path: str | None = None) -> Path:
    """Let the user choose or type a drive/folder path, or use CLI arg."""
    print("\n" + "═" * 60)
    print("  USB DRIVE ORGANIZER — Windows Edition (PRO)")
    print("═" * 60)
    
    if args_path:
        p = Path(args_path)
        if p.exists() and p.is_dir():
            print(f"\n  Using provided path: {p}")
            return p
        print(f"\n  ⚠  Provided path '{args_path}' not found or is not a directory.")

    print("\nEnter the drive letter or full path to organize.")
    print("Examples:  E:    or    E:\\    or    C:\\Users\\You\\Downloads")
    while True:
        raw = input("\n  → Path: ").strip().strip('"')
        if not raw:
            continue
        p = Path(raw)
        if p.exists() and p.is_dir():
            return p
        print(f"  ✗ '{raw}' not found. Please try again.")

# ─────────────────────────────────────────────
#  MAIN LOGIC
# ─────────────────────────────────────────────

def scan_files(root: Path):
    """Return a list of all files under root (recursively, skipping ignore dirs)."""
    files = []
    print(f"\n🔍  Scanning '{root}' …")
    
    for dirpath, dirnames, filenames in os.walk(root):
        # Modify dirnames in-place to skip ignored directories
        dirnames[:] = [d for d in dirnames if d not in IGNORE_DIRS]
        
        for name in filenames:
            file_path = Path(dirpath) / name
            if file_path.is_file():
                files.append(file_path)
                
    print(f"    Found {len(files)} file(s).")
    return files

def find_duplicates(files: list[Path]):
    """
    Find duplicates ultra-fast:
    1. Group by file size first
    2. Only hash files sharing the exact same size
    """
    print("\n🔑  Grouping by size (Phase 1) …")
    size_map: dict[int, list[Path]] = defaultdict(list)
    for p in files:
        try:
            size_map[p.stat().st_size].append(p)
        except OSError:
            pass # Skip if deleted or unreadable
            
    # Filter out unique sizes and empty files (0 bytes)
    potential_dupes = [paths for size, paths in size_map.items() if size > 0 and len(paths) > 1]
    
    if not potential_dupes:
        print("    ✓ Complete. No duplicates found.")
        return {}

    print(f"    Hashing {sum(len(group) for group in potential_dupes)} files with identical sizes (Phase 2) …")
    dupes: dict[str, list[Path]] = defaultdict(list)
    
    def process_group(paths: list[Path]) -> dict[str, list[Path]]:
        # Hash a group of files that have the same size
        group_hashes = defaultdict(list)
        for p in paths:
            h = file_hash(p)
            if h:
                group_hashes[h].append(p)
        # Only return hashes that have more than 1 file
        return {h: p_list for h, p_list in group_hashes.items() if len(p_list) > 1}

    total_groups = len(potential_dupes)
    processed = 0
    
    # Use multi-threading to hash multiple files simultaneously
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count() or 4) as executor:
        futures = {executor.submit(process_group, paths): paths for paths in potential_dupes}
        for future in concurrent.futures.as_completed(futures):
            processed += 1
            if processed % max(1, total_groups // 10) == 0 or processed == total_groups:
                print(f"    [{processed}/{total_groups}] size groups checked …", end="\r")
            
            result = future.result()
            for h, p_list in result.items():
                dupes[h].extend(p_list)
                
    print(f"\n    ✓ Hashing complete. Duplicate groups found: {len(dupes)}")
    return dupes

def preview_duplicates(dupes: dict):
    if not dupes:
        return

    print("\n" + "─" * 60)
    print(f"  DUPLICATE FILES PREVIEW  ({len(dupes)} group(s))")
    print("─" * 60)

    total_waste = 0
    for idx, (h, paths) in enumerate(dupes.items(), 1):
        try:
            size = paths[0].stat().st_size
        except OSError:
            size = 0
        waste = size * (len(paths) - 1)
        total_waste += waste
        print(f"\n  Group {idx}  [{human_size(size)} each | {len(paths)} copies | wasted: {human_size(waste)}]")
        for i, p in enumerate(paths):
            tag = "  KEEP  →" if i == 0 else "  DELETE →"
            print(f"    {tag}  {p}")

    print(f"\n  Total space to reclaim: {human_size(total_waste)}")
    print("─" * 60)

def delete_duplicates(dupes: dict, dry_run: bool = False) -> list[Path]:
    deleted = []
    for h, paths in dupes.items():
        for path in paths[1:]:  # Keep paths[0], delete the rest
            if dry_run:
                print(f"  [DRY RUN] Would delete: {path}")
                deleted.append(path)
                continue
            try:
                path.unlink()
                deleted.append(path)
                print(f"  🗑  Deleted: {path}")
            except (PermissionError, OSError) as e:
                print(f"  ⚠  Could not delete '{path}': {e}")
    return deleted

def organize_files(root: Path, files: list[Path], dry_run: bool = False) -> dict:
    """
    Move media (Images, Videos, Audio) to Date-based folders (Year/Month/filename).
    Move other files to Alphabetical folders (Category/FirstLetter/filename).
    Logs the moves: {original_str: destination_str}
    """
    move_log = {}
    print("\n📂  Organizing files …")
    
    # We shouldn't organize files into folders that conflict with existing system dirs
    for path in files:
        if not path.exists() and not dry_run:
            continue
            
        cat = get_category(path.suffix)
        
        # Smart Date-Based Organization for Media
        if cat in ["Images", "Videos", "Audio"]:
            try:
                mtime = path.stat().st_mtime
                date = datetime.fromtimestamp(mtime)
                month_name = date.strftime("%m_%B") # e.g., "10_October"
                subfolder = root / cat / str(date.year) / month_name
            except OSError:
                subfolder = root / cat / "Unknown_Date"
        else:
            # Alphabetical for everything else
            first_letter = path.stem[0].upper() if path.stem else "_"
            if not first_letter.isalpha():
                first_letter = "#"
            subfolder = root / cat / first_letter

        # Make sure we don't move files if they're already in their exact ideal subfolder
        # (Though they might just have the same name)
        dest_dir = subfolder
        dest = dest_dir / path.name
        
        # If it's already perfectly where it belongs
        if path.parent == dest_dir and path.name == dest.name:
            continue

        if dry_run:
            print(f"  [DRY RUN] Would move: {path.name}  →  {dest.relative_to(root)}")
            move_log[str(path)] = str(dest)
            continue

        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = get_unique_path(dest)

        try:
            shutil.move(str(path), str(dest))
            move_log[str(path)] = str(dest)
        except (PermissionError, OSError) as e:
            print(f"  ⚠  Could not move '{path.name}': {e}")

    moved_count = len(move_log)
    if dry_run:
        print(f"    ✓ [DRY RUN] Would organize {moved_count} file(s).")
    else:
        print(f"    ✓ Organized {moved_count} file(s) into smart category folders.")
        
    return move_log

def clean_empty_directories(root: Path, dry_run: bool = False):
    """Walk bottom-up and remove any directories that are completely empty."""
    print("\n🧹  Cleaning up empty directories …")
    cleaned = 0
    
    # topdown=False ensures we visit children before their parents
    for dirpath, dirnames, filenames in os.walk(root, topdown=False):
        dp = Path(dirpath)
        
        # Never remove the root directory itself or ignore list dirs
        if dp == root or dp.name in IGNORE_DIRS:
            continue
            
        try:
            # Check if directory is empty (no files and no subdirectories)
            if not os.listdir(dp):
                if dry_run:
                    print(f"  [DRY RUN] Would remove empty dir: {dp}")
                else:
                    dp.rmdir()
                cleaned += 1
        except OSError:
            pass
            
    if dry_run:
        print(f"    ✓ [DRY RUN] Would remove {cleaned} empty folder(s).")
    else:
        print(f"    ✓ Removed {cleaned} empty folder(s).")

def save_report(root: Path, deleted: list[Path], move_log: dict, dupes: dict, dry_run: bool = False):
    """Write a JSON + TXT report to the root folder."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    prefix = "DRYRUN_" if dry_run else ""
    report_path = root / f"organizer_report_{prefix}{timestamp}.txt"

    lines = [
        "USB DRIVE ORGANIZER — REPORT",
        f"Run at:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Mode:     {'DRY RUN (No changes made)' if dry_run else 'LIVE'}",
        f"Root:     {root}",
        "",
        f"═══ DUPLICATES {'WOULD BE ' if dry_run else ''}DELETED ({len(deleted)}) ═══",
    ]
    for p in deleted:
        lines.append(f"  {p}")

    lines += ["", f"═══ FILES {'WOULD BE ' if dry_run else ''}ORGANIZED ({len(move_log)}) ═══"]
    for src, dst in move_log.items():
        lines.append(f"  {src}  →  {dst}")

    lines += ["", "═══ DUPLICATE GROUPS (kept 1st copy) ═══"]
    for h, paths in dupes.items():
        lines.append(f"  Hash: {h[:16]}…")
        for p in paths:
            lines.append(f"    {p}")

    try:
        report_path.write_text("\n".join(lines), encoding="utf-8")
        print(f"\n📄  Report saved → {report_path}")
    except OSError as e:
        print(f"\n  ⚠  Could not save report: {e}")

# ─────────────────────────────────────────────
#  BUTLER STARTUP
# ─────────────────────────────────────────────

def start_butler(root: Path, classifier: AIClassifier, watch_dirs: list[Path]):
    """Initialize and start the background watchdog observer."""
    print("\n" + "═" * 60)
    print("  🎩  SMART BUTLER IS NOW ON DUTY")
    print("═" * 60)
    print(f"  Watching: {[str(d) for d in watch_dirs]}")
    print("  AI:       ACTIVE" if classifier.model else "  AI:       INACTIVE (No API Key)")
    print("  Action:   Files will be organized after a 10s stability check.")
    print("  Status:   Listening for new arrivals … (Press Ctrl+C to stop)")
    print("-" * 60)

    event_handler = ButlerHandler(root, classifier)
    observer = watchdog.observers.Observer()
    for d in watch_dirs:
        if d.exists() and d.is_dir():
            observer.schedule(event_handler, str(d), recursive=False)
    
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n👋  Butler is signing off. Drive remains clean!")
        observer.stop()
    observer.join()

# ─────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="USB Drive Organizer Pro - Clean and structure your drives.")
    parser.add_argument("path", nargs="?", default=None, help="The drive or folder path to organize")
    parser.add_argument("--auto-delete", action="store_true", help="Automatically delete duplicates without prompting")
    parser.add_argument("--auto-organize", action="store_true", help="Automatically move files without prompting")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen without actually modifying files")
    parser.add_argument("--butler", action="store_true", help="Run in real-time 'Butler Mode' (watches for new files)")
    parser.add_argument("--watch-dirs", nargs="+", help="Specific folders for the Butler to watch (defaults to root)")
    parser.add_argument("--api-key", help="Gemini API Key for AI topic intelligence")
    
    args = parser.parse_args()

    # 1. Choose Path
    root = pick_drive(args.path)
    
    # 2. Setup AI
    api_key = args.api_key or os.environ.get("GEMINI_API_KEY")
    classifier = AIClassifier(api_key)

    # 3. Handle Butler Mode
    if args.butler:
        watch_dirs = [Path(d) for d in args.watch_dirs] if args.watch_dirs else [root]
        start_butler(root, classifier, watch_dirs)
        return

    start_time = time.time()

    # 2. Scan
    files = scan_files(root)
    if not files:
        print("  No files found. Exiting.")
        sys.exit(0)

    # 3. Hash & find duplicates (Optimized)
    dupes = find_duplicates(files)

    # 4. Preview and Confirm Deletion
    deleted = []
    if dupes:
        preview_duplicates(dupes)
        
        if args.dry_run:
            print("\n⚠  DRY RUN: Simulating deletion of duplicates.")
            deleted = delete_duplicates(dupes, dry_run=True)
        elif args.auto_delete:
            print("\n⚠  AUTO-DELETE ON: Unlinking duplicates without prompting.")
            deleted = delete_duplicates(dupes)
        else:
            print("\n⚠  The files listed as DELETE will be PERMANENTLY removed.")
            confirm = input("   Type  YES  to delete duplicates, or anything else to skip: ").strip()
            if confirm.upper() == "YES":
                deleted = delete_duplicates(dupes)
                print(f"\n  ✓ Deleted {len(deleted)} duplicate file(s).")
            else:
                print("  ↩  Skipped deletion.")

    # 5. Rebuild live file list (exclude deleted)
    deleted_set = set(str(p) for p in deleted)
    remaining = [p for p in files if str(p) not in deleted_set]
    
    # 6. Organize Files
    move_log = {}
    if remaining:
        if args.dry_run:
             print(f"\n📋  DRY RUN: Simulating organization of {len(remaining)} file(s).")
             move_log = organize_files(root, remaining, dry_run=True)
        elif args.auto_organize:
            print(f"\n📋  AUTO-ORGANIZE ON: Assorting {len(remaining)} file(s).")
            move_log = organize_files(root, remaining)
        else:
            print(f"\n📋  {len(remaining)} file(s) will be organized into smart category folders.")
            confirm2 = input("   Type  YES  to organize, or anything else to skip: ").strip()
            if confirm2.upper() == "YES":
                move_log = organize_files(root, remaining)
            else:
                print("  ↩  Skipped organization.")

    # 7. Clean up empty folders left behind
    clean_empty_directories(root, dry_run=args.dry_run)

    # 8. Save report
    save_report(root, deleted, move_log, dupes, dry_run=args.dry_run)

    duration = time.time() - start_time
    print(f"\n🎉  All done in {duration:.1f} seconds!")
    if args.dry_run:
        print("    (This was a DRY RUN. No files were actually changed.)\n")
    else:
        print("    Your drive is clean and beautifully organized.\n")


if __name__ == "__main__":
    # Ensure standard output plays nicely with Windows special characters
    if sys.platform == "win32":
        os.system("color")
    main()
