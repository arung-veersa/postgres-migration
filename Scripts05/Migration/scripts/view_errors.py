"""
View errors from the log file.

Usage:
    python scripts/view_errors.py [--lines 50]
"""

import sys
from pathlib import Path
import argparse

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import LOG_FILE, PROJECT_ROOT


def view_errors(num_lines=100):
    """View errors from the log file."""
    log_path = PROJECT_ROOT / LOG_FILE
    
    if not log_path.exists():
        print(f"Log file not found: {log_path}")
        return
    
    print(f"Reading last {num_lines} lines from: {log_path}")
    print("="*80)
    print()
    
    with open(log_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    # Show last N lines
    for line in lines[-num_lines:]:
        print(line, end='')
    
    print()
    print("="*80)
    print(f"\nTotal lines in log: {len(lines)}")
    
    # Count errors
    error_count = sum(1 for line in lines if ' - ERROR - ' in line)
    warning_count = sum(1 for line in lines if ' - WARNING - ' in line)
    
    print(f"Errors: {error_count}")
    print(f"Warnings: {warning_count}")
    print(f"\nFull log file: {log_path}")


def main():
    parser = argparse.ArgumentParser(description='View errors from ETL log file')
    parser.add_argument('--lines', '-n', type=int, default=100,
                        help='Number of lines to show (default: 100)')
    parser.add_argument('--errors-only', '-e', action='store_true',
                        help='Show only ERROR lines')
    
    args = parser.parse_args()
    
    if args.errors_only:
        view_errors_only()
    else:
        view_errors(args.lines)


def view_errors_only():
    """View only error lines from the log file."""
    log_path = PROJECT_ROOT / LOG_FILE
    
    if not log_path.exists():
        print(f"Log file not found: {log_path}")
        return
    
    print(f"Showing ERROR lines from: {log_path}")
    print("="*80)
    print()
    
    with open(log_path, 'r', encoding='utf-8') as f:
        for line in f:
            if ' - ERROR - ' in line:
                print(line, end='')
    
    print()
    print("="*80)


if __name__ == '__main__':
    main()

