"""
Quick local testing script for Lambda handler.
Tests both mock mode (fast) and real database mode.

Usage:
    python scripts/test_lambda_locally.py
    python scripts/test_lambda_locally.py --skip-real  # Skip real DB test
"""

import sys
import time
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scripts.lambda_handler import lambda_handler
from config.settings import POSTGRES_CONFIG


def print_header(title: str):
    """Print test section header."""
    print()
    print("=" * 70)
    print(title)
    print("=" * 70)


def print_result(test_name: str, passed: bool, duration: float, details: str = ""):
    """Print test result."""
    icon = "✅" if passed else "❌"
    status = "PASSED" if passed else "FAILED"
    print(f"{icon} {test_name}: {status} ({duration:.3f}s)")
    if details:
        print(f"   {details}")


def test_validate_config():
    """Test configuration validation."""
    print("\nTest 1: Validate Configuration")
    print("-" * 70)
    
    event = {'action': 'validate_config'}
    
    start = time.time()
    result = lambda_handler(event, None)
    duration = time.time() - start
    
    passed = result['statusCode'] == 200 and result['body']['status'] == 'success'
    print_result("Config Validation", passed, duration)
    
    return passed


def test_task_01_mock():
    """Test Task 01 with mock database (fast)."""
    print("\nTest 2: Task 01 - Mock Mode")
    print("-" * 70)
    
    event = {'action': 'task_01', 'use_mock': True}
    
    start = time.time()
    result = lambda_handler(event, None)
    duration = time.time() - start
    
    passed = result['statusCode'] == 200 and result['body']['status'] == 'success'
    
    details = ""
    if passed and 'result' in result['body']:
        rows = result['body']['result'].get('affected_rows', 'N/A')
        details = f"Rows affected: {rows}"
    
    print_result("Task 01 (Mock)", passed, duration, details)
    
    return passed


def test_task_02_mock():
    """Test Task 02 with mock database (fast)."""
    print("\nTest 3: Task 02 - Mock Mode")
    print("-" * 70)
    
    event = {'action': 'task_02', 'use_mock': True}
    
    start = time.time()
    result = lambda_handler(event, None)
    duration = time.time() - start
    
    passed = result['statusCode'] == 200 and result['body']['status'] == 'success'
    
    details = ""
    if passed and 'result' in result['body']:
        rows = result['body']['result'].get('updated_rows', 'N/A')
        details = f"Rows updated: {rows}"
    
    print_result("Task 02 (Mock)", passed, duration, details)
    
    return passed


def test_task_01_real():
    """Test Task 01 with real database."""
    print("\nTest 4: Task 01 - Real Database")
    print("-" * 70)
    print(f"Database: {POSTGRES_CONFIG.get('database', 'N/A')}")
    print(f"Host: {POSTGRES_CONFIG.get('host', 'N/A')}")
    print()
    
    event = {'action': 'task_01', 'use_mock': False}
    
    start = time.time()
    result = lambda_handler(event, None)
    duration = time.time() - start
    
    passed = result['statusCode'] == 200 and result['body']['status'] == 'success'
    
    details = ""
    if passed and 'result' in result['body']:
        rows = result['body']['result'].get('affected_rows', 'N/A')
        task_duration = result['body'].get('duration_seconds', 0)
        details = f"Rows affected: {rows}, Duration: {task_duration:.2f}s"
    elif not passed:
        error = result['body'].get('error', 'Unknown error')
        details = f"Error: {error}"
    
    print_result("Task 01 (Real DB)", passed, duration, details)
    
    return passed


def test_task_02_real():
    """Test Task 02 with real database."""
    print("\nTest 5: Task 02 - Real Database")
    print("-" * 70)
    print(f"Database: {POSTGRES_CONFIG.get('database', 'N/A')}")
    print(f"Host: {POSTGRES_CONFIG.get('host', 'N/A')}")
    print()
    
    event = {'action': 'task_02', 'use_mock': False}
    
    start = time.time()
    result = lambda_handler(event, None)
    duration = time.time() - start
    
    passed = result['statusCode'] == 200 and result['body']['status'] == 'success'
    
    details = ""
    if passed and 'result' in result['body']:
        rows = result['body']['result'].get('updated_rows', 'N/A')
        task_duration = result['body'].get('duration_seconds', 0)
        details = f"Rows updated: {rows}, Duration: {task_duration:.2f}s"
    elif not passed:
        error = result['body'].get('error', 'Unknown error')
        details = f"Error: {error}"
    
    print_result("Task 02 (Real DB)", passed, duration, details)
    
    return passed


def test_error_handling():
    """Test error handling with invalid action."""
    print("\nTest 6: Error Handling")
    print("-" * 70)
    
    event = {'action': 'invalid_action'}
    
    start = time.time()
    result = lambda_handler(event, None)
    duration = time.time() - start
    
    # Should return 400 for invalid action
    passed = result['statusCode'] == 400
    print_result("Error Handling", passed, duration)
    
    return passed


def main():
    """Run all tests."""
    print_header("LAMBDA HANDLER - LOCAL TEST SUITE")
    
    # Check for skip flag
    skip_real_db = '--skip-real' in sys.argv
    
    # Track results
    results = []
    
    # Fast tests (mock mode)
    print("\n📦 FAST TESTS (Mock Database)")
    print("=" * 70)
    results.append(test_validate_config())
    results.append(test_task_01_mock())
    results.append(test_task_02_mock())
    results.append(test_error_handling())
    
    # Real database tests
    if not skip_real_db:
        print("\n")
        print("🗄️  INTEGRATION TESTS (Real Database)")
        print("=" * 70)
        print("⚠️  WARNING: The following tests will modify the database")
        print()
        
        response = input("Continue with real database tests? (y/n): ")
        
        if response.lower() == 'y':
            results.append(test_task_01_real())
            results.append(test_task_02_real())
        else:
            print("Skipped real database tests")
    else:
        print("\n")
        print("🗄️  INTEGRATION TESTS (Real Database) - SKIPPED")
        print("=" * 70)
        print("Use without --skip-real flag to run real database tests")
    
    # Summary
    print()
    print("=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    
    passed_count = sum(results)
    total_count = len(results)
    
    print(f"Passed: {passed_count}/{total_count}")
    
    if passed_count == total_count:
        print("✅ All tests passed!")
        exit_code = 0
    else:
        print("❌ Some tests failed")
        exit_code = 1
    
    print("=" * 70)
    
    sys.exit(exit_code)


if __name__ == '__main__':
    main()

