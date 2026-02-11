# Test Suite Documentation

**Date:** 2026-02-11
**Status:** 81 pytest tests passing

---

## Overview

The test suite validates the conditional update logic and core utility functions without requiring database connections. This enables fast, reliable testing of business logic independently from infrastructure.

---

## Test Files

### 1. test_conditional_logic.py (Original - Basic)

**Coverage:**
- Basic conditional logic scenarios (4 scenarios)
- CONFLICTID preservation (3 cases)

**Tests:**
- StatusFlag conditional update
- Rule flag conditional update (2 flags)
- CONFLICTID preservation

**Lines:** 226
**Runtime:** < 1 second
**Status:** All passing

### 2. test_comprehensive.py (Enhanced - Complete)

**Coverage:**
- All 7 rule flags
- StatusFlag edge cases
- WHERE clause matching
- Utility functions
- Data extraction
- Edge cases

**Tests:** 10 test suites with 50+ individual assertions

**Lines:** 500+
**Runtime:** < 1 second
**Status:** All passing (10/10)

---

## Test Suite Breakdown

### TEST 1: All Conditional Flags (7 flags)

**What it tests:**
- All 7 conflict rule flags
- Three scenarios: all N, all Y, mixed

**Validates:**
- Flags with 'N' are updated to detected value
- Flags with 'Y' are preserved (not overwritten)
- Mixed scenarios work correctly

**Flags tested:**
1. SameSchTimeFlag
2. SameVisitTimeFlag
3. SchAndVisitTimeSameFlag
4. SchOverAnotherSchTimeFlag
5. VisitTimeOverAnotherVisitTimeFlag
6. SchTimeOverVisitTimeFlag
7. DistanceFlag

### TEST 2: StatusFlag Edge Cases

**What it tests:**
- StatusFlag behavior with all possible values
- Edge cases: None, empty string, unknown values, case sensitivity

**Test cases:**
- 'N' -> 'U' (New to Updated)
- 'U' -> 'U' (Updated remains Updated)
- 'W' -> 'W' (Whitelist preserved)
- 'I' -> 'I' (Ignore preserved)
- '' -> 'U' (Empty defaults to Updated)
- None -> 'U' (None defaults to Updated)
- 'X' -> 'U' (Unknown defaults to Updated)
- 'w' -> 'U' (Case-sensitive, lowercase not preserved)
- 'i' -> 'U' (Case-sensitive, lowercase not preserved)

### TEST 3: WHERE Clause Matching

**What it tests:**
- (VisitID, ConVisitID) tuple key matching
- NULL ConVisitID handling

**Test cases:**
- Both IDs match -> Found
- Both IDs NULL -> Found
- One ID matches, other different -> Not found
- ConVisitID NULL vs non-NULL -> Not found

### TEST 4: format_exclusion_list Utility

**What it tests:**
- SQL IN clause formatting
- SQL injection prevention (quote escaping)

**Test cases:**
- Normal list: `['123', '456']` -> `'123','456'`
- Empty list: `[]` -> `''`
- Quote escape: `["O'Reilly"]` -> `'O''Reilly'`
- Single item: `['123']` -> `'123'`
- Integer conversion: `[123]` -> `'123'`
- Empty strings: `['', 'valid']` -> `'','valid'`

### TEST 5: MPH Lookup Injection

**What it tests:**
- MPH data injection as inline CTE
- UNION ALL generation
- Dummy row for empty data

### TEST 6: UPDATE Statement Structure

**What it tests:**
- UPDATE statement generation
- SET clause inclusion/exclusion logic
- Parameter count validation

### TEST 7: Batch Key Generation

**What it tests:**
- (VisitID, ConVisitID) tuple generation
- Type conversions

### TEST 8: Duration Formatting

**What it tests:**
- format_duration() utility function
- Human-readable time formatting

### TEST 9: Memory Estimation

**What it tests:**
- estimate_memory_mb() utility function
- Memory usage calculation

### TEST 10: Reference Data Extraction

**What it tests:**
- Database result tuple to Python structure conversion
- None/empty filtering

---

## Running the Tests

### Quick Test (Original)

```bash
cd Scripts13/tasks/tests
python test_conditional_logic.py
```

**Output:** 2 test suites, ~10 assertions
**Runtime:** < 1 second
**Coverage:** Basic conditional logic

### Comprehensive Test (Enhanced)

```bash
cd Scripts13/tasks/tests
python test_comprehensive.py
```

**Output:** 10 test suites, 50+ assertions
**Runtime:** < 1 second
**Coverage:** Complete logic and utilities

### Full Suite (pytest)

```bash
cd Scripts13/tasks
python -m pytest tests/ -v
```

**Output:** 81 tests
**Runtime:** < 2 seconds

---

## Test Coverage Summary

| Component | Original Test | Comprehensive Test |
|-----------|---------------|-------------------|
| StatusFlag Logic | Basic (4 cases) | Complete (9 edge cases) |
| Rule Flags | 2 flags | All 7 flags |
| CONFLICTID | 3 cases | Integrated in matching test |
| WHERE Clause | Implicit | Explicit (5 cases) |
| Utility Functions | None | All 4 functions |
| MPH Injection | None | Complete (4 cases) |
| UPDATE Structure | None | Complete (2 scenarios) |
| Key Generation | None | Complete (4 cases) |
| Data Extraction | None | Complete (5 patterns) |

---

## What's Tested (Comprehensive)

- **Conditional Logic:** All 7 rule flags, StatusFlag edge cases, CONFLICTID preservation
- **Data Structures:** Tuple key generation, WHERE clause matching, reference data extraction
- **Utility Functions:** format_exclusion_list, format_duration, estimate_memory_mb
- **SQL Generation:** MPH lookup CTE injection, UPDATE statement structure

---

## What's NOT Tested (Requires Integration)

### Database Connectivity
- Snowflake connection
- PostgreSQL connection
- VPC/network issues

### Query Execution
- SQL syntax errors
- Query performance
- Result set handling

### Integration
- Container entry point (`main.py`)
- Environment variable handling
- AWS permissions

**For integration testing, use:**
- `validate_config` action (verifies config loading)
- `test_connections` action (verifies database connectivity)
- Small dataset test runs via `build-and-push-ecr.ps1`

---

## How to Extend Tests

### Adding New Test Case

```python
def test_new_feature():
    """Test description"""
    print("=" * 70)
    print("TEST N: NEW FEATURE")
    print("=" * 70)

    test_cases = [
        {
            'input': ...,
            'expected': ...,
            'description': '...'
        }
    ]

    all_passed = True

    for test_case in test_cases:
        result = your_function(test_case['input'])

        if result == test_case['expected']:
            print(f"  [PASS] {test_case['description']}")
        else:
            print(f"  [FAIL] {test_case['description']}")
            all_passed = False

    print("=" * 70)
    print(f"TEST N: {'PASSED' if all_passed else 'FAILED'}")
    print("=" * 70)

    return all_passed
```

---

**Version**: 2.0
**Last Updated**: 2026-02-11
