# TASK_02 Implementation Guide

## Overview

TASK_02 updates `CONFLICTVISITMAPS` with fresh data from Snowflake Analytics, detecting conflicts using 7 different rules.

**Migrated from:** `Scripts05/Snowflake/Files/TASK_02_UPDATE_DATA_CONFLICTVISITMAPS_0.sql`

## Architecture

```
┌────────────────────────────────────┐
│  POSTGRES (ConflictReport)         │
│  ┌──────────────────────────────┐  │
│  │ vw_conflictvisitmaps_base    │  │  ← Filters visits to update
│  └──────────────────────────────┘  │
└────────────────────────────────────┘
              ↓ Fetch via Python
┌────────────────────────────────────┐
│  PYTHON (Task02)                   │
│  - Fetch from Postgres view        │
│  - Fetch from Snowflake Analytics  │
│  - Join to create V1 & V2          │  ← Join in pandas
│  - Calculate conflicts (7 rules)   │
│  - Bulk update Postgres            │
└────────────────────────────────────┘
              ↑ Fetch via AnalyticsRepository
┌────────────────────────────────────┐
│  SNOWFLAKE (Analytics)             │
│  - FACTVISITCALLPERFORMANCE_CR     │
│  - DIM tables (joined)             │
└────────────────────────────────────┘
```

## Key Components

### 1. Postgres View
**File:** `sql/views/vw_conflictvisitmaps_base.sql`

Encapsulates filtering logic for which visits need updating:
- Has CONFLICTID
- UpdateFlag = 1
- No Inservice or PTO dates

### 2. Main Task Class
**File:** `src/tasks/task_02_update_conflicts.py`

Orchestrates the entire process following Task 01 pattern.

### 3. Utility Classes

#### ConflictCalculator
**File:** `src/utils/conflict_calculator.py`

Orchestrates conflict detection:
- Merges V1 and V2
- Applies all 7 rules
- Calculates derived fields
- Prepares update data

#### ConflictRules
**File:** `src/utils/conflict_rules.py`

Implements the 7 conflict detection rules:

| Rule | Description | SQL Line Reference |
|------|-------------|-------------------|
| 1 | Same Scheduled Time | 336-346 |
| 2 | Same Visit Time | 349-359 |
| 3 | Schedule Time = Visit Time | 362-381 |
| 4 | Schedule Overlaps Schedule | 384-399 |
| 5 | Visit Time Overlaps Visit Time | 402-413 |
| 6 | Schedule Overlaps Visit Time | 416-449 |
| 7 | Distance Flag (impossible travel) | 452-496 |

#### GeospatialUtils
**File:** `src/utils/geospatial_utils.py`

Handles distance and ETA calculations:
- Haversine distance formula
- ETA calculations
- Vectorized operations for performance

### 4. Analytics Repository Extension
**File:** `src/repositories/analytics_repository.py`

New method: `fetch_visit_data()`
- Fetches from FACTVISITCALLPERFORMANCE_CR
- Includes all dimension joins
- Supports filtering by SSNs and exclusions

## Execution Flow

### Step 1: Mark Records for Update
```sql
UPDATE conflictvisitmaps 
SET UpdateFlag = 1 
WHERE CONFLICTID IS NOT NULL 
AND VisitDate BETWEEN date_from AND date_to
```

### Step 2: Get Exclusions
- Fetch from `excluded_agency` table
- Fetch from `excluded_ssn` table

### Step 3: Process in Batches
For each SSN prefix (batching strategy):

#### 3a. Fetch Conflict Visits (Postgres)
```sql
SELECT * FROM vw_conflictvisitmaps_base
WHERE SSN LIKE 'AB%'
```

#### 3b. Fetch Analytics Data (Snowflake)
```sql
SELECT ... FROM FACTVISITCALLPERFORMANCE_CR
-- Complex query with 10+ dimension joins
WHERE SSN IN (list_from_step_3a)
```

#### 3c. Create V1 and V2 (Python)
```python
# V1 = Visits with CONFLICTID + Analytics data
v1_df = conflict_visits_df.merge(analytics_df, on=['VisitID', 'AppVisitID'])

# V2 = All Analytics visits (for conflict detection)
v2_df = analytics_df.copy()
```

#### 3d. Calculate Conflicts
```python
conflicts = conflict_calculator.calculate_conflicts(v1_df, v2_df, settings, mph_df)
```

Process:
1. Merge V1 and V2 on VisitDate, SSN (different VisitID, different ProviderID)
2. Apply all 7 rules
3. Filter to only conflicting pairs
4. Calculate derived fields (distance, time diffs, etc.)

#### 3e. Bulk Update
```python
# Use temp table pattern (like Task 01)
1. CREATE TEMP TABLE conflict_updates
2. COPY data to temp table
3. UPDATE conflictvisitmaps FROM conflict_updates
```

## Mapping to Original SQL

| Original SQL | Python Implementation |
|--------------|----------------------|
| Lines 8-9: SET UpdateFlag | `_set_update_flag()` |
| Lines 278-303: V1 query | `analytics_repo.fetch_visit_data()` + `_create_v1_v2()` |
| Lines 306-329: V2 query | `analytics_repo.fetch_visit_data()` |
| Lines 295: JOIN with CONFLICTVISITMAPS | `_create_v1_v2()` merge in pandas |
| Lines 331: V1 JOIN V2 | `conflict_calculator._merge_v1_v2()` |
| Lines 120-126: Flag calculations | `ConflictRules.rule_*()` methods |
| Lines 334-497: WHERE clause (7 rules) | `conflict_calculator._filter_conflicting_pairs()` |
| Lines 98-108: MinuteDiffBetweenSch | `conflict_calculator._calculate_minute_diff()` |
| Lines 109-111: Distance/ETA | `GeospatialUtils` methods |
| Lines 12-13: UPDATE statement | `_bulk_update_conflicts()` |

## Batching Strategy

**Why batch by SSN prefix?**
- Limits data transferred from Snowflake
- Keeps memory usage manageable
- Allows parallel processing (future enhancement)
- Natural partitioning of data

**Batch size:** Configurable via SSN prefix (default: 2 characters = ~100 batches)

## Performance Considerations

### Optimization Techniques
1. **Batching** - Process SSN prefixes sequentially
2. **Bulk operations** - Temp table pattern for updates
3. **Vectorized calculations** - NumPy/pandas for math operations
4. **View-based filtering** - Postgres view pre-filters data
5. **Minimal data transfer** - Only fetch needed SSNs from Snowflake

### Expected Performance
- Similar to Task 01 pattern
- Bulk operations avoid row-by-row loops
- Database-side operations where possible

## Usage

### Prerequisites
1. Postgres view must be created:
   ```bash
   psql -f sql/views/vw_conflictvisitmaps_base.sql
   ```

2. Configuration validated:
   ```bash
   python config/settings.py
   ```

### Running Task 02
```bash
# Windows
py scripts\run_task_02.py

# Linux/Mac
python scripts/run_task_02.py
```

### Output
```
============================================================
TASK_02: Update ConflictVisitMaps
============================================================
Step 1: Marking records for update
Step 2: Fetching exclusion lists
Step 3: Processing batches
============================================================
Batch 1/25: SSN prefix '12*'
============================================================
  Found 150 conflict visits in Postgres
  Fetched 300 visits from Analytics
  Created V1: 150 visits, V2: 300 visits
  Detected 45 conflicts
  Updated 45 records
============================================================
...
============================================================
All batches complete: 1250 total records updated
============================================================
✅ Task completed successfully
Duration: 125.34 seconds
```

## Testing

### Unit Tests
Test individual components:
```bash
pytest tests/unit/test_conflict_rules.py -v
pytest tests/unit/test_geospatial_utils.py -v
pytest tests/unit/test_conflict_calculator.py -v
```

### Integration Tests
Test end-to-end:
```bash
pytest tests/integration/test_task_02.py -v
```

### Manual Validation
Compare results with original Snowflake procedure:
```sql
-- Check row counts
SELECT COUNT(*) FROM conflictvisitmaps WHERE UpdateFlag IS NULL;

-- Validate conflict flags
SELECT 
    SUM(CASE WHEN "SameSchTimeFlag" = 'Y' THEN 1 ELSE 0 END) as rule1,
    SUM(CASE WHEN "SameVisitTimeFlag" = 'Y' THEN 1 ELSE 0 END) as rule2,
    ...
FROM conflictvisitmaps;
```

## Troubleshooting

### Common Issues

**Issue:** "No SSN batches found"
- **Cause:** No records with UpdateFlag = 1
- **Solution:** Check date range in settings

**Issue:** "No analytics data found"
- **Cause:** Snowflake connection or excluded SSNs/agencies
- **Solution:** Check exclusion lists and Snowflake connectivity

**Issue:** Memory errors
- **Cause:** Batch too large
- **Solution:** Increase SSN prefix granularity (use 3 characters instead of 2)

## Future Enhancements

1. **Parallel processing** - Process multiple SSN batches concurrently
2. **Incremental updates** - Only process changed visits
3. **Performance monitoring** - Add metrics collection
4. **Checkpoint/resume** - Allow resuming from interrupted batches
5. **Validation reports** - Generate reconciliation reports

## Related Files

```
Scripts05/Migration/
├── sql/
│   └── views/
│       └── vw_conflictvisitmaps_base.sql
├── src/
│   ├── tasks/
│   │   └── task_02_update_conflicts.py
│   ├── repositories/
│   │   └── analytics_repository.py (extended)
│   └── utils/
│       ├── conflict_calculator.py
│       ├── conflict_rules.py
│       └── geospatial_utils.py
└── scripts/
    └── run_task_02.py
```

## References

- Original SQL: `Scripts05/Snowflake/Files/TASK_02_UPDATE_DATA_CONFLICTVISITMAPS_0.sql`
- Task 01 Pattern: `docs/phase1_guide.md`
- Architecture: Discussion thread on approach selection

