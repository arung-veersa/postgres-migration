# TASK_02 Performance Optimization Guide

## Current Performance Issue

**Observed:** ~7 minutes to fetch 52K rows (98 columns) from Snowflake
**Expected:** Should be < 1 minute for this data volume

## Root Causes

1. **Complex Query:** 12 table joins with 2 window functions (ROW_NUMBER)
2. **String Operations:** TRIM() and LIKE on large datasets
3. **Function on Indexed Column:** DATE(CR1."Visit Date") prevents index usage
4. **Subquery Execution:** Two expensive ROW_NUMBER() OVER subqueries
5. **SELECT DISTINCT:** May be causing unnecessary deduplication scan

## Optimization Strategies

### **Strategy 1: Create Snowflake View (RECOMMENDED)**

Create a pre-joined view in Snowflake to avoid repeating complex joins.

```sql
-- Create in Snowflake Analytics database
CREATE OR REPLACE VIEW ANALYTICS.BI.VW_VISIT_BASE_ENRICHED AS
SELECT 
    CR1."Visit Id" AS "VisitID",
    CR1."Application Visit Id" AS "AppVisitID",
    DATE(CR1."Visit Date") AS "VisitDate",
    TRIM(CAR."SSN") AS "SSN",
    -- ... all other columns with joins pre-computed
FROM ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR AS CR1
INNER JOIN ANALYTICS.BI.DIMCAREGIVER AS CAR 
    ON CAR."Caregiver Id" = CR1."Caregiver Id"
-- ... all joins
WHERE TRIM(CAR."SSN") IS NOT NULL 
AND TRIM(CAR."SSN") != '';

-- Then in Python, query becomes:
SELECT * FROM ANALYTICS.BI.VW_VISIT_BASE_ENRICHED
WHERE "VisitDate" BETWEEN '2023-11-15' AND '2025-12-29'
AND "SSN" IN (...)
```

**Benefits:**
- Query compilation happens once
- Result caching works better
- Simpler Python query
- Can add clustering keys to view

**Estimated Impact:** 50-70% faster

---

### **Strategy 2: Remove DISTINCT (if possible)**

```python
# Current query has SELECT DISTINCT - check if needed
# If the joins don't create duplicates, remove it
query = f"""
    SELECT  -- Remove DISTINCT
        CR1."Bill Rate Non-Billed" AS "BillRateNonBilled",
        ...
```

**How to verify:**
```sql
-- Run this in Snowflake:
SELECT COUNT(*) as total_rows,
       COUNT(DISTINCT "Visit Id") as distinct_visits
FROM ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR
WHERE DATE("Visit Date") BETWEEN '2023-11-15' AND '2025-12-29';
```

If `total_rows` == `distinct_visits`, you don't need DISTINCT.

**Estimated Impact:** 10-20% faster

---

### **Strategy 3: Optimize Date Filter**

```python
# Current (BAD - function on indexed column):
WHERE DATE(CR1."Visit Date") BETWEEN '{date_from_str}' AND '{date_to_str}'

# Optimized (GOOD - allows index usage):
WHERE CR1."Visit Date" >= '{date_from_str}' 
AND CR1."Visit Date" < DATE_ADD('{date_to_str}', INTERVAL 1 DAY)
```

**Estimated Impact:** 15-25% faster

---

### **Strategy 4: Reduce Columns Fetched**

Only fetch columns actually used in conflict detection:

```python
# Instead of SELECT * (98 columns), select only what's needed
essential_columns = [
    # Identity
    'VisitID', 'AppVisitID', 'SSN', 'ProviderID', 'VisitDate',
    # Times
    'SchStartTime', 'SchEndTime', 'VisitStartTime', 'VisitEndTime',
    # Location
    'PLongitude', 'PLatitude', 'PZipCode',
    # Other essentials
    'CaregiverID', 'PatientID', 'PayerID',
    # Add only what conflict rules need
]
```

**Estimated Impact:** 20-30% faster

---

### **Strategy 5: Push Filters into Subqueries**

```python
# Current (BAD):
LEFT JOIN (
    SELECT ... ROW_NUMBER() OVER (...)
    FROM ANALYTICS.BI.DIMPATIENTADDRESS AS DDD
    WHERE DDD."Primary Address" = TRUE 
    AND DDD."Address Type" LIKE '%GPS%'
) AS DPAD_P

# Optimized (GOOD):
LEFT JOIN (
    SELECT ... ROW_NUMBER() OVER (...)
    FROM ANALYTICS.BI.DIMPATIENTADDRESS AS DDD
    WHERE DDD."Primary Address" = TRUE 
    AND DDD."Address Type" = 'GPS'  -- Exact match vs LIKE
    AND DDD."Patient Id" IN (
        -- Pre-filter to only patients we need
        SELECT DISTINCT "Provider Patient Id" 
        FROM ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR
        WHERE DATE("Visit Date") BETWEEN '2023-11-15' AND '2025-12-29'
    )
) AS DPAD_P
```

**Estimated Impact:** 30-40% faster

---

### **Strategy 6: Parallel Batch Fetching**

Process multiple SSN batches in parallel:

```python
from concurrent.futures import ThreadPoolExecutor

def _process_batches_parallel(self, exclusions) -> int:
    """Process batches in parallel using thread pool."""
    
    ssn_batches = self._get_ssn_batches()
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        # Submit all batches
        futures = {
            executor.submit(self._process_single_batch, ssn, exclusions): ssn
            for ssn in ssn_batches
        }
        
        # Collect results
        total_updated = 0
        for future in as_completed(futures):
            try:
                updated = future.result()
                total_updated += updated
            except Exception as e:
                ssn = futures[future]
                self.logger.error(f"Batch {ssn} failed: {e}")
    
    return total_updated
```

**Estimated Impact:** 2-3x faster overall (with 3 workers)

---

### **Strategy 7: Snowflake Warehouse Size**

```python
# Check current warehouse size
SELECT CURRENT_WAREHOUSE(), SYSTEM$WAREHOUSE_SIZE();

# Increase warehouse size temporarily for large operations
ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'LARGE';

# Run your query

# Scale back down
ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'SMALL';
```

**Cost vs Performance:**
- X-Small: 1 credit/hour
- Small: 2 credits/hour
- Medium: 4 credits/hour
- Large: 8 credits/hour

**Estimated Impact:** 2-4x faster (Medium → Large)

---

### **Strategy 8: Snowflake Result Caching**

```python
# Ensure result caching is enabled (default, but verify)
ALTER SESSION SET USE_CACHED_RESULT = TRUE;

# For repeated queries, results are cached for 24 hours
# Subsequent executions will be near-instant
```

**Estimated Impact:** Nearly instant on cache hits

---

### **Strategy 9: Clustering Keys (DBA Task)**

```sql
-- Add clustering key to fact table
ALTER TABLE ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR 
CLUSTER BY (TO_DATE("Visit Date"), "Provider Id");

-- Check clustering quality
SELECT SYSTEM$CLUSTERING_INFORMATION('FACTVISITCALLPERFORMANCE_CR');
```

**Estimated Impact:** 40-60% faster on clustered queries

---

## **Recommended Implementation Order**

### **Quick Wins (< 1 hour):**

1. ✅ **Remove DISTINCT** (if not needed)
   ```python
   # In analytics_repository.py, line 130
   SELECT  # Remove DISTINCT
   ```

2. ✅ **Optimize Date Filter**
   ```python
   # Line 319
   WHERE CR1."Visit Date" >= '{date_from_str}' 
   AND CR1."Visit Date" < '{date_to_str_plus_1}'
   ```

3. ✅ **Change LIKE to Exact Match**
   ```python
   # Lines 275, 299
   AND DDD."Address Type" = 'GPS'  -- Instead of LIKE '%GPS%'
   ```

**Expected Combined Impact:** 30-50% faster

---

### **Medium Effort (2-4 hours):**

4. ✅ **Reduce Columns**
   - Identify minimum required columns
   - Update query to SELECT only those

5. ✅ **Push Filters into Subqueries**
   - Add Patient Id filters to address subqueries

6. ✅ **Parallel Processing**
   - Implement ThreadPoolExecutor for batch processing

**Expected Combined Impact:** 2-3x faster total pipeline

---

### **Long Term (DBA Coordination):**

7. ✅ **Create Snowflake View**
   - Work with DBA to create optimized view
   - Update Python to use view

8. ✅ **Add Clustering Keys**
   - DBA task to cluster fact table

9. ✅ **Increase Warehouse Size**
   - Evaluate cost vs performance tradeoff

**Expected Combined Impact:** 5-10x faster

---

## **Quick Implementation: Optimized Query**

Replace the query in `analytics_repository.py` with this optimized version:

```python
def fetch_visit_data(self, date_from, date_to, ssns: list, ...):
    # Convert dates
    date_from_str = date_from.strftime('%Y-%m-%d')
    date_to_str = (date_to + timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Build SSN list
    ssns_str = ','.join([f"'{s}'" for s in ssns])
    
    # OPTIMIZED QUERY
    query = f"""
        SELECT  -- Removed DISTINCT
            CR1."Bill Rate Non-Billed" AS "BillRateNonBilled",
            -- ... all columns (same as before)
        FROM ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR AS CR1
        INNER JOIN ANALYTICS.BI.DIMCAREGIVER AS CAR 
            ON CAR."Caregiver Id" = CR1."Caregiver Id"
            AND TRIM(CAR."SSN") IS NOT NULL 
            AND TRIM(CAR."SSN") != ''
        -- ... all other joins (same as before)
        WHERE CR1."Visit Date" >= '{date_from_str}'  -- Optimized
        AND CR1."Visit Date" < '{date_to_str}'       -- Optimized
        AND TRIM(CAR."SSN") IN ({ssns_str})
        {excluded_agencies_clause}
        {excluded_ssns_clause}
    """
```

---

## **Performance Monitoring**

Add timing to track improvements:

```python
import time

def fetch_visit_data(self, ...):
    start_time = time.time()
    
    logger.info(f"Fetching visit data for {len(ssns)} SSNs from Analytics")
    df = self.fetch_dataframe(query, params=None)
    
    elapsed = time.time() - start_time
    logger.info(f"Fetched {len(df)} visits from Analytics in {elapsed:.2f} seconds")
    
    # Log performance metrics
    rows_per_second = len(df) / elapsed if elapsed > 0 else 0
    logger.info(f"Performance: {rows_per_second:.0f} rows/second")
    
    return df
```

---

## **Expected Results**

| Optimization | Current | After Quick Wins | After Medium | After Long Term |
|--------------|---------|------------------|--------------|-----------------|
| **Time**     | 7 min   | 3-4 min         | 1-2 min      | < 30 sec       |
| **Rows/sec** | ~124    | ~250            | ~500         | ~2000          |


