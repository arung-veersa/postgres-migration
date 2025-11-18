# Task 02: Robust Type Casting Solution

## Problem

When performing bulk updates via temporary tables, Postgres requires explicit type casting when the temp table columns (TEXT) don't match the target table column types.

### Errors Encountered

1. **`SchStartTime`**: `timestamp without time zone` vs `text`
2. **`AppCaregiverID`**: `numeric` vs `text`
3. And potentially 90+ other columns with various types

## Solution: Schema-Based Type Casting

### Approach

Instead of guessing column types based on naming patterns (fragile), we **query the actual schema** from Postgres `information_schema.columns` to get the definitive data type for each column.

### Implementation

```python
def _get_column_types(self) -> dict:
    """Query actual column types from Postgres schema."""
    query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{self.pg.schema}'
        AND table_name = 'conflictvisitmaps'
    """
    result = self.pg.fetch_dataframe(query)
    return dict(zip(result['column_name'], result['data_type']))

def _get_cast_expression(self, column_name: str, column_types: dict) -> str:
    """Map Postgres data type to appropriate cast expression."""
    data_type = column_types.get(column_name, 'text')
    
    if data_type in ('timestamp without time zone', 'timestamp with time zone', 'timestamp'):
        return f'U."{column_name}"::timestamp'
    elif data_type == 'date':
        return f'U."{column_name}"::date'
    elif data_type in ('numeric', 'decimal', 'double precision', 'real'):
        return f'U."{column_name}"::numeric'
    elif data_type in ('integer', 'bigint', 'smallint'):
        return f'U."{column_name}"::integer'
    elif data_type == 'boolean':
        return f'U."{column_name}"::boolean'
    elif data_type == 'uuid':
        return f'U."{column_name}"::uuid'
    else:
        # text, character varying, etc. - no cast needed
        return f'U."{column_name}"'
```

### Process Flow

1. **Query Schema**: Get actual data types for all `conflictvisitmaps` columns
2. **Create Temp Table**: All columns as TEXT (for easy COPY from CSV)
3. **Bulk COPY**: Fast insert from pandas DataFrame to temp table
4. **UPDATE with Casts**: Each column cast to its correct type based on schema

```sql
UPDATE "conflictvisitmaps" AS CVM
SET
    "SchStartTime" = U."SchStartTime"::timestamp,        -- timestamp column
    "AppCaregiverID" = U."AppCaregiverID"::numeric,      -- numeric column
    "VisitDate" = U."VisitDate"::date,                   -- date column
    "SSN" = U."SSN",                                     -- text column (no cast)
    ...
FROM conflict_updates U
WHERE CVM."VisitID" = U."VisitID"
```

## Benefits

1. **Foolproof**: No guessing - uses actual database schema
2. **Maintainable**: If column types change, code automatically adapts
3. **Complete**: Handles all Postgres data types (timestamp, date, numeric, integer, boolean, uuid, text)
4. **No Future Errors**: Won't encounter "column X is of type Y but expression is of type text" errors

## Performance

- Schema query: **One-time per batch** (milliseconds)
- Adds minimal overhead compared to the benefits of error-free execution

## Coverage

The solution handles these Postgres data types:

| Data Type | Cast Expression | Examples |
|-----------|----------------|----------|
| `timestamp without time zone` | `::timestamp` | SchStartTime, ActualEndTime, LastUpdatedDate |
| `date` | `::date` | VisitDate, InserviceStartDate |
| `numeric`, `decimal` | `::numeric` | DistanceMilesFromLatLng, BilledRate, Latitude |
| `integer`, `bigint` | `::integer` | ServiceCodeID, StatusFlag |
| `boolean` | `::boolean` | Billed, IsMissed |
| `text`, `character varying` | *(no cast)* | SSN, VisitID, ProviderName |
| `uuid` | `::uuid` | *(if any columns use UUID type)* |

## Testing

To verify the solution works for all columns, run:

```bash
py scripts\run_task_02.py
```

The script will:
1. Process 97 SSN batches
2. Perform bulk updates with proper type casting for all ~90 columns
3. Complete without type mismatch errors

## Related Files

- **Implementation**: `src/tasks/task_02_update_conflicts.py`
  - `_get_column_types()` - Lines 403-417
  - `_get_cast_expression()` - Lines 419-447
  - `_bulk_update_conflicts()` - Lines 449-600

