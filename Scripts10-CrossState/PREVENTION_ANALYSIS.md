# Analysis: Preventing Cross-State Conflicts at Generation Time

## Current Situation

The three stored procedures (`TASK_03_INSERT_DATA_FROM_MAIN_TO_CONFLICTVISITMAPS_0/1/2`) generate conflicts by:
1. Self-joining visit data (V1 JOIN V2) based on SSN (same caregiver) and VisitDate
2. Creating conflict records for all overlapping visits
3. **No filtering based on state** during conflict generation

Currently:
- ✅ Conflicts are generated for all scenarios (same state + cross-state)
- ❌ Cross-state conflicts (~52,577 rows) need to be deleted after generation
- ❌ Inefficient: Generate then delete

## Proposed Solution

**Add state-based filtering during conflict generation** using the same V5_CORRECTED logic.

### Benefits
✅ **Prevention over deletion** - Don't create cross-state conflicts in the first place
✅ **Performance** - Reduce INSERT volume by ~52,577 rows
✅ **Data quality** - Cleaner data from the start
✅ **Consistency** - Same logic used everywhere

### Risks
⚠️ **Complexity** - Generation scripts become more complex
⚠️ **Maintenance** - State mapping logic needs to be in sync
⚠️ **Testing** - Need to verify all scenarios still work

---

## Technical Implementation

### Where to Add Filtering

All three scripts use the same pattern:
```sql
FROM V1
INNER JOIN V2
    ON V1.SSN = V2.SSN  
   AND V1.VisitDate = V2.VisitDate
   AND [other conditions]
WHERE [existing filters]
```

**Add after existing WHERE clause:**
```sql
AND NOT (  -- Exclude cross-state conflicts
    -- V5_CORRECTED logic here
)
```

### Logic to Add

Based on V5_CORRECTED, we need to check if the conflict is legitimate (same state or indeterminate).

**Key columns available in generation scripts:**
- `V1.P_PAddressState`, `V1.PA_PAddressState` (from DPAD_P, DPAD_PA)
- `V2.P_PAddressState`, `V2.PA_PAddressState`
- `V1.ProviderID`, `V2.ProviderID` (for fallback)

**State comparison logic:**
```sql
-- Clean and normalize states
V1_P_State_Clean = NULLIF(TRIM(COALESCE(StateMapping, UPPER(V1.P_PAddressState))), '')
V1_PA_State_Clean = NULLIF(TRIM(COALESCE(StateMapping, UPPER(V1.PA_PAddressState))), '')
V2_P_State_Clean = NULLIF(TRIM(COALESCE(StateMapping, UPPER(V2.P_PAddressState))), '')
V2_PA_State_Clean = NULLIF(TRIM(COALESCE(StateMapping, UPPER(V2.PA_PAddressState))), '')

-- Apply fallback
V1_P_Final = CASE WHEN V1_P_State_Clean IS NULL AND V1_PA_State_Clean IS NULL 
                  THEN V1_ProviderState ELSE V1_P_State_Clean END
V1_PA_Final = CASE WHEN V1_P_State_Clean IS NULL AND V1_PA_State_Clean IS NULL 
                   THEN V1_ProviderState ELSE V1_PA_State_Clean END
... (same for V2)

-- Check ANY-to-ANY match
Is_Same_State = 
    (V1_P_Final IS NULL AND V1_PA_Final IS NULL) OR (V2_P_Final IS NULL AND V2_PA_Final IS NULL)
    OR (V1_P_Final = V2_P_Final)
    OR (V1_P_Final = V2_PA_Final)
    OR (V1_PA_Final = V2_P_Final)
    OR (V1_PA_Final = V2_PA_Final)
```

---

## Implementation Approaches

### Option 1: Add CTE for State Filtering (Recommended)
**Pros:**
- ✅ Clean separation of concerns
- ✅ Easier to maintain and test
- ✅ Can be unit tested separately

**Cons:**
- ⚠️ Adds complexity to already large scripts

**Structure:**
```sql
WITH StateMapping AS (...),
     ProviderStates AS (...),
     V1 AS (... existing V1 query ...),
     V2 AS (... existing V2 query ...),
     StateFiltered AS (
         SELECT V1.*, V2.*
         FROM V1
         INNER JOIN V2 ON [existing join conditions]
         WHERE [existing filters]
           AND [state matching logic]
     )
INSERT INTO CONFLICTVISITMAPS
SELECT * FROM StateFiltered;
```

### Option 2: Inline WHERE Clause
**Pros:**
- ✅ Simpler implementation
- ✅ No structural changes

**Cons:**
- ❌ Very long WHERE clause
- ❌ Harder to maintain
- ❌ Duplicated logic across 3 scripts

### Option 3: Create a SQL Function
**Pros:**
- ✅ Reusable across all 3 scripts
- ✅ Single source of truth
- ✅ Easy to update

**Cons:**
- ⚠️ Requires CREATE FUNCTION permission
- ⚠️ Additional database object to manage

**Function signature:**
```sql
CREATE FUNCTION IS_SAME_STATE_CONFLICT(
    P1_P_State VARCHAR,
    P1_PA_State VARCHAR,
    P1_ProviderID VARCHAR,
    P2_P_State VARCHAR,
    P2_PA_State VARCHAR,
    P2_ProviderID VARCHAR
) RETURNS BOOLEAN
```

---

## Recommended Approach

**Hybrid: CTE + Function**

1. **Create a reusable function** `CONFLICTREPORT.PUBLIC.IS_LEGITIMATE_CONFLICT()` with the V5_CORRECTED logic
2. **Use the function in WHERE clause** of all 3 generation scripts
3. **Keep it simple** - function returns TRUE (keep) or FALSE (skip)

This provides:
- ✅ Single source of truth
- ✅ Easy to maintain
- ✅ Minimal changes to generation scripts
- ✅ Can be tested independently

---

## Validation Strategy

Before making changes, we need to:
1. ✅ **Backup** - Save copies of original scripts
2. ✅ **Test data** - Create test dataset with known cross-state scenarios
3. ✅ **Counts** - Document current conflict counts by type
4. ✅ **Verify** - Ensure modified scripts produce same count as (original - deleted)

**Test scenarios:**
- Same state (NY-NY): Should generate conflict
- Cross-state (NY-PA): Should NOT generate conflict
- NULL addresses with fallback: Handle per V5_CORRECTED logic
- Mixed (some same, some cross): Should generate conflict

---

## Migration Plan

### Phase 1: Create Function (Safe)
1. Create `IS_LEGITIMATE_CONFLICT()` function
2. Test function against existing CONFLICTVISITMAPS data
3. Verify results match V5_CORRECTED DELETE query

### Phase 2: Update One Script (Pilot)
1. Modify TASK_03_INSERT...\_2 (smallest, InService conflicts)
2. Run in test environment
3. Compare counts: before vs. after
4. Verify no legitimate conflicts are skipped

### Phase 3: Update Remaining Scripts
1. Apply same changes to \_0 and \_1
2. Full regression test
3. Deploy to production

### Phase 4: Clean Up
1. Run DELETE_V5_CORRECTED one final time (should delete 0 rows)
2. Monitor for any cross-state conflicts appearing
3. Document the change

---

## Questions for Confirmation

1. **Scope**: Should we prevent ALL cross-state conflicts, or only specific types?
2. **Timeline**: Is this urgent, or can we phase it in?
3. **Testing**: Do you have a test environment to validate changes?
4. **Approval**: Who needs to approve changes to these critical generation scripts?
5. **Monitoring**: How do you currently monitor conflict counts?
6. **Rollback**: What's the rollback plan if something goes wrong?

---

## Recommendation Summary

**I recommend:**
1. ✅ **DO** implement this change - prevents the problem at the source
2. ✅ **USE** the SQL function approach for maintainability
3. ✅ **TEST** thoroughly before production
4. ⚠️ **PHASE** implementation (one script at a time)
5. ⚠️ **MONITOR** closely after deployment

**Estimated Effort:**
- Function creation: 1-2 hours
- Script modifications: 2-3 hours per script
- Testing: 4-8 hours
- **Total: 1-2 days**

---

## Next Steps

**Please confirm:**
1. ✅ Approve this approach?
2. ✅ Preferred implementation option (Function/CTE/Inline)?
3. ✅ Timeline and phasing?
4. ✅ Test environment availability?

Once confirmed, I'll proceed with implementation.
