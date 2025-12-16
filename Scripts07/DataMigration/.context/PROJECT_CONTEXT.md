# Project Context - Snowflake to PostgreSQL Migration

**Last Updated:** 2025-12-16  
**Current Version:** 2.3.0  
**Status:** Active development - Testing optimizations

---

## Current State

### What We're Doing
Migrating large fact tables from Snowflake to PostgreSQL with focus on:
- **Primary table:** `FACTVISITCALLPERFORMANCE_CR` (272M records)
- **Goal:** Reduce migration time from 10 days to 8-12 hours
- **Approach:** Concurrent migrations to test optimizations

### Active Work
- ✅ Fixed duplicate CloudWatch logging (Lambda environment detection)
- ✅ Optimized chunking strategy (11min → 0.3s with aggregated query)
- ✅ Added PostgreSQL connection optimizations (session-level settings)
- ✅ Created monitoring queries for concurrent migrations
- 🔄 Testing: Two concurrent migrations (analytics_dev, analytics_dev2)
- 🔄 Pending: Deploy and verify optimizations

### Current Configuration
```json
{
  "parallel_threads": 20,
  "batch_size": 25000,
  "lambda_memory": "10GB",
  "truncate_onstart": true,
  "insert_only_mode": true
}
```

---

## Recent Changes (Last 7 Days)

### 2025-12-16: v2.3 - Logging & Performance Fixes
**Files Modified:**
- `lib/utils.py` - Fixed duplicate CloudWatch logs (Lambda env detection)
- `lib/connections.py` - Added PostgreSQL session optimizations
- `lib/chunking.py` - Aggregated date query (already deployed earlier)
- `config.json` - Adjusted to 20 threads, 25K batch

**Key Improvements:**
- Duplicate logs eliminated (75% CloudWatch cost reduction)
- PostgreSQL COPY operations faster (synchronous_commit=off)
- Created comprehensive monitoring queries
- Fixed schema issues in queries (updated_at → completed_at)

**Status:** Ready to deploy

### 2025-12-15: v2.2 - Parallelism & Memory Tuning
**Changes:**
- Increased threads from 3 → 15 (later adjusted to 10 due to OOM)
- Reduced batch size from 50K → 25K (faster Snowflake queries)
- Fixed chunking aggregation (11min → 0.3s)

**Issues Found:**
- 15 threads caused OOM (6.7GB > 6GB limit)
- Resolved by reducing to 10 threads

---

## Version History

### v2.3.0 (Dec 16, 2025) - Performance & Logging Optimization
- **Chunking:** Single aggregated query (2,200x speedup)
- **Logging:** Eliminated duplicate CloudWatch entries
- **PostgreSQL:** Session-level performance settings
- **Memory:** Right-sized threads to avoid OOM
- **Monitoring:** Complete query library for concurrent migrations

### v2.2.0 (Dec 15, 2025) - Parallelism Boost
- Parallel threads: 3 → 15 (5x concurrency)
- Batch size: 50K → 25K (faster queries)
- Lambda memory: Tested 6GB and 10GB
- Result: Significant speedup but hit OOM

### v2.1.0 (Dec 2025) - Resume & Safety
- Concurrent execution isolation (execution_hash)
- Bulletproof truncation protection
- Enhanced diagnostic logging
- Per-table memory optimization

---

## Key Decisions & Rationale

### Decision: 20 Threads with 10GB Lambda (Dec 16)
**Why:** 
- 10 threads used ~5GB (safe but conservative)
- Snowflake warehouse is fast, can handle more parallel queries
- 20 threads × 300MB = 6GB (60% of 10GB = safe)
- User has faster Snowflake warehouse now

**Expected:** 8-12 hour migration time

### Decision: Single Aggregated Chunking Query (Dec 15)
**Why:**
- Original: 199 individual COUNT queries = 11 minutes
- New: 1 aggregated query = 0.3 seconds
- No downside, pure performance gain

**Result:** Startup time reduced by 99.7%

### Decision: Lambda Environment Detection for Logging (Dec 16)
**Why:**
- Lambda runtime adds its own handler
- Our code added another handler
- Both outputted same logs = duplicates
- Detection prevents adding handler in Lambda

**Result:** Single log entries, 50% CloudWatch cost reduction

---

## Known Issues & Limitations

### Current
- ✅ **Duplicate logs** - FIXED (not yet deployed)
- 🔄 **Old migration slow** - analytics_dev running at old rate (~21 days)
- ✅ **Schema mismatch in queries** - FIXED (updated_at doesn't exist)

### Ongoing Monitoring
- Watch for OOM with 20 threads
- Monitor Snowflake query performance
- Verify no log duplicates after deployment

---

## Architecture Overview

### Data Flow
```
Snowflake (Source)
    ↓
Lambda (15min chunks)
    ↓ COPY (bulk) or UPSERT (incremental)
PostgreSQL (Target)
    ↓
Status Tables (tracking)
```

### Key Components
- **Lambda:** Python 3.11, 10GB memory, 15min timeout
- **Step Functions:** Orchestrates Lambda invocations, handles retries
- **Status Tables:** Track runs, tables, chunks for resume capability
- **Config Hash:** Enables concurrent migration isolation

---

## Migration Patterns

### Full Load (Current)
```json
{
  "truncate_onstart": true,
  "insert_only_mode": true,
  "source_watermark": null,
  "target_watermark": null
}
```
- Truncates target table
- Uses COPY mode (fastest)
- No watermark logic
- Ideal for: Initial loads, complete refreshes

### Incremental Load (Future)
```json
{
  "truncate_onstart": false,
  "insert_only_mode": false,
  "source_watermark": "Visit Updated Timestamp",
  "target_watermark": "Visit Updated Timestamp"
}
```
- Compares watermarks
- Uses UPSERT mode
- Only migrates changed rows
- Ideal for: Ongoing sync, delta loads

---

## Active Migrations

### Migration 1: analytics_dev (OLD - Running)
- **Started:** Dec 15, 22:18
- **Progress:** ~4.3M / 272M records (1.6%)
- **Config:** Old (3 threads, 50K batch)
- **ETA:** ~21 days
- **Status:** Keeping for working data, not optimized

### Migration 2: analytics_dev2 (NEW - Testing)
- **Status:** Ready to start
- **Config:** Optimized (20 threads, 25K batch, fast warehouse)
- **Purpose:** Test all v2.3 optimizations
- **Expected ETA:** 8-12 hours

---

## Performance Metrics

### Target: FACTVISITCALLPERFORMANCE_CR
- **Total records:** 272,107,404
- **Date range:** May 29 - Dec 14, 2025 (199 days)
- **Total chunks:** 10,968 (with 25K batch size)
- **Largest date:** Dec 14 (12.4M records = 497 chunks)

### Bottlenecks Identified
1. **Snowflake query time:** 1.5-9 min (BIGGEST - warehouse dependent)
2. **PostgreSQL COPY:** 5-6 sec (good)
3. **Chunking overhead:** 0.3 sec (excellent after optimization)
4. **Network latency:** ~1-2 sec (acceptable)

### Cost Estimates
**Optimized (8-12 hours):**
- Lambda (10GB × 10 hours): ~$6.50
- Step Functions: ~$0.01
- CloudWatch (fixed logs): ~$0.50
- Snowflake warehouse (MEDIUM): ~$40
- **Total:** ~$47

**Old way (21 days):**
- Lambda: ~$100
- CloudWatch: ~$12
- Snowflake: ~$25
- **Total:** ~$137

---

## Team Preferences & Guidelines

### Working with AI (Cursor)
- ✅ Provide manual AWS CLI commands (don't automate)
- ✅ Describe approach first, get confirmation before code changes
- ✅ Consolidate documentation, avoid temporary files
- ✅ No git commands or commit messages (user handles manually)

### Code Changes
- Always test locally before deploying to Lambda
- Use `rebuild_app_only.ps1` for Lambda package
- Force Lambda container refresh with MIGRATION_VERSION env var
- Verify with CloudWatch logs after deployment

### Documentation
- Keep minimal but effective .md files
- Use `.context/` for session/development context
- Use `docs/` for user-facing documentation
- Update PROJECT_CONTEXT.md after significant changes

---

## Quick Reference

### Most Used Commands

**Monitor Migration:**
```sql
-- Quick dashboard
SELECT target_schema, source_table,
       ROUND(completed_chunks::NUMERIC/total_chunks*100,1) || '%' as progress
FROM migration_status.migration_table_status
WHERE status = 'in_progress';
```

**Rebuild Lambda:**
```powershell
cd Scripts07\DataMigration
.\deploy\rebuild_app_only.ps1
```

**Deploy to Lambda:**
```bash
aws lambda update-function-code \
  --function-name snowflake-postgres-migration \
  --zip-file fileb://deploy/lambda_deployment.zip \
  --region us-east-1
```

### Key Files
- `config.json` - All migration settings
- `lib/chunking.py` - Chunking strategies (optimized here)
- `lib/migration_worker.py` - Core migration logic
- `lib/connections.py` - DB connections (optimized here)
- `lib/utils.py` - Logging setup (fixed here)

---

## Next Steps

### Immediate (This Week)
1. Deploy v2.3 code to Lambda
2. Start analytics_dev2 migration
3. Monitor for 1-2 hours to verify optimizations
4. Compare performance with analytics_dev

### Soon
1. Complete analytics_dev2 migration
2. Verify row counts match source
3. Document actual performance metrics
4. Consider stopping analytics_dev if dev2 succeeds

### Future
1. Apply learnings to other large tables
2. Set up incremental loads for ongoing sync
3. Optimize other fact tables
4. Document final best practices

---

## Getting Help

### If Migration Stalls
1. Check CloudWatch logs for errors
2. Run `sql/diagnose_stuck_migration.sql`
3. Query `migration_status.migration_chunk_status` for failed chunks
4. Review `.context/TROUBLESHOOTING.md`

### If Memory Issues
1. Check "Max Memory Used" in CloudWatch REPORT lines
2. If > 90% of allocated: Reduce parallel_threads
3. If < 50% consistently: Can increase threads

### If Resume Doesn't Work
1. Check run_id matches in status tables
2. Verify config_hash hasn't changed
3. Use explicit `resume_run_id` in Step Function input
4. Review `.context/DECISIONS.md` for resume logic

---

**This document should be updated after:**
- Completing major milestones
- Making architectural decisions
- Deploying new versions
- Discovering important insights
- Before ending extended work sessions

