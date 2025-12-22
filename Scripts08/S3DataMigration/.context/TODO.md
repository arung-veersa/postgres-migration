# TODO - Migration Project Tasks

**Last Updated:** 2025-12-16

---

## 🔴 Immediate (This Week)

### Deploy v2.3 Optimizations
- [x] Run `.\deploy\rebuild_app_only.ps1`
- [x] Deploy Lambda code (upload zip to AWS)
- [x] Update Lambda environment variable (MIGRATION_VERSION=v2.3-optimized)
- [x] Force new Lambda containers
- [x] Verify no duplicate logs in CloudWatch

### Test New Configuration
- [x] Start analytics_dev2 migration
- [ ] Monitor first hour of execution (IN PROGRESS)
- [ ] Check CloudWatch for:
  - Single log entries (no duplicates)
  - 20 parallel threads running
  - Memory usage (should be 5-7 GB)
  - Snowflake fetch times

### Monitor & Compare
- [ ] Run `sql/QUICK_MONITORING.sql` every 30 minutes
- [ ] Compare analytics_dev2 vs analytics_dev performance
- [ ] Document actual metrics
- [ ] Verify row counts after completion

---

## 🟡 This Month

### Complete analytics_dev2 Migration
- [ ] Let migration run to completion
- [ ] Verify data integrity
  ```sql
  SELECT COUNT(*) FROM analytics_dev2.factvisitcallperformance_cr;
  -- Compare with source table count
  ```
- [ ] Check for failed chunks
- [ ] Document actual time-to-completion

### Decide on analytics_dev
- [ ] If analytics_dev2 successful, consider stopping analytics_dev
- [ ] Or let analytics_dev finish for data backup
- [ ] Clean up status tables for old runs

### Apply Learnings
- [ ] Update configuration based on results
- [ ] Fine-tune thread count if needed
- [ ] Adjust batch size if needed
- [ ] Document optimal settings

---

## 🟢 Later / Nice to Have

### Other Tables
- [ ] Apply optimizations to other large fact tables
- [ ] Migrate remaining analytics tables
- [ ] Test with different data patterns

### Incremental Loads
- [ ] Set up watermark-based incremental loads
- [ ] Test upsert performance
- [ ] Schedule regular sync jobs

### Monitoring Improvements
- [ ] Create dashboards for migration tracking
- [ ] Set up alerts for failures
- [ ] Add cost tracking

### Documentation
- [ ] Document final best practices
- [ ] Create runbooks for common scenarios
- [ ] Update deployment guide with lessons learned

---

## ✅ Completed

### Documentation Consolidation (Dec 16, 2025)
- [x] **Phase 1: Context Structure Created**
  - `.context/PROJECT_CONTEXT.md` - Living document of current state
  - `.context/SESSION_HANDOFF_TEMPLATE.md` - Session transition template
  - `.context/CODEBASE_MAP.md` - Code navigation guide
  - `.context/TODO.md` - This file
  - `.context/DECISIONS.md` - Architectural decisions
  - `.context/HISTORICAL_ISSUES.md` - Resolved bugs reference

- [x] **Phase 2: User Documentation Consolidated**
  - `README.md` - Streamlined (639 → 350 lines)
  - `docs/CONFIGURATION.md` - Complete config reference with ALL settings (NEW)
  - `docs/OPTIMIZATION.md` - Performance tuning guide (NEW)
  - `docs/MONITORING.md` - Progress tracking guide (NEW)
  - `docs/DEPLOYMENT.md` - AWS deployment guide (NEW)
  - `docs/TROUBLESHOOTING.md` - Updated, focused on problem-solving
  - `docs/QUICKSTART.md` - Updated to v2.3 with accurate commands

- [x] **Phase 3: Cleanup Complete**
  - Deleted 8 redundant/scattered files
  - Removed `MONITORING_QUERIES.md` (root) → docs/MONITORING.md
  - Removed `docs/FEATURES.md` → docs/CONFIGURATION.md
  - Removed `docs/OPTIMIZATION_GUIDE.md` → docs/OPTIMIZATION.md
  - Removed `docs/MIGRATION_ISSUES_RESOLVED.md` → .context/HISTORICAL_ISSUES.md
  - Removed `docs/SCHEMA_REPLICATION_GUIDE.md` → docs/DEPLOYMENT.md
  - Removed `aws/README.md` → docs/DEPLOYMENT.md
  - Removed `aws/step_functions/README.md` → docs/DEPLOYMENT.md
  - Removed `aws/step_functions/SETUP.md` → docs/DEPLOYMENT.md

- [x] **Complete Settings Documentation**
  - Documented ALL config.json settings (25+ settings)
  - Added Connection Settings section (Snowflake, PostgreSQL)
  - Added missing Global Settings (max_retry_attempts, lambda_timeout_buffer_seconds)
  - Created Complete Settings Reference table
  - Added Setting Precedence explanation
  - Added Environment Variable Substitution guide

### v2.3 Optimizations (Dec 16, 2025)
- [x] Fixed duplicate CloudWatch logging
  - `lib/utils.py` - Lambda environment detection
  - 50% log cost reduction
- [x] Optimized PostgreSQL connections
  - `lib/connections.py` - Session-level settings
  - Added synchronous_commit=off
- [x] Created monitoring queries
  - Complete query library in docs/MONITORING.md
  - `sql/QUICK_MONITORING.sql` - Quick reference
- [x] Fixed schema issues in queries
  - updated_at → completed_at
  - chunk_metadata → chunk_range
- [x] Updated configuration
  - 20 threads with 10GB Lambda
  - 25K batch size
  - Fast Snowflake warehouse
- [x] Deployed v2.3 to AWS Lambda

### v2.2 Optimizations (Dec 15, 2025)
- [x] Optimized chunking with aggregated query
  - 11 minutes → 0.3 seconds
  - Single query vs 199 individual queries
- [x] Increased parallel threads
  - 3 → 15 (later adjusted to 10 due to OOM)
- [x] Reduced batch size
  - 50K → 25K (faster Snowflake queries)
- [x] Memory tuning
  - Tested 6GB and 10GB configurations
  - Identified OOM threshold

### Earlier Fixes
- [x] Resume detection improvements
- [x] Truncation safety checks
- [x] Concurrent execution isolation
- [x] Per-table configuration overrides

---

## 📋 Backlog / Ideas

### Performance
- [ ] Consider connection pooling for PostgreSQL
- [ ] Test with even larger batch sizes (30K-40K)
- [ ] Experiment with compression during transfer
- [ ] Profile memory usage more precisely

### Reliability
- [ ] Add health checks
- [ ] Implement circuit breakers for Snowflake
- [ ] Add automatic warehouse scaling recommendations
- [ ] Better error messages and recovery hints

### Features
- [ ] Support for table partitioning
- [ ] Column-level transformation rules
- [ ] Data quality checks during migration
- [ ] Dry-run mode improvements

### DevOps
- [ ] Automate deployment with CI/CD
- [ ] Terraform for infrastructure
- [ ] Automated testing framework
- [ ] Performance regression tests

---

## 🚫 Not Doing / Deferred

### Decided Against
- ~~CHANGELOG.md~~ - Using git history instead
- ~~Separate deployment automation~~ - Manual AWS commands preferred
- ~~Connection pooling~~ - Added complexity, minimal benefit
- ~~Reduce Lambda memory to 3GB~~ - Testing with 10GB for 20 threads

### Postponed
- Multiple Snowflake warehouse support (not needed yet)
- Cross-region replication (not needed yet)
- Real-time CDC (not needed yet)

---

## Notes

- Always update PROJECT_CONTEXT.md when completing major tasks
- Create SESSION_HANDOFF when ending extended sessions
- Check this file at start of each session
- Archive completed items monthly

---

**Quick Status Check:**
```sql
-- Run this to see current state
SELECT target_schema, source_table,
       completed_chunks || '/' || total_chunks as progress,
       ROUND(completed_chunks::NUMERIC/total_chunks*100,1) || '%' as pct,
       status
FROM migration_status.migration_table_status
WHERE target_schema IN ('analytics_dev', 'analytics_dev2')
ORDER BY target_schema;
```

