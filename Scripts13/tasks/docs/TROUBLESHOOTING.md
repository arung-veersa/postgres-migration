# Troubleshooting & Bug Fix History

This document covers known issues, fixes, and lessons learned during development. Organized by the platform version in which they were encountered.

---

## ECS/Container Issues (Scripts13)

### 1. JSON Parse Error on Startup (FIXED)
**Error**: `json.decoder.JSONDecodeError: Invalid control character at: line 6 column 81`

**Root Cause**: The `SNOWFLAKE_PRIVATE_KEY` environment variable contains literal newline characters (multi-line RSA key). When `settings.py` substitutes `${SNOWFLAKE_PRIVATE_KEY}` into the raw JSON string, the unescaped newlines break `json.loads()`.

**Fix**: Modified `_substitute_env_vars()` in `settings.py` to JSON-encode all environment variable values before substitution:
```python
def replacer(match):
    var_name = match.group(1)
    value = os.environ.get(var_name)
    if value is None:
        return match.group(0)
    # json.dumps adds quotes -- strip them since placeholder is inside a JSON string
    return json.dumps(value)[1:-1]
```

### 2. CloudWatch Log Group Missing (FIXED)
**Error**: `ResourceNotFoundException: The specified log group does not exist`

**Root Cause**: ECS task definition references `/ecs/task02-conflict-updater` log group, but it must be created manually in CloudWatch before the first task run.

**Fix**: Create the log group via AWS Console: CloudWatch > Log groups > Create log group > `/ecs/task02-conflict-updater`.

### 3. AWS CLI Authentication Failures (FIXED)
**Error**: `UnrecognizedClientException: The security token included in the request is invalid`

**Root Cause**: AWS CLI commands were not specifying the named SSO profile. Running `aws configure` sets the `[default]` profile, but SSO credentials live under the named profile.

**Fix**: Always include `--profile <PROFILE_NAME>` in all `aws` commands:
```powershell
aws ecr get-login-password --region us-east-1 --profile HHA-DEV-CONFLICT-MGMT-354073143602 | docker login --username AWS --password-stdin <ECR_URI>
```

### 4. PowerShell JSON Escaping for --overrides (FIXED)
**Error**: `Error parsing parameter '--overrides': Invalid JSON`

**Root Cause**: PowerShell mangles JSON when passed inline to `aws ecs run-task --overrides`. Backslash escaping of quotes doesn't work reliably across PowerShell versions.

**Fix**: Write JSON to a temporary file and reference with `file://`:
```powershell
$json | Out-File -Encoding ascii -FilePath $tempFile
aws ecs run-task --overrides "file://$tempFile" ...
```
This is now handled automatically by `build-and-push-ecr.ps1`.

---

## SQL and Query Issues (Scripts12, carried into Scripts13)

### 5. SQL Placeholder Collision (FIXED)
**Error**: `SQL compilation error: syntax error line 66 at position 8 unexpected 'logic'`

**Root Cause**: Placeholder `{CONFLICT_PAIRS_JOIN}` appeared in both a comment header and the actual SQL template. Python's `replace()` replaced both occurrences.

**Fix**: Renamed placeholders in comments to plain text instead of curly braces:
```sql
-- GOOD: CONFLICT_PAIRS_JOIN placeholder - description
-- BAD:  {CONFLICT_PAIRS_JOIN} - description
```

### 6. Column Name Mismatches (FIXED)
**Error**: `invalid identifier '"ETATravleMinutes"'` (typo in Snowflake)

**Root Cause**: Column names differed between Snowflake and PostgreSQL:
- Snowflake: `"ETATravleMinutes"` (typo)
- PostgreSQL: `"ETATravelMinutes"` (correct)

**Fix**: Added column name mapping in final SELECT:
```sql
"ETATravelMinutes" AS "ETATravleMinutes"  -- Map to Snowflake's typo
```
Also fixed: `SchVisitTimeSame` -> `SchAndVisitTimeSameFlag`

### 7. Configuration Parameter Rename (COMPLETED)
**Change**: `enable_change_detection` -> `skip_unchanged_records`

**Rationale**: More intuitive naming -- describes what happens (skip) rather than internal mechanism (detection).

---

## Performance Optimizations Applied

### 1. Change Detection (98% UPDATE Reduction)
Smart comparison of 40+ business columns + 7 flags before UPDATE.
- Before: 20,742 UPDATEs per run
- After: 251 UPDATEs per run
- Reduction: 98.8%

### 2. Batch Processing with Commit-per-Batch
Stream and commit every 5,000 rows instead of loading all in memory.
- Memory usage: Stable at ~455 MB
- Allows recovery from failures (commit points every batch)

### 3. Persistent Database Connections
Single connection reused across all operations. Eliminated per-batch connection overhead.

### 4. PostgreSQL Index Optimization
Composite indexes for JOIN and lookup operations:
```sql
CREATE INDEX idx_conflictvisitmaps_visitid ON conflictvisitmaps ("VisitID");
CREATE INDEX idx_cvm_visitdate_ssn ON conflictvisitmaps ("VisitDate", "SSN") WHERE ...;
```

### 5. Pair-Precise Stale Cleanup (v3)
Uses exact `(VisitDate, SSN)` pairs instead of separate DISTINCT lists. Eliminates the cross-product problem (640K SSNs x 596 dates = 382M combos vs 12.5M actual pairs).

---

## Lessons Learned

### 1. SQL Template Placeholder Hygiene
Never use exact placeholder syntax `{NAME}` in SQL comments. Use plain text descriptions instead.

### 2. JSON-Safe Environment Variable Substitution
When building JSON strings from environment variables, always JSON-encode values before substitution. Multi-line strings (RSA keys, certificates) will break raw `json.loads()`.

### 3. PowerShell and AWS CLI JSON
PowerShell's quoting rules are incompatible with inline JSON for `--overrides`. Always use `file://` for complex JSON payloads.

### 4. ECS Task Definition Versioning
Each modification to a task definition creates a new immutable revision. Running a task does NOT create a new revision -- only changing the definition does.

### 5. CloudWatch Log Group Must Pre-Exist
Unlike Lambda (which auto-creates log groups), ECS requires manual log group creation before the first task run.

### 6. Named AWS Profiles
When using SSO, every `aws` command needs `--profile`. The `[default]` profile is NOT used automatically if credentials are under a named profile.

---

## Performance Baseline (v3, 2026-02-10)

| Metric | Value |
|---|---|
| Total time | 8m 29s (509s) |
| Rows fetched from Snowflake | 153,901 |
| Matched in Postgres | 51,612 (33.5%) |
| New conflicts (not in PG) | 102,289 (66.5%) |
| Changes detected | 6,134 (5,202 flag + 932 business) |
| Rows actually updated | 216 |
| Rows skipped (no changes) | 45,478 (88.1%) |
| Stale conflicts resolved | 9,294 -> StatusFlag='R' |
| Delta pairs (stale scope) | 12,517,521 (640K SSNs x 596 dates) |
| Peak memory | 455 MB of 2,048 MB allocated |
| Errors | 0 |

### Time Breakdown
- Step 0 (excluded SSNs): ~6s
- Step 1 (delta_keys): ~5s
- Step 2 (base_visits A+B): ~148s
- Step 2d (delta pairs -> PG): ~92s
- Step 3 (conflict detection + streaming batches): ~140s
- Step 4 (stale cleanup): ~117s (84s identify + 32s update)

---

## Future Work

### High Priority
1. **INSERT Logic for New Conflicts** (~102K detected but not inserted per run)
2. **Conflicts Table Aggregation** (parent table updates from conflictvisitmaps)

### Medium Priority
3. **AWS Secrets Manager** (migrate from plain environment variables)
4. **Step Functions Integration** (orchestrate multi-task pipelines)

### Low Priority
5. **Enhanced CloudWatch Metrics** (custom metrics for key performance indicators)
6. **UpdateFlag Cleanup** (maintenance task for stale UpdateFlag values)

---

**Document Version**: 2.0
**Last Updated**: 2026-02-11
**Status**: Current (ECS/Fargate deployment)
