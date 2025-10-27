# ARB 555 - Snowflake to Postgres Migration

### **References:**
- Template: [https://hhaxsupport.atlassian.net/wiki/x/nIESEg](https://hhaxsupport.atlassian.net/wiki/x/nIESEg)
- ARB: [https://hhaxsupport.atlassian.net/browse/ARB-555](https://hhaxsupport.atlassian.net/browse/ARB-555)
- EPIC: [https://hhaxsupport.atlassian.net/browse/BIREP-11535](https://hhaxsupport.atlassian.net/browse/BIREP-11535)
- WBS and HLE: [https://hhaxsupport.atlassian.net/browse/BIREP-16787](https://hhaxsupport.atlassian.net/browse/BIREP-16787)

---

### Problem Statement

- **Business issue**: Conflict Management currently relies on Snowflake for both analytics processing and interactive UI reads, driving up monthly compute-credit consumption.
- **Symptoms**
    - **High compute costs**: Scheduled transformations/tasks in Snowflake are credit intensive.
    - **UI-driven overhead**: Each user query wakes Snowflake compute, compounding spends.
    - **Escalating spend**: Combined batch + interactive workloads exceed budget thresholds.
    - **Performance:** Snowflake is not an appropriate technology for transactional processing - multiple writes and updates.
- **Goal**: Reduce cost by eliminating Snowflake for operational reads and task execution. Improve latency and availability for UI/API reads. Establish clear ownership and lifecycle for conflict data within PostgreSQL. Preserve Analytics in Snowflake as the upstream system of record.
- **Out of scope**: Replatforming the PHP runtime (no change to framework). Decommissioning Snowflake Analytics (retained for analytics workloads).
- **Costing**:  Please refer to the TCO section for a comparison of cost before and after the migration.

---

### Workflow Description

**Current (AS-IS)**
1. **Data Sources:**
    - Analytics (Snowflake): Acts as the primary source system for operational data for Conflict Management.
    - ETL (DBT): As part of making available the operational HHA data (outside CM), a nightly ETL job populates the Analytics DB with data from HHA DBs (all 3 environments).
    - CONFLICTREPORT DB: Uses internal tasks and procedures to populate tables relevant for conflict, tracking and resolution.
2. **Stored Procedure Logic and Processing:**
    - Stored Procedures within `CONFLICTREPORT` are invoked by `Tasks` to manage data and drive conflict finding logic.
    - Queries from the Conflict Management module and reporting dashboards leverage these underlying procedures for operational insights.
3. **Data Storage:**
    - The Snowflake CONFLICTREPORT database serves as the central repository, hosting:
        - Raw and transformed tables for conflict reporting.
        - Conflict resolution metadata.
4. **Display of Insights (UI App):**
    - Conflict Management system, built on PHP 8.0 and Laravel 12.0, provides an interface to Payers, Providers and Aggregators.
    - It queries `ANALYTICS` and `CONFLICTREPORT` views/tables to expose processed data for conflicts.
    - AWS S3 hosts any supplementary documents or evidence attached to conflict cases.

**Future (TO-BE) / Solution Visualization**

The future state architecture enhances performance, modularity, and data availability by migrating key components from a Snowflake-centric model to a Postgres-backed infrastructure. A schema, `conflict`, is created in PostgreSQL which stores the one-time migration from Snowflake `CONFLICTREPORT`, and maintained nightly from existing Snowflake `Analytics` database via Python scripts on Lambda, orchestrated by Step Functions in AWS. The front-end PHP application reads/writes exclusively to this schema in PostgreSQL.

1. **Decoupled and Modular Data Pipelines**
    - Python scripts in Lambda: Nightly updates from Snowflake `Analytics` → PostgreSQL `conflict` for serving and lifecycle operations; optional intraday reruns via AWS Step function triggers.
2. **Enhanced Schema Design**
    - `conflict` schema – the system of record for all conflict lifecycle data (e.g., resolution status, audit logs, user assignments). This is where the writes from the UI would go.
3. **Integration with Conflict Management**
    - PHP Laravel module queries PostgreSQL directly with least-privilege roles; lower latency and higher availability.
    - Caching (e.g., Redis or ElastiCache) in front of PostgreSQL for frequently accessed, immutable data dimension data (e.g., dropdown list values, provider/payer names)
4. **Data Availability and Governance**
    - PostgreSQL stores enriched metadata; lineage documented from Snowflake → Postgres; reconciliation and freshness SLOs.

---

### Hardware & Software Requirements

- **Compute/Services**: Amazon Aurora Serverless for PostgreSQL, AWS Lambda (Python), AWS Step Function, Snowflake retained for analytics.
- **Languages/Frameworks**: Python 3.x (Conflict Creation logic), PHP 8.0 (Laravel 12.0), SQL (PostgreSQL & Snowflake).
- **Connectivity**: Secure network paths from Lambda to Postgres and Snowflake; interface endpoints for Secrets/Logs.
- **Scaling**: Postgres instance sized to peak read workloads; Lambda concurrency tuned for throughput; optional read replica in prod.
- **Pattern**: Command/query segregation for operational reads vs analytics; modular functions; idempotent upserts, incremental updates using "last modified" timestamps or using a CDC-like approach from Snowflake or using MERGE.
- **Libraries/Tools**: `psycopg2`/`asyncpg / pgBouncer`, Snowflake Python Connector, migration tooling (Flyway/Liquibase).

---

### Database Changes & Data Governance

- **Replicated/CDC impact**: Snowflake remains Analytics hub.
- **Changes to DBs**: Introduce `conflict` schema in Postgres with lifecycle tables, audit logs, resolution tracking.
- **Stored procedures**: Migrate conflict creation logic from Snowflake procedures/tasks to Python scripts maintaining re-usability.
- **Archival approach**: Time-based partitioning or archival tables for aged records; lifecycle retention.
- **Read/Write ratio**: Read-heavy; writes primarily from conflict lifecycle updates.
- **Indexing strategy**: Composite indexes on primary predicates (payer/provider, status, date ranges); covering/partial indexes; routine VACUUM/ANALYZE.
- **Lineage & stewardship**: Mapping from Snowflake sources to Postgres targets; defined table owners; catalog entries maintained.

---

### UI/Front-end Impact

- **Target framework**: Laravel (unchanged).
- **Changes**: Update repository/data access to PostgreSQL, keeping the Snowflake one for Analytics access; maintain endpoints and UI flows.
- **Tooling**: Maintain platform standards; ensure accessibility compliance.

---

### API/Data Exchange Impact

- **Endpoints**: Existing Laravel endpoints continue; backend data source switches to PostgreSQL.

---

### Performance Impact

- **Users/Load**: Peak UI read traffic shifts to Postgres for lower latency.
- **Impacted areas**: Operational search/list pages; conflict detail views; background sync windows.
- **SLA impacts**: Improved UI response times; nightly jobs move from Snowflake Tasks to Python to reduce credits.

---

### Security Impact

- **Best practices**: TLS for all DB connections.
- **Config changes**: Secure connections from Lambdas to Postgres; credentials in AWS Secrets Manager; periodic rotation.
- **Logging/Monitoring**: Centralized structured logs to CloudWatch and Datadog; DLQ for failed steps; PII handling per policy.
- **Cloud changes**: Use AWS IAM accounts to control access to operations that create, modify, or delete Amazon Aurora resources.
- **AuthN/AuthZ**: No changes to end-user auth; least-privilege DB roles, restricted write `conflict`.

---

### Testing Approach

- **Unit testing (mandatory)**:
    - Python Scripts: transformations, idempotency, error handling.
    - Laravel: repository/services querying Postgres; conflict logic.
- **Integration tests**:
    - End-to-end: Snowflake sample → Postgres → UI reads.
    - DB migrations and rollbacks; index usage verification.
- **CI integration**: Automated tests on PR; migration validation in ephemeral environments.
- **Coverage goal**: ≥80% for core conflict creation logic.
- **API test plan**: Validate endpoints against PostgreSQL with realistic data volumes and filters.
- **UI automation**: Critical paths (search, filter, detail view, attachment load).
- **Manual tests**: Edge cases for conflict states, access controls, and data freshness thresholds.
- **Performance tests**: Load test Postgres query patterns; benchmark before/after; validate P95 latency targets.

---

### Deployment Strategy

- **Environments**: Dev → QA → Staging → Prod, aligned with existing pipelines.
- **Automation**: automated migrations; blue/green or rolling releases for app changes.
- **Cutover plan**:
    - Dual-read validation window in non-prod and prod subset.
    - Backout plan: revert flag; disable Step Function workflows.

---

### Monitoring

- **Production monitoring**:
    - Postgres: connections, CPU/IO, query latency, locks, bloat, replica lag.
    - Lambdas: success/error rates, durations; DLQ for failures.
    - App: endpoint latency/error rates; slow query logs and auto-explain sampling.
- **Logging**: Centralized structured logs; PII handling compliant with policy.
- **Alerting**: Thresholds for sync freshness, ETL completion, and Postgres health.

---

### Resources Distribution and Availability

- **Team dependencies**
    - Data Engineering: Snowflake mappings, Conflict creation logic
    - App Engineering: Laravel data access changes, feature flagging.
    - DBA/Platform: Postgres provisioning, tuning, backups/DR.
    - DevOps: CI/CD, monitoring, secrets management.
    - QA: Test strategy and execution.

---

### Total Cost of Ownership (TCO)

- **Licensing**: Snowflake compute costs reduced, Postgres managed service costs added.
- **Setup**: One-time migration effort (schema, pipelines, app changes).
- **Integration**: CI/CD updates for migrations and Lambdas.
- **Training**: Teams familiar with Python/Postgres/Laravel.
- **Maintenance**: Ongoing Postgres tuning, Python script operations, monitoring.
- **Scalability**: Vertical/horizontal options (read replicas).
- **Operational efficiency**: Lower latency reads; reduced Snowflake dependency for operational traffic.
- **Current Cost:** The present Snowflake Compute units for maintaining two warehouses dedicated for Conflict Management work is ~3050 units and @USD 3.2/unit translates to ~**USD 9,760 /month.**
- **Estimated Future Cost:** The estimated cost for maintaining one environment is ~ USD 440 /month. Therefore, for three environments including one each of Dev, QA and Production environments would be **~ USD 1320 /month**. The detailed breakup against the respective AWS services is given below (ref [AWS Calculator](https://calculator.aws/#/estimate?id=ac5aa2af3f744f5ebeb85eb8def4430d73d8b483)):
- **Estimated Effort:** The estimated effort to perform the migration from Snowflake to PostgreSQL including aligning the front-end to work with the migrated database is **~210 story points**. Details can be found on the comment in the ticket [https://hhaxsupport.atlassian.net/browse/BIREP-16787](https://hhaxsupport.atlassian.net/browse/BIREP-16787)

---

### Required Documentation

- Data mapping spec (Snowflake → Postgres).
- ERD for `analytics` and `conflict` schema.
- ETL/sync Lambda code and runbooks.
- Secrets/roles matrix and grants.
- Migration plan with rollback steps.
- Test plans and results.
- Cutover and backout plans

---

### Future Expansion

- Incremental sync to near-real-time (CDC) if needed.
- Materialized views or caching layers for heavy queries.
- Read replicas for scale-out, partitioning strategy for very large tables.
- Caching frequently referenced DIM table data from Snowflake Analytics for use in the front-end.

---

## Inline Comments

> **Non-Goal**  
> _Do you mean out of scope?_  
> — Jon Race  
> _Yes, “out of scope” is a better phrase. Basically, this migration is would not consider changing over from PHP front-end._  
> — Arun Gupta

> **45,000 /month /warehouse**  
> _Did we do a review of what queries are costing the most?_  
> — Jon Race  
> _The database updates are done nightly as a series of 12 Snowflake background tasks which in turn invoke complex stored procedures. Mainly the new conflicts are generated from the previous 24 hours Visit data._  
> — Arun Gupta

> **PHP**  
> _PHP?_  
> — Jon Race  
> _PHP 8.0 + Laravel 12_  
> — Arun Gupta

> **Snowflake credits reduced**  
> _What is the estimated savings?_  
> — Jon Race  
> _Added the estimated saving summary later in this section._  
> — Arun Gupta

> **Enhanced Schema Design**  
> _Do you think introducing another `staging` schema in PostgreSQL would be helpful? The Lambda sync jobs from Snowflake could first land raw data here. A second step within the ETL process could then transform and move data from `staging` to `analytics`. This could make the process idempotent and allow for easy data validation and cleansing before exposing it to the application._  
> — pjha.c  
> _Sure, makes sense._  
> — Arun Gupta

> **Establish clear ownership**  
> _You might want to make this more explicit in the schema design section below. The `conflict` schema could be the system of record for all conflict _lifecycle_ data (e.g., resolution status, audit logs, user assignments). The `analytics` schema could be marked a read-only replica of a subset of the snowflake data, used as a reference. This would clarify where writes from the UI would go (only `conflict` tables) and prevent confusion._  
> — pjha.c  
> _Sure, updated as per suggestion._  
> — Arun Gupta

> **idempotent upserts with staging tables**  
> _I’d suggest on elaborating more on the mechanism. Using a staging table and a single SQL statement that performs an INSERT ... ON CONFLICT (...) DO UPDATE ...? or a MERGE (if using a newer PostgreSQL version) is the standard pattern. This ensures that re-running a failed job doesn't create duplicates or corrupt data._  
> — pjha.c  
> _Sure, using MERGE will be the first choice._  
> — Arun Gupta

> **Sync (Python on Lambda): Nightly copies the required subset from Snowflake Analytics → Postgres analytics.**  
> _Could you specify whether the sync from snowflake is a full refresh or, _preferably_, an incremental load? For large datasets, incremental is essential for performance and cost. This requires identifying "last modified" timestamps or using a CDC-like approach from Snowflake._  
> — pjha.c  
> _Yes, preferably an incremental load._  
> — Arun Gupta

> **Python ETL on Lambda, orchestrated by Glue**  
> _Be mindful of the 15-minute timeout for synchronous Lambdas. If the initial data load or a large incremental sync exceeds this, you'll need a different strategy (e.g., AWS fargate, step functions to orchestrate multiple lambdas, or a glue job instead of a lambda)._  
> — pjha.c  
> _Also, AWS step functions is a more native and powerful orchestrator with better state management, error handling, and visual debugging. Consider it as an alternative rather than using glue as a pure orchestrator_  
> — pjha.c  
> _Sure, thanks for the pointers, the 15 min window is definitely we will have to design around._  
> — Arun Gupta

> **connections**  
> _With lambda, ensure you are using a connection pooler (like PgBouncer) for PostgreSQL. Lambdas can create a high number of concurrent connections, which can overwhelm the RDS instance's connection limit. This is a critical operational detail you can’t miss._  
> — pjha.c  
> _Mentioned the same in the Libraries/Tools under Hardware/Software._  
> — Arun Gupta

> **Feature flag to switch reads from Snowflake to Postgres.**  
> _Instead of a single feature flag for all reads, is it possible to consider a phased approach..._  
> — pjha.c  
> _On second thoughts co-existing of the two databases may not be necessary and may make the front-end code more complex. But we can deliberate more on this on the lines of the suggestions._  
> — Arun Gupta

> **Dual-read validation**  
> _This is excellent. Automate this as much as possible. you could run daily jobs that compare record counts and checksums of key tables between the snowflake conflictreport and the PostgreSQL conflict schema during the validation phase._  
> — pjha.c  
> _Okay._  
> — Arun Gupta

> **Composite indexes on primary predicates**  
> _You could go one step further and mandate that the performance test plan must capture slow queries and use EXPLAIN (ANALYZE, BUFFERS) to validate index usage. PostgreSQL's query planner is different from snowflake's, so assumptions may not hold._  
> — pjha.c  
> _Yeah, we want to work around Snowflake’s limited indexing capabilities with PostgreSQL._  
> — Arun Gupta

> **for lower latency**  
> _Any consideration/need of a caching layer (e.g., Redis or ElastiCache) in front of PostgreSQL for frequently accessed, immutable data (e.g., dropdown list values, provider/payer names) ?_  
> — pjha.c  
> _Yeah, drop-down values and names are good candidates for caching, which we will try and explore when syncing the UI with PostgreSQL. But perhaps with dynamic querying caching results is not useful._  
> — Arun Gupta

> **idempotent upserts with staging tables, preferably using MERGE.**  
> _‘preferably using MERGE’ is highly rdbms-specific. Not sure why this is a specification here._  
> — Kristin Jones  
> _This was in response to Praveen Jha’s suggestion of trying to leverage as much data as possible instead of re-writing everything._  
> — Arun Gupta

> **Snowflake retained for analytics.**  
> _Discussed in the ARB meeting - you will NOT be retaining this specific snowflake environment. Please make sure to specify this here. Otherwise we are not improving our cost profile at all_  
> — Kristin Jones  
> _Okay I have added the comment about decommissioning Snowflake in the TCO section._  
> — Arun Gupta

> **Scaling: Postgres instance sized to peak read workloads; Lambda concurrency tuned for throughput; optional read replica in prod.**  
> _This should be an aurora cluster, with a read-only instance available for read operations. If the balance of write vs read leans toward the read side, then a single write node is appropriate; if the balance is toward the write side, then a single read node is appropriate. (2 vs 1 pattern)_  
> — Kristin Jones  
> _Sure, thanks for the suggestion._  
> — Arun Gupta

> **Symptoms**  
> _Although Cost is a significant factor, performance is also a significant factor. Snowflake is not an appropriate technology for transactional processing - multiple writes and updates._  
> — jstauffer  
> _Mentioned the point about performance._  
> — Arun Gupta

> **ETL (DBT): A nightly ETL job populates the Analytics DB with data from HHA DBs (all 3 environments).**  
> _Although that is correct, this is not part of the Conflict Mgmt (CM) solution - CM uses Analytics as the primary source of information about visits that are analyzed for conflicts. That data is increasingly coming from Sandata as well as HHA._  
> — jstauffer  
> _Indicated that this is happening outside CM._  
> — Arun Gupta

> **OLTP SQL DB serves as the central repository, hosting:**  
> _What is this OLTP database?_  
> — jstauffer  
> _Changed it to the Snowflake_  
> — Arun Gupta

> **AWS S3 hosts any supplementary documents or evidence attached to conflict cases.**  
> _What documents are stored here? I wasn’t aware of this store…_  
> — jstauffer  
> _There is an option for Payer and Providers to communicate from within CM, as part of this chat they can optionally attach documents which are stored in S3._  
> — Arun Gupta

> **Sync (Python on Lambda): Nightly copies the required subset from Snowflake Analytics → Postgres analytics. (Preferably incremental load)**  
> _We should not be copying data from snowflake into the Conflict Mgmt database._  
> — jstauffer  
> _This is mainly the master data and visit related data, which is referred from within the nightly tasks (to create the conflicts) and also directly from the front-end PHP application. We could continue to refer these directly from Snowflake views, but the idea was to remove as much dependency as possible._  
> — Arun Gupta

> **ETL (Python on Lambda): Nightly updates fromanalytics → conflict for serving and lifecycle operations; optional intraday reruns via Glue triggers.**  
> _This is not an ETL job - it is the logic that identifies new conflicts in the source data, and creates conflict objects in the target database._  
> — jstauffer  
> _Yes, ETL may not be correct terminology. It is indeed the business logic corresponding to the present-day nightly tasks in Snowflake, which creates the Conflicts reading the visit data created in the last 24 hours._  
> — Arun Gupta

> **analytics schema – read-only replica of a subset of the snowflake data, used as a reference.**  
> _See comment above - this should not exist - don’t copy this data._  
> — jstauffer  
> _Okay_  
> — Arun Gupta

> **Necessary supporting schema corresponding to the above two such as a staging schema shall be created.**  
> _I don’t think a staging schema is necessary._  
> — jstauffer  
> _Praveen Jha had suggested that it may be useful, but then if we are really eliminating copying over of Analytics data, this may not be required._  
> — Arun Gupta

> **PHP Laravel module queries PostgreSQL directly with least-privilege roles; lower latency and higher availability.**  
> _Detail visit data from Snowflake may still be queried._  
> — jstauffer  
> _Visit data is mainly queried from the nightly jobs. From the front end, mainly the DIM tables (master data) are referred. We can optionally cache this._  
> — Arun Gupta

> **Amazon RDS for PostgreSQL**  
> _We should use Aurora Serverless PostgreSQL._  
> — jstauffer  
> _Changed mention of RDS to serverless Aurora._  
> — Arun Gupta

> **AWS Glue**  
> _What will this be used for?_  
> — jstauffer  
> _Instead of Glue, mentioned Step Functions, to orchestrate the Python programs hosted in AWS Lambda._  
> — Arun Gupta

> **Amazon VPC (private subnets, VPC endpoints),**  
> _Are you creating any new VPCs as part of this? If not, then no reason to mention here._  
> — jstauffer  
> _Yeah, we can review this one._  
> — Arun Gupta

> **and/or Postgres functions**  
> _Stored Procs and Functions are not allowed in our Architecture. **NO POSTGRES FUNCTIONS** should be used_  
> — jstauffer  
> _Okay, we could create the reusable code within Python. But coming from the Snowflake experience, would we be open to create re-usable Views to simplify the logic?_  
> — Arun Gupta

> **S3 for large attachments**  
> _Per comment above, what attachments are involved in this solution?_  
> — jstauffer  
> _Optional documents exchanged in chat between payer and provider._  
> — Arun Gupta

> **and sync/ETL loads**  
> _Again,m won’t exist._  
> — jstauffer  
> _Okay._  
> — Arun Gupta

> **Events/DEX: No new DEX events initially; future eventing on conflict state changes possible.**  
> _DEX is deprecated - we will not be introducing any new events._  
> — jstauffer  
> _Okay._  
> — Arun Gupta

---

## Footer Comments

> 1. What was the reason that Snowflake was used for this in the first place?  Are we losing any functionality that snowflake provides?  
> 2. Please update the TCO portion to include the reduction in cost that we will realize.  It was talked about a bit, but we should have that documented. I’d like to understand when in the process we will start seeing that cost reduction.  
> — Lynn Roth  
> _1. We are not fully aware what went into choosing Snowflake in the first place, since we took over from another vendor in Apr this year. Snowflake is mostly used as a data store, so we don’t know of any particular advantage with Snowflake.  
> 2. It is rough calculations at the moment but summarized in the TCO section as needed._  
> — Arun Gupta  
> _Use of Snowflake as a transactional data store was inappropriate. The original team was explicitly directed not to use Snowflake for this purpose, but did so anyway for convenience, and compounded their bad decisions by implementing conflict logic as stored procs._  
> — jstauffer

> If I’m not mistaken, the Conflict Management UI and backend database were recently operationalized. However, this migration appears to be creating another silo of processed data from Snowflake into PostgreSQL solely for UI “reads”. These UI queries could instead be optimized to reduce costs and avoid the need for an additional data silo, PostgreSQL database, and related ETL processes.  
> Additionally, during our Unified Data Platform (UDP) impact analysis of the Analytics database, we identified that Conflict Management as a downstream consumer. We also noted potential sales opportunities for this interface if offered to Sandata customers (payers and providers) as part of a unified data repository in Snowflake.  
> It’s also important to note that this PG migration and ongoing ETL activity (including Sandata data) are likely to increase the data volume by 2–5x, which would make it significantly more expensive to manage compared to leveraging the current Snowflake database.  
> — Khaleel Gudiyatham  
> _From the recently concluded NY State demo, we can infer that visualization is our USP and users (Payers/Providers/Aggregators) would like to have newer ways to visualize the data, which in turn mean UI queries can get more complex from what they are right now.  
> Migration to PostgreSQL is the right step as more demand builds for Conflict Management._  
> — Arun Gupta

> Noting the “Snowflake credits reduced” - can you elaborate on this? By reducing our snowflake usage, are we losing credits that we have gained up to this point? What is the impact on cost (up to this point) of these credits? What is the impact going forward (can we utilize these credits on separate snowflake environments)?  
> — Kristin Jones  
> _This is basically to save on Snowflake compute cost._  
> — Arun Gupta  
> _“Credits” is the unit of usage for Snowflake - it corresponds to usage (e.g., an X-Small is 1 credit per hour, Small is 2 credits, etc.). We pre-purchase capacity to get a volume discount, and then burn through them via usage. We have a global bucket of credits across all of our Snowflake usage. When the previous team built this functionality on Snowflake, it caused us to run through our pre-purchased credits much too quickly._  
> — jstauffer

---

**Source:** [ARB 555 - Snowflake to Postgres Migration (Confluence)](https://hhaxsupport.atlassian.net/wiki/spaces/PMN/pages/1944223799/ARB+555+-+Snowflake+to+Postgres+Migration)