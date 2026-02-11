## **Sequential Task Summary**

### **TASK_01** - Copy Data from ConflictVisitMaps to Temp
Truncates and refreshes the temporary staging table `CONFLICTVISITMAPS_TEMP` with recent conflict data (2 years back to 45 days forward) and manages payer/provider reminders.

### **TASK_02_0** - Update ConflictVisitMaps (Main Conflicts)
Updates existing conflict records for **same schedule time, same visit time, overlapping times, and distance-based conflicts**. Sets `UpdateFlag = 1` for records within the date range before performing updates.

### **TASK_02_1** - Update ConflictVisitMaps (In-Service Conflicts)
Updates existing conflict records specifically for **In-Service conflicts**, handling both reverse in-service updates and new in-service conflicts based on caregiver in-service schedules.

### **TASK_02_3** - Update Status and Calculate Rates
Marks conflicts as **'Deleted' or 'Resolved'** based on deleted/missed visits. Calculates `BilledRateMinute` for both primary and conflicting visits, and updates various status flags and resolve dates.

### **TASK_03_0** - Insert New Conflicts (Main Types)
Inserts **new conflict records** for same schedule time, same visit time, overlapping times, and distance-based conflicts. Ensures no duplicate insertions using `NOT EXISTS` checks.

### **TASK_03_1** - Insert New Conflicts (PTO)
Inserts **new PTO (Paid Time Off) conflict records**, handling both direct PTO conflicts (visit vs PTO) and reverse scenarios, ensuring no duplicates.

### **TASK_03_2** - Insert New Conflicts (In-Service)
Inserts **new In-Service conflict records**, handling both direct and reverse in-service conflicts. Also updates `ShVTSTTime`, `ShVTENTime`, and `ReverseUUID` columns for proper conflict linking.

### **TASK_04** - Assign ConflictIDs
Assigns unique `CONFLICTID` values to new conflict records that don't have one yet using `ROW_NUMBER()`. Also updates `CRDATEUNIQUE` with the minimum created date for each conflict group.

### **TASK_05** - Insert into Conflicts Table
Inserts new `CONFLICTID`s into the `CONFLICTS` master table. Updates payer/provider reminders, sets status flags for missed visits, and creates notifications and communication interactions for new and resolved conflicts.

### **TASK_06** - Assign Group IDs
Assigns `TempGroupID` and `GroupID` to link related conflicts using hierarchical grouping logic. Creates notifications for conflicts that remain unresolved beyond a threshold number of days.

### **TASK_07** - Update Phone Contact Information
Updates `AgencyContact` and `AgencyPhone` fields for both primary and conflicting provider visits using data from the `CONTACT_MAINTENANCE` table (only where contact info is currently missing).

### **TASK_08** - Create New Log History
Inserts new records into `LOG_HISTORY` for newly inserted conflicts (`LogTypeFlag = 'Inserted'`) and populates `LOG_HISTORY_VALUES` with initial field values for audit tracking.

### **TASK_09_0** - Update Log History
Logs **updates** to existing conflict records by comparing current and previous values. Inserts changed fields into `LOG_HISTORY` and `LOG_HISTORY_VALUES` with `LogTypeFlag = 'UpdatedNew'`. Updates `LastLoadDate` and `InProgressFlag` in settings.

### **TASK_09_1** - Get Final Billable Units (Python API Call)
**Python stored procedure** that calls an external HHAeXchange Revenue API to fetch billable units. Updates `BILLABLEMINUTESFULLSHIFT`, `BILLABLEUNITSFULLSHIFT`, `BILLABLEMINUTESOVERLAP`, and `BILLABLEUNITSOVERLAP` fields. Logs API failures.

### **TASK_10** - Load Provider Dashboard Data
Populates **provider-specific dashboard tables** with conflict metrics including total counts, shift prices, overlap prices, and final prices, grouped by provider, office, date, conflict type, agency, caregiver, and patient.

### **TASK_11** - Load Payer Dashboard Data
Populates **payer-specific dashboard tables** with conflict counts and financial impacts, grouped by payer, visit date, conflict type, agency, patient, and caregiver.

### **TASK_12** - Load Payer Dashboard Chart Data
Populates **state-level and payer chart dashboard tables** with conflict counts and financial impacts, grouped by payer, provider, visit date, conflict type, status, cost type, visit type, county, and service code.

### **TASK_13** - Load Payer Conflict Summary
Creates **detailed payer conflict summary tables** (`COUNT` and `IMPACT`) with comprehensive breakdown including payer, provider, TIN, contract, patient details, caregiver name, conflict type, visit date, and status flags.

---

**Overall Flow**: The scripts execute a complete ETL pipeline that:
1. Stages data (TASK_01)
2. Updates existing conflicts (TASK_02_*)
3. Inserts new conflicts (TASK_03_*)
4. Assigns IDs and groups (TASK_04-06)
5. Enriches contact data (TASK_07)
6. Tracks changes via logging (TASK_08-09_0)
7. Fetches external billable units (TASK_09_1)
8. Populates dashboard tables (TASK_10-13)