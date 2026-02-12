# Task 02 Conflict Updater - ECS Deployment Guide (AWS Console)

**Date:** 2026-02-10
**Cluster:** `conflict-batch-1`
**ECR Repository:** `conflict-snowflake`

---

## Lambda vs ECS - What's Different?

| Concern | Lambda (Scripts12) | ECS Container (Scripts13) |
|---|---|---|
| **Layers** | Required 2 Lambda Layers (psycopg2, snowflake-connector) | **No layers.** All dependencies are bundled inside the Docker image via `requirements.txt` |
| **Timeout** | 15-minute hard limit | No limit (runs until done) |
| **Entry point** | `lambda_handler.lambda_handler` | `scripts/main.py` (standalone Python script) |
| **Secrets** | Lambda environment variables (plain text) | Secrets Manager → injected as env vars by ECS |
| **Environment variables** | Same 7 vars | Same 7 vars, same names, same values |
| **Code packaging** | ZIP upload | Docker image pushed to ECR |
| **Logs** | CloudWatch `/aws/lambda/...` | CloudWatch `/ecs/task02-conflict-updater` |

**The 7 environment variables are identical** to what you had in Lambda:

```
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_PRIVATE_KEY
POSTGRES_HOST
POSTGRES_USER
POSTGRES_PASSWORD
```

---

## Prerequisites

- Docker Desktop installed and running on your machine
- AWS Console access with permissions for ECR, ECS, Secrets Manager, IAM, CloudWatch
- The ECS cluster `conflict-batch-1` already exists
- The ECR repository `conflict-snowflake` already exists

---

## Step 1: Build the Docker Image Locally

Open a terminal in `Scripts13/tasks/` (the folder containing the `Dockerfile`).

```powershell
cd Scripts13\tasks

docker build -t conflict-snowflake:latest .
```

**Expected output** (first build takes 2-3 minutes for dependency installation):
```
 => [base 2/6] RUN apt-get update && ...
 => [base 3/6] COPY requirements.txt .
 => [base 4/6] RUN pip install --no-cache-dir -r requirements.txt
 => [base 5/6] COPY config/ config/
 ...
 => => naming to docker.io/library/conflict-snowflake:latest
```

**Verify** the image was built:
```powershell
docker images conflict-snowflake
```
You should see one row with tag `latest`, size ~400-500 MB.

---

## Step 2: Push the Image to ECR

### 2a. Get the push commands from the AWS Console

1. Go to **Amazon ECR** in the AWS Console
2. Click **Repositories** → **conflict-snowflake**
3. Click the **View push commands** button (top right)
4. A dialog appears with 4 commands customized for your account and region

### 2b. Run the push commands

The dialog shows 4 commands. You already did the build in Step 1, so you only need commands **1, 3, and 4**:

**Command 1** -- Authenticate Docker to ECR:
```powershell
aws ecr get-login-password --region <REGION> | docker login --username AWS --password-stdin <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com
```
*(Copy this exactly from the console dialog -- it has your actual account ID and region filled in.)*

Expected: `Login Succeeded`

**Command 3** -- Tag the image:
```powershell
docker tag conflict-snowflake:latest <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/conflict-snowflake:latest
```

**Command 4** -- Push to ECR:
```powershell
docker push <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/conflict-snowflake:latest
```

### 2c. Verify the push

1. Close the push commands dialog
2. You should now see the `latest` tag in the **conflict-snowflake** repository with a recent push timestamp
3. Note the **Image URI** shown -- you'll need it in Step 6

---

## Step 3: Store Secrets in AWS Secrets Manager

You need to create **two secrets** -- one for Snowflake and one for PostgreSQL. These hold the **same values** you had as Lambda environment variables.

### 3a. Create the Snowflake secret

1. Go to **AWS Secrets Manager** in the AWS Console
2. Click **Store a new secret**
3. Secret type: **Other type of secret**
4. Key/value pairs -- click **+ Add row** for each and enter these 4 keys:

   | Key | Value |
   |---|---|
   | `account` | *(your Snowflake account identifier)* |
   | `user` | *(your Snowflake username)* |
   | `warehouse` | *(your Snowflake warehouse name)* |
   | `private_key` | *(your RSA private key content -- paste the full PEM text including the `-----BEGIN PRIVATE KEY-----` and `-----END PRIVATE KEY-----` lines; use `\n` for line breaks if the console doesn't accept real newlines)* |

5. Click **Next**
6. Secret name: **`task02/snowflake`**
7. Click **Next** → **Next** → **Store**

### 3b. Create the PostgreSQL secret

1. Click **Store a new secret**
2. Secret type: **Other type of secret**
3. Key/value pairs -- add these 3 keys:

   | Key | Value |
   |---|---|
   | `host` | *(your PostgreSQL host)* |
   | `user` | *(your PostgreSQL username)* |
   | `password` | *(your PostgreSQL password)* |

4. Click **Next**
5. Secret name: **`task02/postgres`**
6. Click **Next** → **Next** → **Store**

### 3c. Note the secret ARNs

After creating each secret, click into it and copy the **Secret ARN** from the top of the detail page. You'll need these in Step 6. They look like:
```
arn:aws:secretsmanager:us-east-1:123456789012:secret:task02/snowflake-AbCdEf
arn:aws:secretsmanager:us-east-1:123456789012:secret:task02/postgres-GhIjKl
```

---

## Step 4: Create the CloudWatch Log Group

1. Go to **CloudWatch** in the AWS Console
2. Left sidebar: **Logs** → **Log groups**
3. Click **Create log group**
4. Log group name: **`/ecs/task02-conflict-updater`**
5. Retention: choose a retention period (e.g., 30 days)
6. Click **Create**

---

## Step 5: Set Up the IAM Execution Role

The ECS task needs an **execution role** that can pull the ECR image and read Secrets Manager values.

### Option A: Use existing `ecsTaskExecutionRole`

If you already have an `ecsTaskExecutionRole` (ECS often creates one automatically):

1. Go to **IAM** in the AWS Console
2. Left sidebar: **Roles** → search for **`ecsTaskExecutionRole`**
3. Click on the role
4. Click **Add permissions** → **Create inline policy**
5. Click the **JSON** tab and paste:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": [
        "arn:aws:secretsmanager:*:*:secret:task02/snowflake-*",
        "arn:aws:secretsmanager:*:*:secret:task02/postgres-*"
      ]
    }
  ]
}
```

6. Click **Next** → policy name: `Task02SecretsAccess` → **Create policy**

### Option B: Create a new role

1. Go to **IAM** → **Roles** → **Create role**
2. Trusted entity type: **AWS service**
3. Use case: **Elastic Container Service** → **Elastic Container Service Task**
4. Click **Next**
5. Search and attach these policies:
   - `AmazonECSTaskExecutionRolePolicy` (required for ECR pull + CloudWatch logs)
6. Click **Next**
7. Role name: **`ecsTaskExecutionRole`**
8. Click **Create role**
9. Then click into the new role and add the inline Secrets Manager policy from Option A above (steps 4-6)

### Note the role ARN

On the role's detail page, copy the **ARN** from the top. It looks like:
```
arn:aws:iam::123456789012:role/ecsTaskExecutionRole
```

---

## Step 6: Create the ECS Task Definition

1. Go to **Amazon ECS** in the AWS Console
2. Left sidebar: **Task definitions**
3. Click **Create new task definition** → **Create new task definition with JSON**
4. Delete the default JSON and paste the JSON below
5. **Replace these 5 placeholders** before submitting:

   | Placeholder | Replace with | Where to find it |
   |---|---|---|
   | `<EXECUTION_ROLE_ARN>` | IAM role ARN | Step 5 |
   | `<ACCOUNT_ID>` | Your 12-digit AWS account ID | Top-right of any AWS Console page |
   | `<REGION>` | Your AWS region (e.g., `us-east-1`) | Top-right of any AWS Console page |
   | `<SNOWFLAKE_SECRET_ARN>` | Full Snowflake secret ARN | Step 3c |
   | `<POSTGRES_SECRET_ARN>` | Full PostgreSQL secret ARN | Step 3c |

```json
{
  "family": "task02-conflict-updater",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "1024",
  "memory": "2048",
  "executionRoleArn": "<EXECUTION_ROLE_ARN>",
  "containerDefinitions": [
    {
      "name": "task02-conflict-updater",
      "image": "<ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/conflict-snowflake:latest",
      "essential": true,
      "environment": [
        { "name": "ACTION", "value": "task02_00_run_conflict_update" },
        { "name": "ENVIRONMENT", "value": "dev" },
        { "name": "LOG_LEVEL", "value": "INFO" }
      ],
      "secrets": [
        {
          "name": "SNOWFLAKE_ACCOUNT",
          "valueFrom": "<SNOWFLAKE_SECRET_ARN>:account::"
        },
        {
          "name": "SNOWFLAKE_USER",
          "valueFrom": "<SNOWFLAKE_SECRET_ARN>:user::"
        },
        {
          "name": "SNOWFLAKE_WAREHOUSE",
          "valueFrom": "<SNOWFLAKE_SECRET_ARN>:warehouse::"
        },
        {
          "name": "SNOWFLAKE_PRIVATE_KEY",
          "valueFrom": "<SNOWFLAKE_SECRET_ARN>:private_key::"
        },
        {
          "name": "POSTGRES_HOST",
          "valueFrom": "<POSTGRES_SECRET_ARN>:host::"
        },
        {
          "name": "POSTGRES_USER",
          "valueFrom": "<POSTGRES_SECRET_ARN>:user::"
        },
        {
          "name": "POSTGRES_PASSWORD",
          "valueFrom": "<POSTGRES_SECRET_ARN>:password::"
        }
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/task02-conflict-updater",
          "awslogs-region": "<REGION>",
          "awslogs-stream-prefix": "task02"
        }
      },
      "stopTimeout": 30
    }
  ],
  "runtimePlatform": {
    "cpuArchitecture": "X86_64",
    "operatingSystemFamily": "LINUX"
  }
}
```

6. Click **Create**

---

## Step 7: Test -- Run a Connection Test First

Start with a connection test before running the full pipeline.

### 7a. Run the connection test task

1. Go to **Amazon ECS** → **Clusters** → **conflict-batch-1**
2. Click the **Tasks** tab
3. Click **Run new task**
4. Configure:
   - **Compute options**: Launch type
   - **Launch type**: FARGATE
   - **Task definition**:
     - Family: `task02-conflict-updater`
     - Revision: LATEST
   - **Desired tasks**: 1
5. Under **Networking**:
   - Select a **VPC** and **subnet(s)** that have access to both Snowflake (internet) and PostgreSQL
   - **Security group**: must allow outbound HTTPS (port 443) for Snowflake and outbound TCP (port 5432) for PostgreSQL
   - **Auto-assign public IP**: ENABLED (needed for Snowflake connectivity unless you have a NAT gateway)
6. Expand **Container overrides** → click on **task02-conflict-updater**
7. Under **Environment variable overrides**, click **Add environment variable**:
   - Key: `ACTION`
   - Value: `test_connections`
8. Click **Create** (or **Run task**)

### 7b. Monitor the connection test

1. You'll be taken to the task list. Click on the task ID that just appeared
2. Wait for **Last status** to change: `PROVISIONING` → `PENDING` → `RUNNING` → `STOPPED`
   (Connection test takes ~30 seconds once running)
3. Click the **Logs** tab on the task detail page to see output
4. Look for these success messages:
   ```
   Snowflake connection successful: X.XX.X
   PostgreSQL connection successful
   ```
   And the table list at the end.

### 7c. Check the exit code

On the task detail page, scroll to the **Containers** section:
- **Exit code**: `0` = success, `1` = failure
- If exit code is `1`, the **Logs** tab will show the error

### Troubleshooting connection test failures

| Symptom | Likely cause | Fix |
|---|---|---|
| Task stays in `PROVISIONING` then stops | `CannotPullContainerError` -- ECR image not found or role can't pull | Check the **image** URI in the task definition matches your ECR repo exactly; verify the execution role has `AmazonECSTaskExecutionRolePolicy` attached |
| `ResourceInitializationError` | Secrets Manager access denied | Open the execution role in IAM and verify the inline Secrets Manager policy exists with the correct secret ARNs |
| `Failed to connect to Snowflake` | RSA key issue or network | Check the `private_key` value in Secrets Manager; ensure the subnet has internet access (public IP enabled or NAT gateway) |
| `Failed to connect to PostgreSQL` | Network or credentials | Check the security group allows outbound on port 5432; verify PG credentials in Secrets Manager |
| Exit code `137` | Out of memory | Go to **Task definitions**, create a new revision with higher memory (e.g., 4096) |

---

## Step 8: Run the Full Conflict Update Pipeline

Once the connection test passes:

1. Go to **ECS** → **Clusters** → **conflict-batch-1** → **Tasks** tab → **Run new task**
2. Same settings as Step 7a, **except do not add the ACTION override** (it defaults to `task02_00_run_conflict_update`)
3. Click **Create**

### Monitor the full run

1. Click on the task ID
2. Click the **Logs** tab
3. Expected duration: **~8-10 minutes** based on the latest performance numbers
4. Watch for the step-by-step progress in the logs:

   ```
   TASK 02 CONFLICT UPDATER - ECS CONTAINER
   Action: task02_00_run_conflict_update
   ...
   Step 0 (excluded SSNs): ~6s
   Step 1 (delta_keys): ~5s
   Step 2 Part A (base_visits delta): ~30s
   Step 2 Part B (base_visits related): ~120s
   Step 2d (delta pairs to PG): ~90s
   Step 3 (conflict detection streaming): ~140s
   Step 4 (stale cleanup): ~120s
   ...
   EXECUTION RESULT
   Status: completed
   Duration: 8m 29s
   ```

5. Final confirmation: **Exit code** = `0` and `Status: completed` in the logs

### Verify results in PostgreSQL

After a successful run, verify in your PostgreSQL database:

```sql
-- Check recently updated conflicts
SELECT COUNT(*), "StatusFlag"
FROM conflict_dev.conflictvisitmaps
WHERE "UpdatedDate" >= NOW() - INTERVAL '30 minutes'
GROUP BY "StatusFlag";

-- Check stale conflicts resolved in this run
SELECT COUNT(*)
FROM conflict_dev.conflictvisitmaps
WHERE "StatusFlag" = 'R'
AND "UpdatedDate" >= NOW() - INTERVAL '30 minutes';
```

---

## Step 9: Validate Config (Optional Quick Test)

If you want to verify the config loads correctly without connecting to any databases:

1. Run a new task (same as Step 7a) with container override: `ACTION` = `validate_config`
2. This completes in ~5 seconds and logs the parsed configuration
3. Useful to confirm Secrets Manager values are being injected properly

---

## Running with Parameter Overrides

To override default parameters, add them as **Container overrides** → **Environment variable overrides** when running a task:

| Variable | Default | Example override |
|---|---|---|
| `LOOKBACK_HOURS` | 36 | `48` |
| `LOOKBACK_YEARS` | 2 | `1` |
| `LOOKFORWARD_DAYS` | 45 | `30` |
| `BATCH_SIZE` | 5000 | `10000` |
| `ENABLE_ASYMMETRIC_JOIN` | true | `false` |
| `ENABLE_STALE_CLEANUP` | true | `false` |

---

## Redeploying After Code Changes

### Option A: Automated Deploy Script (Recommended)

The interactive PowerShell script handles the full workflow:

```powershell
cd Scripts13\tasks\deploy
.\build-and-push-ecr.ps1
```

The script walks you through 5 steps, each with a Y/n prompt:

1. **SSO Login** -- Authenticates to AWS (skip if session is still active)
2. **Build Docker Image** -- Builds from `Scripts13/tasks/Dockerfile`
3. **Push to ECR** -- Tags with `latest` + timestamp, pushes both tags
4. **Register Task Definition** -- Resolves `ecs-task-definition.json` template with secrets from `deploy/.env`, shows a masked summary, and registers with ECS via `aws ecs register-task-definition`
5. **Run ECS Task** -- Interactive menu to run the default pipeline, individual actions, or custom action combinations

**First-time setup for the deploy script:**

1. Copy the example env file and fill in your credentials:
   ```powershell
   cd Scripts13\tasks\deploy
   copy .env.example .env
   # Edit .env with real Snowflake, PostgreSQL, AWS, and email values
   ```
2. Verify prerequisites (see comments at the top of `build-and-push-ecr.ps1` for the full list):
   - Docker Desktop installed and running
   - AWS CLI v2 with SSO profile configured
   - ECR repository, ECS cluster, CloudWatch log group, and IAM role created

### Option B: Manual AWS Console Steps

When you make code changes and need to update the running image manually:

1. **Rebuild** the image locally:
   ```powershell
   cd Scripts13\tasks
   docker build -t conflict-snowflake:latest .
   ```

2. **Push** to ECR:
   - Go to **Amazon ECR** → **conflict-snowflake** → **View push commands**
   - Run commands 1, 3, and 4 from the dialog (authenticate, tag, push)

3. **Run a new task** from the ECS console -- it will automatically pull the updated `:latest` image.
   No task definition changes needed unless you modify CPU, memory, or secrets.
