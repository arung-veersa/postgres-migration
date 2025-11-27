#!/bin/bash
# Deploy AWS Step Functions State Machine
# This creates/updates the Step Functions state machine for ETL pipeline orchestration

set -e

# Parse arguments
LAMBDA_FUNCTION_ARN=""
STATE_MACHINE_NAME="cm-etl-pipeline-state-machine"
ROLE_ARN=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --lambda-arn)
      LAMBDA_FUNCTION_ARN="$2"
      shift 2
      ;;
    --name)
      STATE_MACHINE_NAME="$2"
      shift 2
      ;;
    --role-arn)
      ROLE_ARN="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Validate inputs
if [ -z "$LAMBDA_FUNCTION_ARN" ]; then
    echo "ERROR: Lambda Function ARN is required"
    echo "Usage: ./deploy_step_functions.sh --lambda-arn 'arn:aws:lambda:...'"
    exit 1
fi

echo "============================================="
echo "Deploying Step Functions State Machine"
echo "============================================="
echo ""
echo "Configuration:"
echo "  State Machine Name: $STATE_MACHINE_NAME"
echo "  Lambda Function ARN: $LAMBDA_FUNCTION_ARN"
echo ""

# Read and update definition
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFINITION_FILE="$SCRIPT_DIR/step_functions/etl_pipeline.json"

if [ ! -f "$DEFINITION_FILE" ]; then
    echo "ERROR: State machine definition not found: $DEFINITION_FILE"
    exit 1
fi

# Replace ARN placeholder
DEFINITION=$(cat "$DEFINITION_FILE" | sed "s|arn:aws:lambda:REGION:ACCOUNT_ID:function:etl-pipeline-function|$LAMBDA_FUNCTION_ARN|g")

echo "[1/3] Checking if state machine exists..."
EXISTING_ARN=$(aws stepfunctions list-state-machines --query "stateMachines[?name=='$STATE_MACHINE_NAME'].stateMachineArn" --output text 2>/dev/null || echo "")

if [ -n "$EXISTING_ARN" ]; then
    echo "  State machine exists: $EXISTING_ARN"
    
    # Get role ARN if not provided
    if [ -z "$ROLE_ARN" ]; then
        echo "  Getting role ARN from existing state machine..."
        ROLE_ARN=$(aws stepfunctions describe-state-machine --state-machine-arn "$EXISTING_ARN" --query "roleArn" --output text)
        echo "  Using existing role: $ROLE_ARN"
    fi
    
    echo "[2/3] Updating state machine definition..."
    
    # Create temp file
    TEMP_FILE=$(mktemp)
    echo "$DEFINITION" > "$TEMP_FILE"
    
    aws stepfunctions update-state-machine \
        --state-machine-arn "$EXISTING_ARN" \
        --definition "file://$TEMP_FILE" \
        --role-arn "$ROLE_ARN"
    
    rm "$TEMP_FILE"
    echo "  ✓ State machine updated successfully"
else
    echo "  State machine does not exist. Creating new..."
    
    if [ -z "$ROLE_ARN" ]; then
        echo ""
        echo "ERROR: Role ARN is required for creating new state machine"
        echo ""
        echo "Please create an IAM role and pass it via --role-arn parameter"
        echo "See: aws/README.md for role creation instructions"
        exit 1
    fi
    
    echo "[2/3] Creating state machine..."
    
    # Create temp file
    TEMP_FILE=$(mktemp)
    echo "$DEFINITION" > "$TEMP_FILE"
    
    RESULT=$(aws stepfunctions create-state-machine \
        --name "$STATE_MACHINE_NAME" \
        --definition "file://$TEMP_FILE" \
        --role-arn "$ROLE_ARN" \
        --type STANDARD \
        --output json)
    
    EXISTING_ARN=$(echo "$RESULT" | jq -r '.stateMachineArn')
    rm "$TEMP_FILE"
    echo "  ✓ State machine created: $EXISTING_ARN"
fi

echo "[3/3] Verifying deployment..."
aws stepfunctions describe-state-machine --state-machine-arn "$EXISTING_ARN" > /dev/null

echo ""
echo "============================================="
echo "✓ Deployment Successful"
echo "============================================="
echo ""
echo "State Machine ARN: $EXISTING_ARN"
echo ""
echo "Next Steps:"
echo "  1. Go to AWS Step Functions Console"
echo "  2. Find your state machine: $STATE_MACHINE_NAME"
echo "  3. Click 'Start execution'"
echo "  4. Use input: {}"
echo ""
echo "Or execute via CLI:"
echo "  aws stepfunctions start-execution --state-machine-arn $EXISTING_ARN"
echo ""
echo "============================================="

