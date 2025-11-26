#!/usr/bin/env bash
###############################################################################
# AWS Lambda Deployment Script (Automated)
# 
# This script automates the complete Lambda deployment process:
#   1. Builds the deployment package
#   2. Uploads to AWS Lambda
#   3. Configures Lambda settings
#   4. Tests the deployment
#
# Prerequisites:
#   - AWS CLI installed and configured
#   - Python 3.11+ installed
#   - Lambda function already created in AWS
#
# Usage:
#   ./deploy/deploy_to_aws.sh YOUR_LAMBDA_FUNCTION_NAME
#
###############################################################################

set -e  # Exit on error

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Functions
print_header() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}\n"
}

print_step() {
    echo -e "${GREEN}[Step $1]${NC} $2"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

# Check arguments
if [ $# -eq 0 ]; then
    print_error "Missing Lambda function name"
    echo "Usage: $0 YOUR_LAMBDA_FUNCTION_NAME"
    echo ""
    echo "Example:"
    echo "  $0 etl-pipeline-function"
    exit 1
fi

FUNCTION_NAME=$1
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ZIP_FILE="$PROJECT_ROOT/deploy/lambda_deployment.zip"

print_header "AWS LAMBDA DEPLOYMENT"
echo "Function: $FUNCTION_NAME"
echo "Project: $PROJECT_ROOT"
echo ""

# Step 1: Build deployment package
print_step 1 "Building deployment package"
cd "$PROJECT_ROOT"
python deploy/deploy_lambda.py

if [ ! -f "$ZIP_FILE" ]; then
    print_error "Deployment package not created"
    exit 1
fi

print_success "Package built successfully"
SIZE=$(du -h "$ZIP_FILE" | cut -f1)
echo "  Package size: $SIZE"

# Step 2: Verify AWS CLI is configured
print_step 2 "Verifying AWS CLI"
if ! command -v aws &> /dev/null; then
    print_error "AWS CLI not found"
    echo "Install: https://aws.amazon.com/cli/"
    exit 1
fi

AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "")
if [ -z "$AWS_ACCOUNT" ]; then
    print_error "AWS CLI not configured"
    echo "Run: aws configure"
    exit 1
fi

print_success "AWS CLI configured (Account: $AWS_ACCOUNT)"

# Step 3: Check if Lambda function exists
print_step 3 "Checking Lambda function exists"
if aws lambda get-function --function-name "$FUNCTION_NAME" &> /dev/null; then
    print_success "Function exists: $FUNCTION_NAME"
else
    print_error "Function not found: $FUNCTION_NAME"
    echo "Create the function first in AWS Console or run:"
    echo "  aws lambda create-function --function-name $FUNCTION_NAME ..."
    exit 1
fi

# Step 4: Upload code
print_step 4 "Uploading code to Lambda"
echo "  This may take 1-2 minutes..."

UPLOAD_RESULT=$(aws lambda update-function-code \
    --function-name "$FUNCTION_NAME" \
    --zip-file "fileb://$ZIP_FILE" \
    --output json)

if [ $? -eq 0 ]; then
    print_success "Code uploaded successfully"
    LAST_MODIFIED=$(echo "$UPLOAD_RESULT" | grep -o '"LastModified"[^,]*' | cut -d'"' -f4)
    echo "  Last modified: $LAST_MODIFIED"
else
    print_error "Code upload failed"
    exit 1
fi

# Step 5: Update Lambda configuration
print_step 5 "Updating Lambda configuration"

# Update handler
aws lambda update-function-configuration \
    --function-name "$FUNCTION_NAME" \
    --handler "lambda_handler.lambda_handler" \
    --timeout 900 \
    --memory-size 512 \
    --output text &> /dev/null

print_success "Configuration updated"
echo "  Handler: lambda_handler.lambda_handler"
echo "  Timeout: 900 seconds (15 minutes)"
echo "  Memory: 512 MB"

# Step 6: Wait for function to be ready
print_step 6 "Waiting for function to be ready"
echo "  Waiting for updates to complete..."

MAX_WAIT=60
WAITED=0
while [ $WAITED -lt $MAX_WAIT ]; do
    STATE=$(aws lambda get-function --function-name "$FUNCTION_NAME" \
        --query 'Configuration.State' --output text)
    
    if [ "$STATE" == "Active" ]; then
        print_success "Function is ready"
        break
    fi
    
    echo "  State: $STATE (waiting...)"
    sleep 5
    WAITED=$((WAITED + 5))
done

if [ $WAITED -ge $MAX_WAIT ]; then
    print_warning "Function state check timed out (still deploying)"
fi

# Step 7: Test deployment
print_step 7 "Testing deployment"

echo "  Testing validate_config action..."
TEST_RESULT=$(aws lambda invoke \
    --function-name "$FUNCTION_NAME" \
    --payload '{"action": "validate_config"}' \
    --cli-binary-format raw-in-base64-out \
    /tmp/lambda_test_output.json 2>&1)

if grep -q "200" /tmp/lambda_test_output.json 2>/dev/null; then
    print_success "Test passed: validate_config"
else
    print_warning "Test failed or returned non-200 status"
    echo "  Check CloudWatch logs for details"
fi

# Cleanup
rm -f /tmp/lambda_test_output.json

# Summary
print_header "DEPLOYMENT COMPLETE"

echo -e "${GREEN}${BOLD}✓ Lambda function deployed successfully!${NC}\n"

echo "Function Details:"
echo "  Name: $FUNCTION_NAME"
echo "  Handler: lambda_handler.lambda_handler"
echo "  Runtime: Python 3.11+"
echo "  Timeout: 900 seconds"
echo "  Memory: 512 MB"
echo ""

echo "Next Steps:"
echo "  1. Configure environment variables (if not already set):"
echo "     - POSTGRES_HOST, POSTGRES_DATABASE, POSTGRES_USER, etc."
echo ""
echo "  2. Test in AWS Console:"
echo "     https://console.aws.amazon.com/lambda/home?#/functions/$FUNCTION_NAME"
echo ""
echo "  3. View CloudWatch logs:"
echo "     aws logs tail /aws/lambda/$FUNCTION_NAME --follow"
echo ""
echo "  4. Test actions:"
echo "     - validate_config: {\"action\": \"validate_config\"}"
echo "     - task_01: {\"action\": \"task_01\"}"
echo "     - task_02: {\"action\": \"task_02\"}"
echo ""

print_success "Deployment script completed"

