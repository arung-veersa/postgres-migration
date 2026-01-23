<#
.SYNOPSIS
    Builds the AWS Lambda Code Package.
    
.DESCRIPTION
    Bundles lambda_handler.py, source folders, and small dependencies (python-dotenv).
    Does NOT include large binaries like psycopg2 (provided via Layer).

.NOTES
    Run from Scripts09/Tasks/deploy folder.
#>

$ErrorActionPreference = "Stop"
$PackageName = "lambda-code.zip"
$BuildDir = "build_code"
$RootTaskDir = ".."  # Relative to deploy folder

# Cleanup
if (Test-Path $BuildDir) { Remove-Item -Path $BuildDir -Recurse -Force }
if (Test-Path $PackageName) { Remove-Item -Path $PackageName -Force }

New-Item -ItemType Directory -Path $BuildDir | Out-Null

# 1. Copy Lambda Handler
Write-Host "Copying lambda_handler.py..."
Copy-Item -Path "$RootTaskDir\lambda_handler.py" -Destination $BuildDir

# 2. Copy Source Directories
$DirectoriesToCopy = @("src", "config", "connectors", "utils", "sql")
foreach ($Dir in $DirectoriesToCopy) {
    $SourcePath = "$RootTaskDir\$Dir"
    if (Test-Path $SourcePath) {
        Write-Host "Copying $Dir directory..."
        Copy-Item -Path $SourcePath -Destination $BuildDir -Recurse
    } else {
        Write-Warning "$Dir directory not found at $SourcePath!"
    }
}

# 3. Install Small Dependencies (from requirements.txt)
Write-Host "Installing dependencies from requirements.txt..."
pip install -r "$RootTaskDir\requirements.txt" -t $BuildDir --quiet

# 4. Clean up __pycache__ and dist-info (optional, but keeps it clean)
Write-Host "Cleaning up..."
Get-ChildItem -Path $BuildDir -Recurse -Filter "__pycache__" | Remove-Item -Recurse -Force
# We keep dist-info for pip consistency, but you could remove it if you really want to shave KB.

# 5. Zip
Write-Host "Zipping code package..."
Compress-Archive -Path "$BuildDir\*" -DestinationPath $PackageName

Write-Host "Created $PackageName"
Write-Host "Deploy this to AWS Lambda Function Code."
