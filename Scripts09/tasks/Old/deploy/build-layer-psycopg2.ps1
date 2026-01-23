<#
.SYNOPSIS
    Builds the AWS Lambda Layer for psycopg2.
    
.DESCRIPTION
    Creates a zip file containing psycopg2-binary compatible with AWS Lambda (Amazon Linux 2).
    This layer is separated because psycopg2 often has binary compatibility issues.

.NOTES
    Run from Scripts09/Tasks/deploy folder.
#>

$ErrorActionPreference = "Stop"
$LayerName = "layer-psycopg2.zip"
$BuildDir = "build_layer_psycopg2"
$ReqFile = "requirements-psycopg2.txt"

# Cleanup
if (Test-Path $BuildDir) { Remove-Item -Path $BuildDir -Recurse -Force }
if (Test-Path $LayerName) { Remove-Item -Path $LayerName -Force }

# Create structure: python/lib/python3.11/site-packages
# AWS Lambda extracts /python to /opt/python, so site-packages should be directly in python/ or python/lib/python3.x/site-packages
$TargetDir = "$BuildDir\python"
New-Item -ItemType Directory -Path $TargetDir -Force | Out-Null

Write-Host "Installing psycopg2-binary..."
# Important: --platform manylinux2014_x86_64 for AWS Lambda compatibility
pip install -r $ReqFile -t $TargetDir --platform manylinux2014_x86_64 --only-binary=:all: --implementation cp --python-version 3.11 --upgrade

Write-Host "Zipping layer..."
Compress-Archive -Path "$BuildDir\*" -DestinationPath $LayerName

Write-Host "Created $LayerName"
Write-Host "Upload this to AWS Lambda Layers."
