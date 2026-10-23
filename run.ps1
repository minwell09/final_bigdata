# Helper chạy script trong venv với JDK 17 (Windows PowerShell)
#   .\run.ps1 src\env_check.py
#   .\run.ps1 src\ingest_all.py
param([Parameter(Mandatory = $true, Position = 0)][string]$Script,
      [Parameter(ValueFromRemainingArguments = $true)]$Rest)
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
if (-not $env:JAVA_HOME) {
    $jdk = Get-ChildItem "C:\Program Files\Eclipse Adoptium" -Directory -Filter "jdk-17*" -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($jdk) { $env:JAVA_HOME = $jdk.FullName }
}
if ($env:JAVA_HOME) { $env:Path = "$env:JAVA_HOME\bin;$env:Path" }
$env:PYTHONIOENCODING = "utf-8"
& "$PSScriptRoot\.venv\Scripts\python.exe" $Script @Rest
exit $LASTEXITCODE
