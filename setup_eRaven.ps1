param(
    [Parameter(Mandatory=$true)] [string]$TargetDatabase,
    [Parameter(Mandatory=$true)] [string]$AppName,
    [Parameter(Mandatory=$true)] [boolean]$FlagBuildRaven = $False,
    [Parameter(Mandatory=$false)] [int]$EnvNum = 1
)

# Get python installation path
$PythonPath = ($Env:PATH -split ';') | Where-Object {$_ -like "*\Python38\"}

$EnvName = ".venv"

# Create virtual environment
if (Test-Path -Path "$PSScriptRoot\$EnvName") {
    Write-Host "Python environment exists"
} else {
    Write-Host "Creating Python environment"
    & "$PythonPath\python.exe" -m venv "$PSScriptRoot\$EnvName"
    & "$PSScriptRoot\$EnvName\Scripts\python.exe" -m pip install --upgrade pip
}

# Set Location
& Set-Location $PSScriptRoot

# Activate virtual environment
& "$PSScriptRoot\$EnvName\Scripts\Activate.ps1"

# Install requirements
& "$PSScriptRoot\$EnvName\Scripts\pip.exe" install -r "$PSScriptRoot\requirements.txt"

& "$PSScriptRoot\$EnvName\Scripts\pip.exe" install snowflake-snowpark-python[pandas]

# Install mufg_snowflakeconn
#& "$PSScriptRoot\$EnvName\Scripts\pip.exe"  install "$PSScriptRoot\mufgsnowflakeconn\mufg_snowflakeconn-1.0.3-py3-none-any.whl"

# Call Build eRaven
if ($FlagBuildRaven){

    $python_exe = "$PSScriptRoot\$EnvName\Scripts\python.exe"
    $script = "$PSScriptRoot\automate_build.py"
    $db_arg = "-db=$TargetDatabase"
    $app_arg = "-app=$AppName"
    $metadata_arg = "-metadata=True"
    $build_arg = "-build=True"
    $grant_arg = "-grant=True"
    $env_num = "-envn=$EnvNum"
    & $python_exe $script $db_arg $app_arg $metadata_arg $build_arg $grant_arg $env_num
    
}

# Deactivate virtual environment
& deactivate
