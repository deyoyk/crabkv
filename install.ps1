# CrabKV Installer for Windows
# Usage:
#   .\install.ps1
#   .\install.ps1 -Port 6380
#   .\install.ps1 -Autostart

param(
    [int]$Port = 6379,
    [string]$InstallDir = "$env:LOCALAPPDATA\CrabKV",
    [switch]$Autostart,
    [string]$Version = "latest"
)

$Repo = "deyoyk/crabkv"

Write-Host "CrabKV Installer" -ForegroundColor Cyan
Write-Host "================"
Write-Host ""

# Get latest version
if ($Version -eq "latest") {
    $release = Invoke-RestMethod -Uri "https://api.github.com/repos/$Repo/releases/latest"
    $Version = $release.tag_name
    Write-Host "Latest version: $Version"
}

# Download
$url = "https://github.com/$Repo/releases/download/$Version/crabkv-windows-amd64.zip"
$zipPath = "$env:TEMP\crabkv.zip"

Write-Host "Downloading from $url..."
Invoke-WebRequest -Uri $url -OutFile $zipPath

# Extract
Write-Host "Installing to $InstallDir..."
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
Expand-Archive -Path $zipPath -DestinationPath $InstallDir -Force

$binaryPath = "$InstallDir\crabkv.exe"

# Add to PATH
$currentPath = [Environment]::GetEnvironmentVariable("Path", "User")
if ($currentPath -notlike "*$InstallDir*") {
    [Environment]::SetEnvironmentVariable("Path", "$currentPath;$InstallDir", "User")
    Write-Host "Added $InstallDir to PATH" -ForegroundColor Green
    $env:Path = "$env:Path;$InstallDir"
}

# Autostart via Task Scheduler
if ($Autostart) {
    $action = New-ScheduledTaskAction -Execute $binaryPath -Argument "-p $Port"
    $trigger = New-ScheduledTaskTrigger -AtLogon
    $settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)

    Register-ScheduledTask -TaskName "CrabKV" -Action $action -Trigger $trigger -Settings $settings -Force | Out-Null
    Write-Host "Autostart configured via Task Scheduler" -ForegroundColor Green
}

# Start server
Write-Host "Starting CrabKV on port $Port..."
Start-Process -FilePath $binaryPath -ArgumentList "-p $Port" -WindowStyle Hidden
Write-Host ""
Write-Host "CrabKV is running on port $Port" -ForegroundColor Green
Write-Host "Connect with: redis-cli -p $Port"

# Cleanup
Remove-Item $zipPath -ErrorAction SilentlyContinue
