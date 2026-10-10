# Запуск команд gate_stonk. Live требует app.mode: live, safety.dedicated_account_ack: true,
# ключ -Live и переменную окружения оболочки (не в .env):
#   $env:GATE_LIVE_CONFIRM = "I_UNDERSTAND_SPOT_TRADING"
param(
    [ValidateSet("doctor", "index-watch", "run", "cancel-only", "status", "report", "validate", "simulate")]
    [string]$Command = "run",
    [string]$Config = "config.yaml",
    [switch]$Live,
    [double]$Minutes = 0,
    [double]$Seconds = 0,
    [int]$Seed = 1,
    [double]$Hours = 24,
    [string]$Csv = ""
)
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
$Python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) { throw "Сначала выполните setup_windows.ps1." }
$Arguments = @("-m", "gate_stonk", $Command, "--config", $Config)
$Inv = [System.Globalization.CultureInfo]::InvariantCulture
if ($Command -eq "run" -and $Live) { $Arguments += "--live" }
if ($Command -eq "run" -and $Seconds -gt 0) { $Arguments += @("--seconds", $Seconds.ToString($Inv)) }
if ($Command -eq "simulate") { $Arguments += @("--seed", $Seed.ToString($Inv)) }
if ($Command -eq "simulate" -and $Minutes -gt 0) { $Arguments += @("--minutes", $Minutes.ToString($Inv)) }
if ($Command -eq "index-watch" -and $Minutes -gt 0) { $Arguments += @("--minutes", $Minutes.ToString($Inv)) }
if ($Command -eq "index-watch" -and $Csv -ne "") { $Arguments += @("--csv", $Csv) }
if ($Command -eq "report") { $Arguments += @("--hours", $Hours.ToString($Inv)) }
& $Python @Arguments
exit $LASTEXITCODE
