# Установка gate_stonk на Windows: venv, зависимости, проверка конфигурации и офлайн-тесты.
# Торговлю НЕ запускает.
param([string]$Config = "config.yaml")
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

if (-not (Test-Path ".venv\Scripts\python.exe")) {
    py -3.12 -m venv .venv
    if ($LASTEXITCODE -ne 0) { throw "Установите Python 3.12 x64 с python.org и повторите setup." }
}
$Python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
& $Python -m pip install -e ".[test]"
if ($LASTEXITCODE -ne 0) { throw "Не удалось установить зависимости." }
if (-not (Test-Path $Config)) {
    Copy-Item "config.example.yaml" $Config
    Write-Host "Создан $Config из config.example.yaml (режим paper). Проверьте параметры."
}
if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Создан .env из .env.example. Ключи вписывайте только туда; GATE_LIVE_CONFIRM в .env не храните."
}
& $Python -m gate_stonk validate --config $Config
if ($LASTEXITCODE -ne 0) { throw "Конфигурация не прошла проверку; исправьте указанные поля." }
& $Python -m pytest -q
if ($LASTEXITCODE -ne 0) { throw "Офлайн-тесты не прошли. Не запускайте торговлю." }
Write-Host "Установка и офлайн-тесты завершены. Торговля НЕ запущена."
Write-Host "Дальше: .\run_stonk.ps1 -Command doctor, затем -Command index-watch -Minutes 30, затем paper."
