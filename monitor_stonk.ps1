# Периодический вывод сохранённого status.json (локальный снимок, не свежий запрос к бирже).
param([string]$StateDir = "runtime_stonk", [double]$Seconds = 2)
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
if ($Seconds -lt 0.2) { throw "Seconds должно быть не меньше 0.2." }
$Status = Join-Path $StateDir "status.json"
while ($true) {
    Clear-Host
    Write-Host "gate_stonk — сохранённый локальный статус, НЕ свежий запрос к бирже"
    if (Test-Path $Status) {
        try {
            $Text = [System.IO.File]::ReadAllText((Resolve-Path $Status))
            $Age = ((Get-Date).ToUniversalTime() - (Get-Item $Status).LastWriteTimeUtc).TotalSeconds
            Write-Host ("Возраст файла: {0:N1} с" -f $Age)
            Write-Host $Text
        } catch { Write-Host "Файл статуса заменяется; повтор..." }
    } else { Write-Host "Статуса ещё нет. Проверьте app.state_dir." }
    Start-Sleep -Milliseconds ([int]($Seconds * 1000))
}
