<#
.SYNOPSIS
    Локальный запуск IPSAS одной командой: создаёт venv, ставит зависимости,
    готовит .env, поднимает dev-сервер и ждёт ответа /health.

.DESCRIPTION
    Скрипт идемпотентен: повторный запуск не переустанавливает зависимости,
    если requirements не менялись. Работает в текущем каталоге репозитория
    и не требует Docker.

.PARAMETER Port
    Порт dev-сервера. По умолчанию 5000 (или значение PORT из .env).

.PARAMETER Install
    Принудительно переустановить зависимости.

.PARAMETER NoRun
    Только подготовить окружение, сервер не запускать.

.PARAMETER SkipChecks
    Не выполнять HTTP-проверку готовности /health.

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\deploy\local-start.ps1

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\deploy\local-start.ps1 -NoRun
#>
[CmdletBinding()]
param(
    [int]$Port = 0,
    [switch]$Install,
    [switch]$NoRun,
    [switch]$SkipChecks
)

$ErrorActionPreference = 'Stop'
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch { }

$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

function Write-Step { param([string]$Text) Write-Host "`n=== $Text ===" -ForegroundColor Cyan }
function Write-Ok { param([string]$Text) Write-Host "  [ok] $Text" -ForegroundColor Green }
function Write-Warn { param([string]$Text) Write-Host "  [!]  $Text" -ForegroundColor Yellow }
function Write-Err { param([string]$Text) Write-Host "  [!!] $Text" -ForegroundColor Red }

Write-Host "IPSAS - локальный запуск" -ForegroundColor White
Write-Host "Каталог: $RepoRoot"

# 1. Интерпретатор Python
Write-Step "1/5 Проверка Python"
$pythonCmd = $null
foreach ($candidate in @('py', 'python')) {
    $found = Get-Command $candidate -ErrorAction SilentlyContinue
    if (-not $found) { continue }
    try {
        $versionText = & $candidate -c "import sys; print('.'.join(map(str, sys.version_info[:3])))" 2>$null
    } catch { continue }
    if ($LASTEXITCODE -ne 0 -or -not $versionText) { continue }
    $parts = $versionText.Trim().Split('.')
    if ([int]$parts[0] -lt 3 -or ([int]$parts[0] -eq 3 -and [int]$parts[1] -lt 10)) {
        Write-Warn "$candidate = $versionText (нужен 3.10+)"
        continue
    }
    $pythonCmd = $candidate
    Write-Ok "$candidate = $versionText"
    break
}
if (-not $pythonCmd) {
    Write-Err "Python 3.10+ не найден. Установите Python и повторите."
    exit 1
}

# 2. Виртуальное окружение
Write-Step "2/5 Виртуальное окружение"
$VenvDir = Join-Path $RepoRoot '.venv'
$VenvPython = Join-Path $VenvDir 'Scripts\python.exe'
if (-not (Test-Path $VenvPython)) {
    Write-Host "  создаю .venv (первый запуск, ~10 c)..."
    & $pythonCmd -m venv $VenvDir
    if ($LASTEXITCODE -ne 0) { Write-Err "Не удалось создать .venv"; exit 1 }
    Write-Ok ".venv создан"
} else {
    Write-Ok ".venv уже существует"
}

# 3. Зависимости
Write-Step "3/5 Зависимости"
$Stamp = Join-Path $VenvDir '.ipsas-deps-stamp'
$Requirements = Join-Path $RepoRoot 'requirements-dev.txt'
$needInstall = $Install -or -not (Test-Path $Stamp)
if (-not $needInstall) {
    $stampTime = (Get-Item $Stamp).LastWriteTimeUtc
    $reqTime = (Get-Item $Requirements).LastWriteTimeUtc
    if ($reqTime -gt $stampTime) { $needInstall = $true }
}
if ($needInstall) {
    & $VenvPython -m pip install --quiet --upgrade pip
    & $VenvPython -m pip install --quiet -r $Requirements
    if ($LASTEXITCODE -ne 0) { Write-Err "Установка зависимостей не удалась"; exit 1 }
    Set-Content -Path $Stamp -Value (Get-Date).ToString('o') -Encoding UTF8
    Write-Ok "зависимости установлены (requirements-dev.txt)"
} else {
    Write-Ok "зависимости актуальны (для переустановки: -Install)"
}

# 4. Конфигурация
Write-Step "4/5 Конфигурация"
$EnvFile = Join-Path $RepoRoot '.env'
if (-not (Test-Path $EnvFile)) {
    Copy-Item (Join-Path $RepoRoot '.env.example') $EnvFile
    Write-Ok ".env создан из .env.example"
} else {
    Write-Ok ".env уже существует"
}
if ($Port -gt 0) {
    $env:PORT = "$Port"
    Write-Ok "порт из аргумента: $Port"
}
$resolvedPort = ''
if ($env:PORT) { $resolvedPort = $env:PORT }
if (-not $resolvedPort -and (Test-Path $EnvFile)) {
    $fromFile = Select-String -Path $EnvFile -Pattern '^\s*PORT\s*=\s*(\S+)' | Select-Object -First 1
    if ($fromFile) { $resolvedPort = $fromFile.Matches[0].Groups[1].Value.Trim('"', "'") }
}
if (-not $resolvedPort) { $resolvedPort = '5000' }

# 5. Запуск
Write-Step "5/5 Запуск"
if ($NoRun) {
    Write-Ok "окружение готово. Запуск: $RepoRoot\deploy\local-start.ps1"
    exit 0
}

Write-Host "  Поднимаю dev-сервер на http://127.0.0.1:$resolvedPort"
Write-Host "  Остановить: Ctrl+C`n" -ForegroundColor DarkGray

$job = Start-Process -FilePath $VenvPython `
    -ArgumentList 'run.py' `
    -WorkingDirectory $RepoRoot `
    -NoNewWindow -PassThru

if (-not $SkipChecks) {
    $deadline = (Get-Date).AddSeconds(45)
    $ready = $false
    while ((Get-Date) -lt $deadline) {
        if ($job.HasExited) {
            Write-Err "Сервер завершился с кодом $($job.ExitCode). Смотрите вывод выше."
            exit 1
        }
        Start-Sleep -Milliseconds 700
        try {
            $response = Invoke-WebRequest -Uri "http://127.0.0.1:$resolvedPort/health" -UseBasicParsing -TimeoutSec 3
            if ($response.StatusCode -eq 200) {
                Write-Ok ('готов: ' + [string]$response.Content)
                $ready = $true
                break
            }
        } catch { }
    }
    if (-not $ready) {
        Write-Warn "Сервер не ответил на /health за 45 c. Откройте адрес вручную."
    }
}

Write-Host ""
Write-Host "  Открыть:      http://127.0.0.1:$resolvedPort" -ForegroundColor Green
Write-Host "  Проверка:     http://127.0.0.1:$resolvedPort/health"
Write-Host "  Остановить:   Ctrl+C`n" -ForegroundColor Green

try { $job.WaitForExit() } catch { }
if ($job.HasExited -and $job.ExitCode -ne 0) { exit $job.ExitCode }
