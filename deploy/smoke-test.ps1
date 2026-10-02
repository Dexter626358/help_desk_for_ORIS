<#
.SYNOPSIS
    Smoke-тест запущенного экземпляра IPSAS (health, dashboard, 12 сервисов).

.DESCRIPTION
    Повторяет deploy/smoke-test.sh для Windows. Используется после локального
    запуска и после развёртывания на сервере.

.PARAMETER BaseUrl
    Адрес проверяемого экземпляра. По умолчанию http://127.0.0.1:5000.

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\deploy\smoke-test.ps1

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\deploy\smoke-test.ps1 -BaseUrl https://ipsas.example.org
#>
[CmdletBinding()]
param(
    [string]$BaseUrl = 'http://127.0.0.1:5000'
)

$ErrorActionPreference = 'Stop'
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch { }

$BaseUrl = $BaseUrl.TrimEnd('/')
$pass = 0
$fail = 0
$failed = New-Object System.Collections.Generic.List[string]

function Test-Route {
    param([string]$Route, [string]$Expect = '200')

    $url = "$BaseUrl$Route"
    $code = '000'
    try {
        $response = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 20 -MaximumRedirection 5
        $code = [string]$response.StatusCode
    } catch {
        if ($_.Exception.Response) { $code = [string]$_.Exception.Response.StatusCode.value__ }
    }
    if ($code -eq $Expect) {
        Write-Host ("  [ok]  {0,-40} {1}" -f $Route, $code) -ForegroundColor Green
        $script:pass++
    } else {
        Write-Host ("  [!!]  {0,-40} {1} (ожидался {2})" -f $Route, $code, $Expect) -ForegroundColor Red
        $script:fail++
        $script:failed.Add($Route)
    }
}

Write-Host "IPSAS smoke-тест: $BaseUrl" -ForegroundColor White
Write-Host ("Время: {0}`n" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'))

Write-Host 'Служебные:' -ForegroundColor Cyan
Test-Route '/health'
Test-Route '/health/ready'

Write-Host "`nИнтерфейс:" -ForegroundColor Cyan
Test-Route '/'
Test-Route '/dashboard'

Write-Host "`nСервисы:" -ForegroundColor Cyan
Test-Route '/services/xml-validation'
Test-Route '/services/xml-report'
Test-Route '/services/references'
Test-Route '/services/pdf-matching'
Test-Route '/services/issue-metadata'
Test-Route '/services/issue-pdf-csv'
Test-Route '/services/journal-site'
Test-Route '/services/eng-metadata'
Test-Route '/services/archive-by-sender'
Test-Route '/services/issue-supp-images'
Test-Route '/services/sandbox-journal-setup'
Test-Route '/services/xml-editor/'

Write-Host "`nСодержимое /health:" -ForegroundColor Cyan
$body = ''
try { $body = (Invoke-WebRequest -Uri "$BaseUrl/health" -UseBasicParsing -TimeoutSec 10).Content } catch { }
Write-Host "  $body"
if ($body -match '"status"') {
    Write-Host '  [ok]  /health вернул status' -ForegroundColor Green
    $pass++
} else {
    Write-Host '  [!!]  /health не вернул status' -ForegroundColor Red
    $fail++
    $failed.Add('/health (body)')
}

Write-Host "`n----------------------------------------" -ForegroundColor White
Write-Host ("Пройдено: {0}   Провалено: {1}" -f $pass, $fail) -ForegroundColor White
if ($fail -ne 0) {
    Write-Host ("Непрошедшие: {0}" -f ($failed -join ', ')) -ForegroundColor Red
    exit 1
}
Write-Host 'Все проверки пройдены.' -ForegroundColor Green
exit 0
