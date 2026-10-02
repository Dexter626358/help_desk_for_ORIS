# Smoke-тест развёрнутого экземпляра IPSAS.
#
# Использование: bash deploy/smoke-test.sh [BASE_URL]
# Вызывайте через `bash`, а не `./`: git на Windows не выставляет бит
# исполнения, и на Linux-сервере файл приходит без него.
# Код возврата: 0 - все проверки пройдены, 1 - есть непрошедшие.

set -u

BASE_URL="${1:-http://127.0.0.1:8000}"
BASE_URL="${BASE_URL%/}"

PASS=0
FAIL=0
FAILED_ROUTES=""

check() {
    local route="$1"
    local expect="$2"
    local url="${BASE_URL}${route}"
    local code
    code=$(curl -s -L -o /dev/null -w '%{http_code}' --max-time 20 "$url" 2>/dev/null || echo "000")
    if [ "$code" = "$expect" ]; then
        printf '  \033[32m[ok]\033[0m  %-40s %s\n' "$route" "$code"
        PASS=$((PASS + 1))
    else
        printf '  \033[31m[!!]\033[0m  %-40s %s (ожидался %s)\n' "$route" "$code" "$expect"
        FAIL=$((FAIL + 1))
        FAILED_ROUTES="${FAILED_ROUTES} ${route}"
    fi
}

echo "IPSAS smoke-тест: ${BASE_URL}"
echo "Время: $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo

echo "Служебные:"
check "/health" 200
check "/health/ready" 200

echo
echo "Интерфейс:"
check "/dashboard" 200
check "/" 200

echo
echo "Сервисы:"
check "/services/xml-validation" 200
check "/services/xml-report" 200
check "/services/references" 200
check "/services/pdf-matching" 200
check "/services/issue-metadata" 200
check "/services/issue-pdf-csv" 200
check "/services/journal-site" 200
check "/services/eng-metadata" 200
check "/services/archive-by-sender" 200
check "/services/issue-supp-images" 200
check "/services/sandbox-journal-setup" 200
check "/services/xml-editor/" 200

echo
echo "Содержимое /health:"
HEALTH_BODY=$(curl -s --max-time 10 "${BASE_URL}/health" 2>/dev/null || echo "")
echo "  ${HEALTH_BODY}"
if echo "$HEALTH_BODY" | grep -q '"status"'; then
    printf '  \033[32m[ok]\033[0m  /health вернул status\n'
    PASS=$((PASS + 1))
else
    printf '  \033[31m[!!]\033[0m  /health не вернул status\n'
    FAIL=$((FAIL + 1))
fi

echo
echo "----------------------------------------"
echo "Пройдено: ${PASS}   Провалено: ${FAIL}"
if [ "$FAIL" -ne 0 ]; then
    echo "Непрошедшие маршруты:${FAILED_ROUTES}"
    echo
    echo "Что смотреть:"
    echo "  systemctl status ipsas / journalctl -u ipsas -n 100 --no-pager"
    echo "  ss -ltnp | grep 8000     (слушает ли gunicorn)"
    echo "  nginx -t                 (если прокси настроен)"
    exit 1
fi
echo "Все проверки пройдены."
exit 0
