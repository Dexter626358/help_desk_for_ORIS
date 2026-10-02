# Развёртывание IPSAS на сервере

Схема: Docker Compose на сервере, наружу отдаёт nginx. Обновление — две команды,
которые вы выполняете сами.

```bash
git pull
docker compose up -d --build
```

**`--build` обязателен.** Код лежит внутри образа, а не в контейнере. Без
`--build` compose увидит, что образ `ipsas:latest` уже собран, и запустит
старый код — без единой ошибки в журнале. Это самая частая причина
«я запушил, но на сервере ничего не изменилось».

Один раз нужно подготовить сервер — шаги ниже.

---

## Что нужно на сервере

| Что | Версия | Зачем |
|---|---|---|
| Docker Engine + плагин compose | 24+ | сам сервис |
| git | любой | получать обновления |
| nginx | любой | отдавать сервис наружу по HTTP |
| Python на хосте | **не нужен** | всё внутри контейнера |

Проверка:

```bash
docker --version && docker compose version
```

Если `docker compose version` не сработало, а `docker-compose --version`
сработало — у вас compose версии 1, всё равно можно, команды те же.

---

## Шаг 1. Разово: nginx

```bash
sudo apt install -y nginx
sudo cp /opt/ipsas/deploy/nginx.example.conf /etc/nginx/sites-available/ipsas.conf
sudo ln -s /etc/nginx/sites-available/ipsas.conf /etc/nginx/sites-enabled/ipsas.conf
sudo rm -f /etc/nginx/sites-enabled/default
```

Конфигурация готова к применению как есть: порт 80, доступ по адресу сервера,
без домена, без пароля. Внутри она проксирует на `127.0.0.1:8000` — туда,
куда compose публикует порт контейнера.

Пока контейнер не запущен, `nginx -t` может ругаться на недоступный апстрим —
это нормально. Проверить синтаксис можно сразу, а перезапускать nginx — после
первого `docker compose up`.

Если порт 80 на этой машине занят другим сайтом, уберите `default_server`
в блоке `server` и поправьте `server_name`.

## Шаг 2. Разово: код и конфигурация

### 2.1. SSH-ключ для GitLab

Серверу нужен свой ключ, чтобы `git pull` работал без пароля.

```bash
ssh-keygen -t ed25519 -C "ipsas-server"
```

На вопросы «файл» и «пароль» просто нажмите Enter — ключ без пароля,
файл создастся в `~/.ssh/id_ed25519`. Откройте его и скопируйте содержимое:

```bash
cat ~/.ssh/id_ed25519.pub
```

Выведется строка вида `ssh-ed25519 AAAAC3Nza... ipsas-server` — её нужно
вставить в GitLab. Куда именно, см. ниже.

### 2.2. Добавить ключ в GitLab

**Вариант А. Deploy key — только для этого проекта (рекомендуется).**
Откройте проект →
**Settings → Repository → Deploy keys → Add new key**.

| Поле | Что вписать |
|---|---|
| Key | содержимое `id_ed25519.pub` |
| Title | `ipsas-server` |
| Read/Write | **Read-only** — этого достаточно для `git pull` |
| **Expiry date** | оставьте пустым, или поставьте дату и продлевайте |

Ключ появится в списке с отпечатком (`fingerprint`) — полезно сверить.

**Вариант Б. Ключ в профиль человека.** **User Settings → SSH Keys** →
добавьте тот же ключ. Он будет работать на всех проектах, к которым у
вас есть доступ, и деплой сломается, когда этот человек уйдёт из GitLab.
Вариант А удобнее.

### 2.3. Проверить, что ключ принят

```bash
ssh -T git@gl-pub.rcsi.science
```

При первом обращении спросит, доверять ли серверу:

```
The authenticity of host 'gl-pub.rcsi.science' can't be established.
ED25519 key fingerprint is SHA256:XXXXXXXXXXXXXXXXXXXX.
Are you sure you want to continue connecting (yes/no)?
```

Отпечаток должен совпасть с тем, что даёт ваш GitLab-администратор
(**GitLab → Help → SSH key fingerprints** в самом GitLab). Если совпал —
введите `yes`. Если не совпал — не вводите `yes` и выясните причину
с администратором: это либо переустановка сервера GitLab, либо попытка
перехвата.

Успех выглядит так:

```
Welcome to GitLab, @<кто-то>!
```

Ошибка — так:

```
git@gl-pub.rcsi.science: Permission denied (publickey).
```

### 2.4. Клонировать

Теперь всё готово:

```bash
sudo mkdir -p /opt/ipsas
sudo chown -R "$USER":"$USER" /opt/ipsas
git clone git@gl-pub.rcsi.science:other-web-applications/help_desk_for_oris.git /opt/ipsas
cd /opt/ipsas
```

Если ключ лежит не в стандартном месте, добавьте в `~/.ssh/config`:

```
Host gl-pub.rcsi.science
    IdentityFile ~/.ssh/id_ipsas
```

### 2.5. Конфигурация приложения

```bash
cp deploy/ipsas.env.production.example .env
openssl rand -hex 32                 # вставьте результат в SECRET_KEY в .env
nano .env                            # заполнить SECRET_KEY и ISSUE_FETCH_ALLOWED_HOSTS
chmod 600 .env
```

В `.env` обязательно два значения, без них приложение не стартует:

| Переменная | Значение |
|---|---|
| `SECRET_KEY` | вывод `openssl rand -hex 32` |
| `ISSUE_FETCH_ALLOWED_HOSTS` | `journals.rcsi.science` |

`.env` в `.gitignore`, поэтому `git pull` его никогда не затронет. Редактировать
в `.env` можно сколько угодно — на сервер это не влияет.

## Шаг 3. Разово: первый запуск

```bash
cd /opt/ipsas
docker compose up -d --build
docker compose logs -f          # Ctrl+C, чтобы выйти; контейнер останется
```

Сборка занимает несколько минут в первый раз (ставится Python-зависимости),
дальше — секунды.

Проверка:

```bash
curl -s http://127.0.0.1:8000/health
# {"service":"ipsas","status":"ok"}

docker compose ps               # должен быть статус healthy
bash deploy/smoke-test.sh http://127.0.0.1:8000
# Пройдено: 17   Провалено: 0
```

И наконец:

```bash
sudo systemctl enable --now nginx
```

Дальше сервис доступен по адресу сервера: `http://<ip-сервера>/`.

---

## Обновление

```bash
cd /opt/ipsas
git pull
docker compose up -d --build
docker compose logs --tail=30
```

Проверить, что всё живо:

```bash
curl -s http://127.0.0.1:8000/health
bash deploy/smoke-test.sh http://127.0.0.1:8000
```

Откат на предыдущий коммит:

```bash
git log --oneline -5           # найти нужный
git checkout <commit>
docker compose up -d --build
```

Вернуться на `main` после отката:

```bash
git checkout main
```

Полностью снести и поставить заново (данные в томах сохранятся):

```bash
docker compose down
docker compose up -d --build
```

---

## Что важно не перепутать

**1. Не заменяйте тома на каталоги с хоста.** В `docker-compose.yml` стоят
именованные тома `ipsas-temp` и `ipsas-data`. Они наследуют владельца из
образа (uid 10001), и приложение может писать. Если примонтировать каталог
хоста, он будет принадлежать root, и загрузка файлов упадёт с
`Permission denied`.

**2. `GUNICORN_BIND` нельзя задавать в `.env`.** Внутри контейнера gunicorn
обязан слушать `0.0.0.0:8000`. Значение `127.0.0.1:8000` подходит для systemd
и сломает Docker: порт не пробросится и nginx отдаст `502 Bad Gateway`.
Поэтому в `docker-compose.yml` оно задано через `environment:`, и это
перекрывает `.env` — проверено.

**3. `SESSION_COOKIE_SECURE=false` — пока нет HTTPS.** В production cookie
помечается `Secure`, и браузер отбрасывает её при обычном `http://`. Из-за
этого ломаются все формы с ошибкой CSRF. Значение уже проставлено и в
`docker-compose.yml`, и в `.env`. Когда появится сертификат — верните
`true` и перезапустите контейнер.

**4. Один воркер.** `GUNICORN_WORKERS=1` задан в compose. Лимит запросов и
очередь задач живут в памяти процесса, со вторым воркером появятся два
независимых лимита.

**5. Данные не переживают `down -v`.** `docker compose down` тома сохраняет,
а `docker compose down -v` удаляет вместе со всеми скачанными файлами.

---

## Если что-то не работает

| Симптом | Что делать |
|---|---|
| `git pull` просит пароль | Нет SSH-ключа на сервере. Сгенерируйте `ssh-keygen -t ed25519` и добавьте публичный ключ в GitLab как deploy key (**Settings → Repository → Deploy keys**). Права **Read** достаточно: ключ нужен только для чтения. Полный порядок — в разделе «Шаг 2» |
| `RuntimeError: SECRET_KEY is required in production` | `.env` создан, но `SECRET_KEY=` пустой |
| `RuntimeError: ISSUE_FETCH_ALLOWED_HOSTS is required` | То же, пустой allowlist |
| `error loading env file .env` | Файла `.env` нет в каталоге с `docker-compose.yml` |
| `502 Bad Gateway` от nginx | Контейнер не слушает. `docker compose ps` и `docker compose logs --tail=50` |
| Контейнер перезапускается по кругу | `docker compose logs --tail=50` — скорее всего, не заполнен `.env` |
| `Permission denied` при загрузке файла | Каталог хоста примонтирован вместо тома (см. пункт 1 выше) |
| Статус `unhealthy` | Проверьте `docker inspect --format '{{json .State.Health}}' ipsas` |
| Страница открывается, формы не отправляются | `SESSION_COOKIE_SECURE=true` при открытом `http://` → поставьте `false` |
| Всё сразу «слишком много запросов» | Не работает `TRUST_PROXY_HEADERS=true` — все пользователи делят один лимит в 30 запросов/мин |
| 404 на `/services/xml-editor` | Это нормально: канонический адрес `/services/xml-editor/` |

Полезные команды:

```bash
docker compose ps                     # состояние контейнера
docker compose logs -f                # журнал
docker compose logs --tail=50         # последние 50 строк
docker compose restart                # перезапуск без пересборки
sudo nginx -t && sudo systemctl reload nginx
tail -f /var/log/nginx/access.log     # кто и откуда ходит
```

---

## Альтернатива: systemd вместо Docker

В репозитории есть `deploy/ipsas.service.example` — запуск без Docker,
Python 3.11 ставится прямо на сервер. Нужен, если Docker на сервере
запрещён. Тогда каталоги `/var/lib/ipsas` и `/var/log/ipsas` создаются
вручную, а `.env` кладётся в `/etc/ipsas/ipsas.env`. Описание — в
`DEPLOYMENT.md`.

---

## Что сервис умеет и что ему нужно для полной работы

Базовое развёртывание поднимает все 12 сервисов, но часть функций работает
«вхолостую», пока не заданы дополнительные настройки. Дописываете их
в `.env`, потом `docker compose up -d`.

| Функция | Что нужно дополнительно |
|---|---|
| Валидатор XML, HTML-отчёт, список литературы, CSV для PDF, редактор XML | Ничего, работает сразу |
| Разбор выпуска по ссылке (OJS/HTML) | Хост в `ISSUE_FETCH_ALLOWED_HOSTS` |
| Проверка настроек сайта журнала | Ничего, работает с загруженным `.data` |
| Обновление ENG-метаданных на Платформе | `PLATFORM_USERNAME` / `PLATFORM_PASSWORD`, хост в allowlist, `PLATFORM_APPLY_ENABLED=1` |
| Архивация рукописей «по отправителю» | Учётные данные Платформы (по умолчанию dry-run) |
| Загрузка рисунков выпуска в доп. файлы | Учётные данные Платформы (по умолчанию dry-run) |
| Настройка журнала в песочнице | `SANDBOX_BASE_URL`, `SANDBOX_GATE_*`, `SANDBOX_OJS_*`, хост песочницы в allowlist |

Запись на Платформу **выключена** по умолчанию: пока
`PLATFORM_APPLY_ENABLED=0`, эти сервисы только показывают, что собираются
сделать, ничего не меняя. Включайте осознанно — ошибка затрагивает живой
выпуск.
