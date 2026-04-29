# Spotify Data Studio Bot

Telegram-бот для аналитики Spotify-базы данных. Он принимает обычные вопросы на русском, превращает их в безопасные MySQL `SELECT`-запросы через OpenRouter, показывает красиво оформленный ответ в Telegram и умеет выгружать результат в `CSV`, показывать `SQL` и строить график.

## Что умеет

- Отвечает на вопросы по Spotify-базе обычным текстом.
- Генерирует только безопасные `SELECT` / `WITH ... SELECT` запросы.
- Строит готовые отчёты по трекам, артистам, жанрам, настроению и аудио-фичам.
- Форматирует ответы как мини-отчёты, а не одной длинной строкой.
- Экспортирует отчёты в `CSV`.
- Показывает SQL последнего запроса.
- Строит графики по последнему результату.
- Ведёт историю последних запросов в чате.
- Даёт красивые логи в терминале.

## Основные команды

```text
/start - краткая справка
/reports - список готовых отчётов
/examples - примеры вопросов
/vibe - музыкальный портрет базы
/surprise - случайная подборка треков
/history - последние запросы в текущем чате
/health - проверка подключения к базе
```

## Готовые отчёты

```text
/top_tracks - самые популярные треки
/top_artists - артисты с наибольшим числом треков
/genre_map - жанры по популярности, энергии и настроению
/dance - самые танцевальные треки
/energy - самые энергичные треки
/mood - распределение треков по настроению
/tempo - самые быстрые треки
/explicit - сравнение explicit и clean треков
/hidden_gems - скрытые находки
/audio_profile - средний аудио-профиль базы
```

## Примеры вопросов

```text
топ 10 самых популярных треков
какие жанры самые энергичные
покажи грустные треки с высокой популярностью
найди быстрые танцевальные треки
какие артисты чаще всего встречаются в базе
```

## Установка

```bash
cd "/Users/argenkulzhanov/Desktop/Новая папка/database_nazik"
python3 -m pip install -r requirements.txt
```

## Настройка .env

Создайте файл `.env` в корне проекта:

```env
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
OPENROUTER_API_KEY=your_openrouter_api_key

DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=Sword123
DB_NAME=spotify_db

OPENROUTER_MODEL=openai/gpt-4.1-mini
LOG_LEVEL=INFO
SHOW_SQL=false
```

## Запуск

```bash
python3 app.py
```

После запуска в терминале должны появиться логи такого вида:

```text
15:22:15 | INFO     | spotify_bot    | Booting Spotify Data Studio bot
15:22:16 | INFO     | spotify_bot    | Polling started
```

## Структура проекта

```text
app.py                         главный Telegram-бот
requirements.txt               зависимости Python
sql/create_db_and_tables.sql   создание базы и таблиц
sql/create_index.sql           индексы
sql/create_views.sql           SQL views
cleaned_dataset/               нормализованные CSV-таблицы
dataset/spotify.csv            исходный датасет
normalization.ipynb            подготовка данных
normalization_tracks.ipynb     нормализация треков
```

## Схема базы

База называется `spotify_db`.

Основные таблицы:

- `artists`
- `albums`
- `genres`
- `tracks`
- `track_artist`
- `audio_features`

Связи:

- `tracks.album_id -> albums.album_id`
- `tracks.genre_id -> genres.genre_id`
- `track_artist.track_id -> tracks.track_id`
- `track_artist.artist_id -> artists.artist_id`
- `audio_features.track_id -> tracks.track_id`

## Как работает AI-часть

1. Пользователь пишет вопрос в Telegram.
2. Бот определяет, это обычный чат или вопрос к базе.
3. Если нужен запрос к базе, OpenRouter генерирует MySQL `SELECT`.
4. Код проверяет SQL на безопасность.
5. Запрос выполняется в MySQL.
6. Бот отправляет понятный ответ на русском.
7. Последний результат можно выгрузить кнопками `CSV`, `SQL`, `Chart`.

## Безопасность SQL

Бот блокирует опасные операции:

```text
INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, CREATE, REPLACE,
GRANT, REVOKE, CALL, SHOW, DESCRIBE, EXPLAIN
```

Также запрещены системные схемы:

```text
information_schema, performance_schema, mysql, sys
```

## Формат ответов

Бот старается отвечать не одной строкой, а аккуратным списком:

```text
Топ треков по популярности:

1. Unholy - артисты: Sam Smith, Kim Petras, популярность: 100
2. Quevedo: Bzrp Music Sessions, Vol. 52 - артисты: Bizarrap, Quevedo, популярность: 99
3. I'm Good (Blue) - артисты: David Guetta, Bebe Rexha, популярность: 98
```

Если AI вдруг вернёт слишком сжатый ответ, код автоматически перестроит его в читаемый нумерованный список.

## Проверка

```bash
python3 -m py_compile app.py
python3 -c "import app; app.setup_logging(); app.logger.info('demo log: service=%s status=%s', 'spotify_bot', 'ok')"
```

## Идея проекта

Это не просто бот с готовыми SQL-командами, а маленькая аналитическая студия в Telegram: можно быстро спросить базу человеческим языком, получить аккуратный ответ, скачать данные и сразу построить график.
