import csv
import html
import json
import logging
import os
import re
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

os.environ.setdefault("MPLCONFIGDIR", os.path.join(tempfile.gettempdir(), "spotify_bot_matplotlib"))
os.environ.setdefault("XDG_CACHE_HOME", os.path.join(tempfile.gettempdir(), "spotify_bot_cache"))

import matplotlib
import mysql.connector
import pandas as pd
import requests
from mysql.connector import Error
from requests import RequestException

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, InputFile, Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)


OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_MODEL = "openai/gpt-4.1-mini"
MAX_RESULT_ROWS = 20
MAX_PREVIEW_ROWS = 10
TABLE_COLUMN_WIDTH = 18
LAST_RESULT_KEY = "last_result"
QUERY_HISTORY_KEY = "query_history"
CSV_CALLBACK_DATA = "last_result_csv"
SQL_CALLBACK_DATA = "last_result_sql"
VISUALIZATION_CALLBACK_DATA = "last_result_visualization"
TELEGRAM_READ_TIMEOUT = 120
TELEGRAM_WRITE_TIMEOUT = 120
TELEGRAM_CONNECT_TIMEOUT = 30
TELEGRAM_POOL_TIMEOUT = 30
OPENROUTER_TIMEOUT = 90
OPENROUTER_RETRIES = 3
HISTORY_LIMIT = 5


class PrettyLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        created_at = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        level = record.levelname.ljust(8)
        return f"{created_at} | {level} | {record.name:<14} | {record.getMessage()}"


def setup_logging() -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, level_name, logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(PrettyLogFormatter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(log_level)

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.INFO)


logger = logging.getLogger("spotify_bot")


SCHEMA_DESCRIPTION = """
Database: spotify_db

Tables:
- artists(artist_id, artist_name)
- albums(album_id, album_name)
- genres(genre_id, genre_name)
- tracks(track_id, track_name, album_id, genre_id, popularity, duration_ms, is_explicit)
- track_artist(id, track_id, artist_id)
- audio_features(feature_id, track_id, danceability, energy, `key`, loudness, mode,
  speechiness, acousticness, instrumentalness, liveness, valence, tempo, time_signature)

Relationships:
- tracks.album_id -> albums.album_id
- tracks.genre_id -> genres.genre_id
- track_artist.track_id -> tracks.track_id
- track_artist.artist_id -> artists.artist_id
- audio_features.track_id -> tracks.track_id
""".strip()


SQL_SYSTEM_PROMPT = f"""
You convert user questions into safe MySQL SELECT queries for a Spotify analytics bot.

Rules:
- Return only SQL, with no markdown and no explanation.
- Output exactly one query.
- Allowed statements: SELECT or WITH ... SELECT only.
- Never use INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, CREATE, REPLACE, GRANT, REVOKE.
- Never query information_schema, mysql, performance_schema, or sys.
- Use only the tables and columns from the schema below.
- Add LIMIT {MAX_RESULT_ROWS} unless the question clearly asks for a single aggregate value
  such as COUNT, SUM, AVG, MIN, or MAX.
- Use MySQL syntax.
- When using audio_features.key, wrap it in backticks: audio_features.`key`.
- For "dance", "танцевальный", "под танцы" use audio_features.danceability.
- For "energy", "энергичный" use audio_features.energy.
- For "mood", "настроение", "happy/sad" use audio_features.valence.
- For "fast", "быстрый" use audio_features.tempo.
- For "long", "длинный" use tracks.duration_ms.
- For explicit tracks use tracks.is_explicit.

Schema:
{SCHEMA_DESCRIPTION}
""".strip()


ANSWER_SYSTEM_PROMPT = """
Ты аналитик Spotify-данных и отвечаешь в Telegram.

Правила:
- Отвечай только на русском.
- Не показывай SQL, если пользователь явно не попросил SQL.
- Не выдавай сырые column=value дампы.
- Пиши кратко, естественно и полезно, но не сжимай несколько строк результата в одну длинную строку.
- Если результат один, ответь одной понятной фразой.
- Если строк несколько, оформи ответ как мини-отчёт:
  1) первая строка - короткий заголовок или вывод;
  2) затем пустая строка;
  3) затем нумерованный список, каждый объект с новой строки.
- Для треков используй формат: "1. Название — артист(ы), популярность: N".
- Для жанров/артистов/категорий используй формат: "1. Название — показатель: значение".
- Не перечисляй больше 10 строк в текстовом ответе, даже если данных больше.
- Не используй длинные предложения через запятую для списков.
- Если данных нет, честно скажи, что подходящих данных в базе не найдено.
- Опирайся только на вопрос, SQL-результат и строки данных.
""".strip()


ROUTER_SYSTEM_PROMPT = """
Classify the user's message.

Return exactly one word:
- DATABASE: if the message asks for Spotify data, analytics, reports, metrics, retrieval,
  filtering, aggregation, rankings, comparisons, tracks, artists, albums, genres, audio features.
- CHAT: if the message is casual conversation, greeting, thanks, help request without asking for data,
  or general dialogue that should not query the database.
""".strip()


CHAT_SYSTEM_PROMPT = """
Ты дружелюбный Telegram-помощник для Spotify-базы.

Правила:
- Отвечай только на русском.
- Будь кратким и естественным.
- Не упоминай SQL, если пользователь сам не спрашивает.
- Если уместно, мягко напомни, что умеешь искать треки, артистов, жанры,
  анализировать популярность, настроение, энергию, темп и строить отчёты через /reports.
""".strip()


VISUALIZATION_SYSTEM_PROMPT = """
You are choosing a chart configuration for tabular query results.

Return JSON only with this schema:
{
  "chart_type": "line" | "bar" | "horizontal_bar" | "pie",
  "x": "column_name or null",
  "y": ["numeric_column_1", "numeric_column_2"],
  "title": "short chart title in Russian"
}

Rules:
- Use only provided column names.
- Prefer bar or horizontal_bar for rankings and category comparisons.
- Prefer pie only for one category column and one numeric column with a small number of rows.
- Include 1 or 2 numeric columns in "y".
- If the result has one row with many numeric columns, set "x" to null and choose "bar".
- Keep the title short and useful.
""".strip()


SIMPLE_CHAT_RESPONSES = {
    "привет": "Привет! Могу найти треки, артистов, жанры и собрать отчёты. Список отчётов: /reports.",
    "здравствуйте": "Здравствуйте! Могу помочь с аналитикой Spotify-базы. Список отчётов: /reports.",
    "салам": "Салам! Могу искать по Spotify-базе и строить отчёты. Попробуйте /reports.",
    "hello": "Привет! Могу помочь с аналитикой Spotify-базы. Список отчётов: /reports.",
    "hi": "Привет! Могу помочь с аналитикой Spotify-базы. Список отчётов: /reports.",
    "спасибо": "Пожалуйста! Можете задать вопрос по трекам, артистам, жанрам или открыть /reports.",
    "ок": "Хорошо. Когда понадобится аналитика по Spotify-базе, просто напишите вопрос.",
    "понял": "Отлично. Могу дальше помочь с треками, артистами, жанрами и отчётами.",
    "пока": "До связи! Возвращайтесь с любыми вопросами по Spotify-базе.",
}


@dataclass(frozen=True)
class ReportDefinition:
    command: str
    title: str
    description: str
    filename_prefix: str
    sql: str


REPORTS: dict[str, ReportDefinition] = {
    "top_tracks": ReportDefinition(
        command="top_tracks",
        title="Топ треков",
        description="Самые популярные треки с артистами, альбомами и жанрами.",
        filename_prefix="top_tracks",
        sql="""
            SELECT
                t.track_name,
                GROUP_CONCAT(DISTINCT a.artist_name ORDER BY a.artist_name SEPARATOR ', ') AS artists,
                al.album_name,
                g.genre_name,
                t.popularity
            FROM tracks t
            LEFT JOIN track_artist ta ON ta.track_id = t.track_id
            LEFT JOIN artists a ON a.artist_id = ta.artist_id
            LEFT JOIN albums al ON al.album_id = t.album_id
            LEFT JOIN genres g ON g.genre_id = t.genre_id
            GROUP BY t.track_id, t.track_name, al.album_name, g.genre_name, t.popularity
            ORDER BY t.popularity DESC, t.track_name
            LIMIT 20
        """.strip(),
    ),
    "top_artists": ReportDefinition(
        command="top_artists",
        title="Топ артистов",
        description="Артисты с наибольшим числом треков и средней популярностью.",
        filename_prefix="top_artists",
        sql="""
            SELECT
                a.artist_name,
                COUNT(DISTINCT t.track_id) AS tracks_count,
                ROUND(AVG(t.popularity), 2) AS avg_popularity,
                MAX(t.popularity) AS best_track_popularity
            FROM artists a
            JOIN track_artist ta ON ta.artist_id = a.artist_id
            JOIN tracks t ON t.track_id = ta.track_id
            GROUP BY a.artist_id, a.artist_name
            ORDER BY tracks_count DESC, avg_popularity DESC
            LIMIT 20
        """.strip(),
    ),
    "genre_map": ReportDefinition(
        command="genre_map",
        title="Жанры",
        description="Жанры по количеству треков, средней популярности, энергии и настроению.",
        filename_prefix="genre_map",
        sql="""
            SELECT
                g.genre_name,
                COUNT(DISTINCT t.track_id) AS tracks_count,
                ROUND(AVG(t.popularity), 2) AS avg_popularity,
                ROUND(AVG(af.energy), 3) AS avg_energy,
                ROUND(AVG(af.valence), 3) AS avg_valence
            FROM genres g
            JOIN tracks t ON t.genre_id = g.genre_id
            LEFT JOIN audio_features af ON af.track_id = t.track_id
            GROUP BY g.genre_id, g.genre_name
            ORDER BY tracks_count DESC, avg_popularity DESC
            LIMIT 20
        """.strip(),
    ),
    "dance": ReportDefinition(
        command="dance",
        title="Танцевальные треки",
        description="Треки с самым высоким danceability.",
        filename_prefix="dance_tracks",
        sql="""
            SELECT
                t.track_name,
                GROUP_CONCAT(DISTINCT a.artist_name ORDER BY a.artist_name SEPARATOR ', ') AS artists,
                ROUND(af.danceability, 3) AS danceability,
                t.popularity
            FROM tracks t
            JOIN audio_features af ON af.track_id = t.track_id
            LEFT JOIN track_artist ta ON ta.track_id = t.track_id
            LEFT JOIN artists a ON a.artist_id = ta.artist_id
            GROUP BY t.track_id, t.track_name, af.danceability, t.popularity
            ORDER BY af.danceability DESC, t.popularity DESC
            LIMIT 20
        """.strip(),
    ),
    "energy": ReportDefinition(
        command="energy",
        title="Энергичные треки",
        description="Треки с самой высокой energy.",
        filename_prefix="energy_tracks",
        sql="""
            SELECT
                t.track_name,
                GROUP_CONCAT(DISTINCT a.artist_name ORDER BY a.artist_name SEPARATOR ', ') AS artists,
                ROUND(af.energy, 3) AS energy,
                ROUND(af.tempo, 1) AS tempo,
                t.popularity
            FROM tracks t
            JOIN audio_features af ON af.track_id = t.track_id
            LEFT JOIN track_artist ta ON ta.track_id = t.track_id
            LEFT JOIN artists a ON a.artist_id = ta.artist_id
            GROUP BY t.track_id, t.track_name, af.energy, af.tempo, t.popularity
            ORDER BY af.energy DESC, t.popularity DESC
            LIMIT 20
        """.strip(),
    ),
    "mood": ReportDefinition(
        command="mood",
        title="Настроение треков",
        description="Распределение треков по настроению на основе valence.",
        filename_prefix="mood",
        sql="""
            SELECT
                CASE
                    WHEN af.valence >= 0.7 THEN 'Happy'
                    WHEN af.valence <= 0.3 THEN 'Sad'
                    ELSE 'Neutral'
                END AS mood,
                COUNT(*) AS tracks_count,
                ROUND(AVG(af.valence), 3) AS avg_valence,
                ROUND(AVG(t.popularity), 2) AS avg_popularity
            FROM audio_features af
            JOIN tracks t ON t.track_id = af.track_id
            GROUP BY mood
            ORDER BY tracks_count DESC
        """.strip(),
    ),
    "tempo": ReportDefinition(
        command="tempo",
        title="Темп треков",
        description="Самые быстрые треки по BPM.",
        filename_prefix="tempo",
        sql="""
            SELECT
                t.track_name,
                GROUP_CONCAT(DISTINCT a.artist_name ORDER BY a.artist_name SEPARATOR ', ') AS artists,
                ROUND(af.tempo, 1) AS tempo,
                t.popularity
            FROM tracks t
            JOIN audio_features af ON af.track_id = t.track_id
            LEFT JOIN track_artist ta ON ta.track_id = t.track_id
            LEFT JOIN artists a ON a.artist_id = ta.artist_id
            GROUP BY t.track_id, t.track_name, af.tempo, t.popularity
            ORDER BY af.tempo DESC
            LIMIT 20
        """.strip(),
    ),
    "explicit": ReportDefinition(
        command="explicit",
        title="Explicit-контент",
        description="Сравнение explicit и non-explicit треков.",
        filename_prefix="explicit",
        sql="""
            SELECT
                CASE WHEN is_explicit = 1 THEN 'Explicit' ELSE 'Clean' END AS content_type,
                COUNT(*) AS tracks_count,
                ROUND(AVG(popularity), 2) AS avg_popularity,
                ROUND(AVG(duration_ms) / 1000, 1) AS avg_duration_seconds
            FROM tracks
            GROUP BY content_type
            ORDER BY tracks_count DESC
        """.strip(),
    ),
    "hidden_gems": ReportDefinition(
        command="hidden_gems",
        title="Скрытые находки",
        description="Треки с сильными аудио-характеристиками, но не самой высокой популярностью.",
        filename_prefix="hidden_gems",
        sql="""
            SELECT
                t.track_name,
                GROUP_CONCAT(DISTINCT a.artist_name ORDER BY a.artist_name SEPARATOR ', ') AS artists,
                g.genre_name,
                t.popularity,
                ROUND((af.danceability + af.energy + af.valence) / 3, 3) AS vibe_score,
                ROUND(af.tempo, 1) AS tempo
            FROM tracks t
            JOIN audio_features af ON af.track_id = t.track_id
            LEFT JOIN genres g ON g.genre_id = t.genre_id
            LEFT JOIN track_artist ta ON ta.track_id = t.track_id
            LEFT JOIN artists a ON a.artist_id = ta.artist_id
            WHERE t.popularity BETWEEN 35 AND 70
            GROUP BY t.track_id, t.track_name, g.genre_name, t.popularity, af.danceability,
                     af.energy, af.valence, af.tempo
            ORDER BY vibe_score DESC, t.popularity DESC
            LIMIT 20
        """.strip(),
    ),
    "audio_profile": ReportDefinition(
        command="audio_profile",
        title="Аудио-профиль базы",
        description="Средние значения danceability, energy, valence, tempo и acousticness.",
        filename_prefix="audio_profile",
        sql="""
            SELECT
                ROUND(AVG(af.danceability), 3) AS avg_danceability,
                ROUND(AVG(af.energy), 3) AS avg_energy,
                ROUND(AVG(af.valence), 3) AS avg_valence,
                ROUND(AVG(af.tempo), 1) AS avg_tempo,
                ROUND(AVG(af.acousticness), 3) AS avg_acousticness,
                ROUND(AVG(t.popularity), 2) AS avg_popularity
            FROM audio_features af
            JOIN tracks t ON t.track_id = af.track_id
        """.strip(),
    ),
}


def load_env_file(path: str = ".env") -> None:
    if not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Environment variable {name} is required")
    return value


def get_bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def get_db_connection():
    logger.debug("Opening MySQL connection to %s:%s/%s", os.getenv("DB_HOST", "localhost"), os.getenv("DB_PORT", "3306"), os.getenv("DB_NAME", "spotify_db"))
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", "Sword123"),
        database=os.getenv("DB_NAME", "spotify_db"),
    )


def call_openrouter(messages: list[dict[str, str]], max_tokens: int | None = None) -> str:
    api_key = require_env("OPENROUTER_API_KEY")
    model = os.getenv("OPENROUTER_MODEL", DEFAULT_MODEL)
    default_max_tokens = max(16, int(os.getenv("OPENROUTER_MAX_TOKENS", "240")))

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": os.getenv("APP_URL", "https://localhost"),
        "X-Title": os.getenv("APP_NAME", "spotify-telegram-ai"),
    }
    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0,
        "max_tokens": max(16, max_tokens or default_max_tokens),
    }

    last_error: Exception | None = None
    for attempt in range(OPENROUTER_RETRIES):
        try:
            logger.debug("OpenRouter request: model=%s attempt=%s", model, attempt + 1)
            response = requests.post(
                OPENROUTER_URL,
                headers=headers,
                json=payload,
                timeout=OPENROUTER_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()
            try:
                content = data["choices"][0]["message"]["content"].strip()
                logger.debug("OpenRouter response received: chars=%s", len(content))
                return content
            except (KeyError, IndexError) as exc:
                raise RuntimeError(
                    f"Unexpected OpenRouter response: {json.dumps(data, ensure_ascii=True)}"
                ) from exc
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code is not None and 400 <= status_code < 500 and status_code not in {408, 429}:
                raise
            last_error = exc
        except RequestException as exc:
            last_error = exc

        if attempt < OPENROUTER_RETRIES - 1:
            logger.warning("OpenRouter retry scheduled: attempt=%s error=%s", attempt + 1, last_error)
            time.sleep(1.5 * (attempt + 1))

    if last_error is not None:
        raise last_error
    raise RuntimeError("OpenRouter request failed")


def extract_sql(raw_text: str) -> str:
    cleaned = raw_text.strip()
    cleaned = re.sub(r"^```(?:sql)?\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    cleaned = cleaned.strip()

    match = re.search(r"(?is)\b(with|select)\b.*", cleaned)
    if not match:
        raise ValueError("Модель не вернула SQL.")

    sql = match.group(0).strip()
    sql = sql.split(";")[0].strip()
    return sql


def is_safe_query(query: str) -> bool:
    normalized = query.strip().lower()
    if not normalized:
        return False
    if ";" in normalized:
        return False
    if "--" in normalized or "/*" in normalized or "*/" in normalized:
        return False
    if not (normalized.startswith("select") or normalized.startswith("with")):
        return False

    forbidden = {
        "insert",
        "update",
        "delete",
        "drop",
        "alter",
        "truncate",
        "create",
        "replace",
        "grant",
        "revoke",
        "call",
        "show",
        "describe",
        "explain",
    }
    tokens = set(re.findall(r"[a-z_]+", normalized))
    if forbidden & tokens:
        return False

    blocked_schemas = {"information_schema", "performance_schema", "mysql", "sys"}
    if blocked_schemas & tokens:
        return False

    return True


def generate_sql(question: str) -> str:
    raw_sql = call_openrouter(
        [
            {"role": "system", "content": SQL_SYSTEM_PROMPT},
            {"role": "user", "content": question},
        ]
    )
    sql = extract_sql(raw_sql)
    if not is_safe_query(sql):
        logger.warning("Unsafe SQL rejected: %s", sql)
        raise ValueError("Модель сгенерировала небезопасный SQL.")
    logger.info("SQL generated safely: %s", " ".join(sql.split())[:220])
    return sql


def is_probably_database_question(question: str) -> bool:
    lowered = question.strip().lower()
    database_keywords = [
        "сколько",
        "какие",
        "какой",
        "какая",
        "топ",
        "трек",
        "песня",
        "артист",
        "исполнитель",
        "альбом",
        "жанр",
        "spotify",
        "популяр",
        "танц",
        "dance",
        "energy",
        "энерг",
        "mood",
        "настро",
        "valence",
        "tempo",
        "темп",
        "быстр",
        "длин",
        "explicit",
        "отчет",
        "отчёт",
        "баз",
        "данн",
        "средн",
        "сравн",
    ]
    return any(keyword in lowered for keyword in database_keywords)


def route_message(question: str) -> str:
    lowered = question.strip().lower()
    if lowered in SIMPLE_CHAT_RESPONSES:
        return "CHAT"

    try:
        raw_route = call_openrouter(
            [
                {"role": "system", "content": ROUTER_SYSTEM_PROMPT},
                {"role": "user", "content": question},
            ],
            max_tokens=16,
        )
        route = raw_route.strip().upper()
        if "DATABASE" in route:
            return "DATABASE"
        if "CHAT" in route:
            return "CHAT"
    except Exception:
        logger.debug("Router fallback used", exc_info=True)
        pass

    return "DATABASE" if is_probably_database_question(question) else "CHAT"


def build_chat_answer(message_text: str) -> str:
    lowered = message_text.strip().lower()
    if lowered in SIMPLE_CHAT_RESPONSES:
        return SIMPLE_CHAT_RESPONSES[lowered]

    try:
        return call_openrouter(
            [
                {"role": "system", "content": CHAT_SYSTEM_PROMPT},
                {"role": "user", "content": message_text},
            ],
            max_tokens=140,
        )
    except Exception:
        return "Могу помочь с Spotify-базой: треки, артисты, жанры, популярность, настроение и отчёты через /reports."


def run_sql(query: str, limit: int | None = MAX_RESULT_ROWS) -> list[dict[str, Any]]:
    connection = None
    cursor = None

    try:
        started_at = time.perf_counter()
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True, buffered=True)
        cursor.execute(query)
        if limit is None:
            rows = cursor.fetchall()
        else:
            rows = cursor.fetchmany(limit)
        elapsed_ms = (time.perf_counter() - started_at) * 1000
        logger.info("SQL executed: rows=%s elapsed=%.1fms", len(rows), elapsed_ms)
        return rows
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None and connection.is_connected():
            connection.close()


def build_human_answer(question: str, sql: str, rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "По вашему запросу в Spotify-базе ничего не найдено."

    if len(rows) == 1 and len(rows[0]) == 1:
        key, value = next(iter(rows[0].items()))
        prompt = {
            "question": question,
            "sql": sql,
            "result_type": "single_value",
            "column": key,
            "value": value,
        }
    else:
        prompt = {
            "question": question,
            "sql": sql,
            "result_type": "table_rows",
            "rows": rows,
        }

    answer = call_openrouter(
        [
            {"role": "system", "content": ANSWER_SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(prompt, ensure_ascii=False, default=str)},
        ],
        max_tokens=int(os.getenv("OPENROUTER_ANSWER_MAX_TOKENS", "420")),
    )
    if len(rows) > 1 and answer.count("\n") < min(4, len(rows)):
        logger.info("AI answer was too compact, using local pretty formatter")
        return build_pretty_rows_answer(question, rows)
    return answer


def humanize_column_name(column: str) -> str:
    labels = {
        "track_name": "трек",
        "artist_name": "артист",
        "artists": "артисты",
        "album_name": "альбом",
        "genre_name": "жанр",
        "popularity": "популярность",
        "avg_popularity": "средняя популярность",
        "tracks_count": "треков",
        "danceability": "danceability",
        "energy": "energy",
        "valence": "valence",
        "tempo": "tempo",
        "mood": "настроение",
        "vibe_score": "vibe score",
    }
    return labels.get(column, column.replace("_", " "))


def format_row_as_sentence(row: dict[str, Any]) -> str:
    title_key = next(
        (
            key
            for key in [
                "track_name",
                "artist_name",
                "artists",
                "genre_name",
                "album_name",
                "mood",
                "content_type",
            ]
            if key in row and row.get(key) not in {None, ""}
        ),
        None,
    )

    if title_key is None:
        title_key = next(iter(row.keys()))

    title = str(row.get(title_key, ""))
    details = []
    for key, value in row.items():
        if key == title_key or value is None or value == "":
            continue
        details.append(f"{humanize_column_name(key)}: {value}")

    if details:
        return f"{title} — " + ", ".join(details[:4])
    return title


def build_pretty_rows_answer(question: str, rows: list[dict[str, Any]], max_rows: int = 10) -> str:
    lines = ["Результат по вашему запросу:"]
    lines.append("")
    for index, row in enumerate(rows[:max_rows], start=1):
        lines.append(f"{index}. {format_row_as_sentence(row)}")

    if len(rows) > max_rows:
        lines.append("")
        lines.append(f"Показал {max_rows} из {len(rows)} строк. Полную таблицу можно скачать через CSV.")

    return "\n".join(lines)


def truncate_text(value: Any, width: int = TABLE_COLUMN_WIDTH) -> str:
    text = "" if value is None else str(value)
    if len(text) <= width:
        return text
    return text[: width - 3] + "..."


def build_text_table(rows: list[dict[str, Any]], max_rows: int = MAX_PREVIEW_ROWS) -> str:
    if not rows:
        return "Нет данных."

    preview_rows = rows[:max_rows]
    headers = list(preview_rows[0].keys())
    widths: dict[str, int] = {}

    for header in headers:
        widths[header] = min(TABLE_COLUMN_WIDTH, max(len(header), 10))

    for row in preview_rows:
        for header in headers:
            widths[header] = min(
                TABLE_COLUMN_WIDTH,
                max(widths[header], len(truncate_text(row.get(header), TABLE_COLUMN_WIDTH))),
            )

    header_line = " | ".join(header.ljust(widths[header]) for header in headers)
    separator_line = "-+-".join("-" * widths[header] for header in headers)
    body_lines = []

    for row in preview_rows:
        body_lines.append(
            " | ".join(
                truncate_text(row.get(header), widths[header]).ljust(widths[header])
                for header in headers
            )
        )

    if len(rows) > max_rows:
        body_lines.append(f"... показано {max_rows} из {len(rows)} строк")

    return "\n".join([header_line, separator_line, *body_lines])


def create_csv_report(rows: list[dict[str, Any]], filename_prefix: str) -> tuple[str, str]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{filename_prefix}_{timestamp}.csv"

    with tempfile.NamedTemporaryFile(
        mode="w",
        newline="",
        suffix=".csv",
        delete=False,
        encoding="utf-8-sig",
    ) as temp_file:
        if rows:
            writer = csv.DictWriter(temp_file, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        else:
            temp_file.write("result\nНет данных\n")
        temp_path = temp_file.name

    return temp_path, filename


def extract_json_object(raw_text: str) -> dict[str, Any]:
    match = re.search(r"\{.*\}", raw_text, flags=re.DOTALL)
    if not match:
        raise ValueError("Модель не вернула JSON.")
    return json.loads(match.group(0))


def build_result_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("CSV", callback_data=CSV_CALLBACK_DATA),
                InlineKeyboardButton("SQL", callback_data=SQL_CALLBACK_DATA),
                InlineKeyboardButton("Chart", callback_data=VISUALIZATION_CALLBACK_DATA),
            ]
        ]
    )


def store_last_result(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    title: str,
    question: str,
    sql: str,
    rows: list[dict[str, Any]],
    filename_prefix: str,
) -> None:
    context.chat_data[LAST_RESULT_KEY] = {
        "title": title,
        "question": question,
        "sql": sql,
        "rows": rows,
        "filename_prefix": filename_prefix,
    }


def get_last_result(context: ContextTypes.DEFAULT_TYPE) -> dict[str, Any] | None:
    return context.chat_data.get(LAST_RESULT_KEY)


def get_user_label(update: Update) -> str:
    user = update.effective_user
    if user is None:
        return "unknown-user"
    name = user.username or user.full_name or str(user.id)
    return f"{name}#{user.id}"


def remember_question(context: ContextTypes.DEFAULT_TYPE, question: str, sql: str, row_count: int) -> None:
    history = context.chat_data.setdefault(QUERY_HISTORY_KEY, [])
    history.append(
        {
            "time": datetime.now().strftime("%H:%M"),
            "question": question,
            "sql": sql,
            "row_count": row_count,
        }
    )
    del history[:-HISTORY_LIMIT]


def build_history_text(context: ContextTypes.DEFAULT_TYPE) -> str:
    history = context.chat_data.get(QUERY_HISTORY_KEY, [])
    if not history:
        return "История пока пустая. Задайте вопрос по Spotify-базе, и я запомню последние запросы."

    lines = ["Последние запросы:"]
    for index, item in enumerate(reversed(history), start=1):
        question = truncate_text(item["question"], 55)
        lines.append(f"{index}. {item['time']} - {question} ({item['row_count']} строк)")
    return "\n".join(lines)


def build_examples_text() -> str:
    return (
        "Примеры вопросов:\n"
        "- топ 10 самых популярных треков\n"
        "- какие жанры самые энергичные\n"
        "- покажи треки с грустным настроением и высокой популярностью\n"
        "- какие артисты чаще всего встречаются в базе\n"
        "- найди быстрые танцевальные треки\n\n"
        "Фишки:\n"
        "/vibe - быстрый музыкальный портрет базы\n"
        "/surprise - неожиданная подборка треков\n"
        "/history - последние запросы в этом чате\n"
        "/health - проверка подключения к базе"
    )


def build_vibe_summary(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "Не получилось собрать музыкальный портрет: данных нет."

    row = rows[0]
    dance = float(row.get("avg_danceability") or 0)
    energy = float(row.get("avg_energy") or 0)
    valence = float(row.get("avg_valence") or 0)
    tempo = float(row.get("avg_tempo") or 0)
    popularity = float(row.get("avg_popularity") or 0)

    if energy >= 0.65 and dance >= 0.6:
        mood = "База звучит бодро и танцевально."
    elif valence <= 0.4:
        mood = "В базе заметен более спокойный и меланхоличный вайб."
    elif dance >= 0.6:
        mood = "В базе много треков, которые хорошо ложатся в плейлист для движения."
    else:
        mood = "База довольно сбалансированная: без явного перекоса в одну сторону."

    return (
        f"{mood}\n\n"
        f"Danceability: {dance:.3f}\n"
        f"Energy: {energy:.3f}\n"
        f"Valence: {valence:.3f}\n"
        f"Tempo: {tempo:.1f} BPM\n"
        f"Средняя популярность: {popularity:.2f}"
    )


def prepare_dataframe(rows: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    for column in df.columns:
        numeric_series = pd.to_numeric(df[column], errors="coerce")
        if numeric_series.notna().all():
            df[column] = numeric_series
            continue

        datetime_series = pd.to_datetime(df[column], errors="coerce")
        if datetime_series.notna().all():
            df[column] = datetime_series

    return df


def get_numeric_columns(df: pd.DataFrame) -> list[str]:
    return [column for column in df.columns if pd.api.types.is_numeric_dtype(df[column])]


def get_datetime_columns(df: pd.DataFrame) -> list[str]:
    return [column for column in df.columns if pd.api.types.is_datetime64_any_dtype(df[column])]


def get_categorical_columns(df: pd.DataFrame) -> list[str]:
    return [
        column
        for column in df.columns
        if not pd.api.types.is_numeric_dtype(df[column])
        and not pd.api.types.is_datetime64_any_dtype(df[column])
    ]


def fallback_visualization_spec(question: str, title: str, df: pd.DataFrame) -> dict[str, Any]:
    numeric_columns = get_numeric_columns(df)
    datetime_columns = get_datetime_columns(df)
    categorical_columns = get_categorical_columns(df)

    if not numeric_columns:
        raise ValueError("Для визуализации нужны числовые данные.")

    if len(df) == 1:
        return {
            "chart_type": "bar",
            "x": None,
            "y": numeric_columns[: min(4, len(numeric_columns))],
            "title": title,
        }

    if datetime_columns:
        return {
            "chart_type": "line",
            "x": datetime_columns[0],
            "y": numeric_columns[: min(2, len(numeric_columns))],
            "title": title,
        }

    if categorical_columns:
        chart_type = "horizontal_bar" if len(df) > 8 else "bar"
        return {
            "chart_type": chart_type,
            "x": categorical_columns[0],
            "y": [numeric_columns[0]],
            "title": title,
        }

    return {
        "chart_type": "bar",
        "x": None,
        "y": numeric_columns[: min(4, len(numeric_columns))],
        "title": title or question,
    }


def choose_visualization_spec(question: str, title: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    df = prepare_dataframe(rows)
    fallback = fallback_visualization_spec(question, title, df)

    payload = {
        "question": question,
        "title": title,
        "columns": list(df.columns),
        "dtypes": {column: str(dtype) for column, dtype in df.dtypes.items()},
        "sample_rows": df.head(8).astype(str).to_dict(orient="records"),
        "row_count": len(df),
    }

    try:
        raw_spec = call_openrouter(
            [
                {"role": "system", "content": VISUALIZATION_SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            max_tokens=180,
        )
        spec = extract_json_object(raw_spec)
    except Exception:
        return fallback

    valid_chart_types = {"line", "bar", "horizontal_bar", "pie"}
    chart_type = spec.get("chart_type")
    x_column = spec.get("x")
    y_columns = spec.get("y")

    if chart_type not in valid_chart_types:
        return fallback
    if x_column is not None and x_column not in df.columns:
        return fallback
    if not isinstance(y_columns, list) or not y_columns:
        return fallback
    if any(column not in df.columns for column in y_columns):
        return fallback
    if any(column not in get_numeric_columns(df) for column in y_columns):
        return fallback

    return {
        "chart_type": chart_type,
        "x": x_column,
        "y": y_columns[:2],
        "title": str(spec.get("title") or title or question),
    }


def build_visualization(
    *,
    question: str,
    title: str,
    rows: list[dict[str, Any]],
    filename_prefix: str,
) -> tuple[str, str]:
    if not rows:
        raise ValueError("Нет данных для визуализации.")

    df = prepare_dataframe(rows)
    spec = choose_visualization_spec(question, title, rows)
    chart_type = spec["chart_type"]
    x_column = spec["x"]
    y_columns = spec["y"]
    chart_title = spec["title"]

    plt.style.use("ggplot")
    fig, ax = plt.subplots(figsize=(11, 6.5))

    if x_column is None:
        metric_df = pd.DataFrame(
            {
                "metric": y_columns,
                "value": [float(df.iloc[0][column]) for column in y_columns],
            }
        )
        ax.bar(metric_df["metric"], metric_df["value"], color="#2f6bff")
        ax.set_xlabel("Показатель")
        ax.set_ylabel("Значение")
    elif chart_type == "line":
        for y_column in y_columns:
            ax.plot(df[x_column], df[y_column], marker="o", linewidth=2, label=y_column)
        ax.set_xlabel(x_column)
        ax.set_ylabel(", ".join(y_columns))
        if len(y_columns) > 1:
            ax.legend()
    elif chart_type == "horizontal_bar":
        sorted_df = df.sort_values(by=y_columns[0], ascending=True).tail(15)
        ax.barh(sorted_df[x_column].astype(str), sorted_df[y_columns[0]], color="#1f8a70")
        ax.set_xlabel(y_columns[0])
        ax.set_ylabel(x_column)
    elif chart_type == "pie":
        pie_df = df.head(8)
        ax.pie(
            pie_df[y_columns[0]],
            labels=pie_df[x_column].astype(str),
            autopct="%1.1f%%",
            startangle=90,
        )
        ax.axis("equal")
    else:
        plot_df = df.head(15)
        if len(y_columns) == 1:
            ax.bar(plot_df[x_column].astype(str), plot_df[y_columns[0]], color="#2f6bff")
            ax.set_ylabel(y_columns[0])
        else:
            x_positions = range(len(plot_df))
            width = 0.35
            ax.bar(
                [position - width / 2 for position in x_positions],
                plot_df[y_columns[0]],
                width=width,
                label=y_columns[0],
                color="#2f6bff",
            )
            ax.bar(
                [position + width / 2 for position in x_positions],
                plot_df[y_columns[1]],
                width=width,
                label=y_columns[1],
                color="#1f8a70",
            )
            ax.set_xticks(list(x_positions))
            ax.set_xticklabels(plot_df[x_column].astype(str))
            ax.legend()
        ax.set_xlabel(x_column)

    ax.set_title(chart_title)
    if chart_type in {"bar", "line"} and x_column is not None:
        plt.setp(ax.get_xticklabels(), rotation=30, ha="right")

    fig.tight_layout()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{filename_prefix}_{timestamp}.png"
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as temp_file:
        fig.savefig(temp_file.name, dpi=180, bbox_inches="tight")
        temp_path = temp_file.name
    plt.close(fig)

    return temp_path, filename


def build_reports_help_text() -> str:
    lines = ["Доступные отчёты:"]
    for report in REPORTS.values():
        lines.append(f"/{report.command} - {report.title}")
        lines.append(report.description)
    return "\n".join(lines)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None:
        return
    logger.info("Command /start from %s", get_user_label(update))

    text = (
        "Я Spotify Data Studio в Telegram: отвечаю на вопросы по базе, строю отчёты, CSV и графики.\n\n"
        "Можно писать обычным текстом: «топ 10 популярных треков», «самые энергичные жанры», "
        "«грустные треки с высокой популярностью».\n\n"
        "Команды: /reports, /examples, /vibe, /surprise, /history, /health"
    )
    await update.message.reply_text(text)


async def reports_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None:
        return
    logger.info("Command /reports from %s", get_user_label(update))
    await update.message.reply_text(build_reports_help_text())


async def examples_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None:
        return
    logger.info("Command /examples from %s", get_user_label(update))
    await update.message.reply_text(build_examples_text())


async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None:
        return
    logger.info("Command /history from %s", get_user_label(update))
    await update.message.reply_text(build_history_text(context))


async def health_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None:
        return
    logger.info("Command /health from %s", get_user_label(update))
    try:
        rows = run_sql(
            """
            SELECT
                (SELECT COUNT(*) FROM tracks) AS tracks_count,
                (SELECT COUNT(*) FROM artists) AS artists_count,
                (SELECT COUNT(*) FROM genres) AS genres_count,
                (SELECT COUNT(*) FROM audio_features) AS features_count
            """.strip(),
            limit=None,
        )
        row = rows[0]
        await update.message.reply_text(
            "База отвечает.\n"
            f"Треков: {row['tracks_count']}\n"
            f"Артистов: {row['artists_count']}\n"
            f"Жанров: {row['genres_count']}\n"
            f"Аудио-фич: {row['features_count']}"
        )
    except Exception as exc:
        logger.exception("Health check failed")
        await update.message.reply_text(f"Проверка не прошла: {exc}")


async def vibe_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None:
        return
    logger.info("Command /vibe from %s", get_user_label(update))
    try:
        report = REPORTS["audio_profile"]
        rows = run_sql(report.sql, limit=None)
        store_last_result(
            context,
            title=report.title,
            question=report.description,
            sql=report.sql,
            rows=rows,
            filename_prefix=report.filename_prefix,
        )
        await update.message.reply_text(
            build_vibe_summary(rows),
            reply_markup=build_result_keyboard(),
        )
    except Exception as exc:
        logger.exception("Vibe command failed")
        await update.message.reply_text(f"Не удалось собрать vibe-портрет: {exc}")


async def surprise_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None:
        return
    logger.info("Command /surprise from %s", get_user_label(update))
    sql = """
        SELECT
            t.track_name,
            GROUP_CONCAT(DISTINCT a.artist_name ORDER BY a.artist_name SEPARATOR ', ') AS artists,
            g.genre_name,
            t.popularity,
            ROUND(af.danceability, 3) AS danceability,
            ROUND(af.energy, 3) AS energy,
            ROUND(af.valence, 3) AS valence
        FROM tracks t
        JOIN audio_features af ON af.track_id = t.track_id
        LEFT JOIN genres g ON g.genre_id = t.genre_id
        LEFT JOIN track_artist ta ON ta.track_id = t.track_id
        LEFT JOIN artists a ON a.artist_id = ta.artist_id
        WHERE t.popularity >= 40
        GROUP BY t.track_id, t.track_name, g.genre_name, t.popularity,
                 af.danceability, af.energy, af.valence
        ORDER BY RAND()
        LIMIT 7
    """.strip()
    try:
        rows = run_sql(sql, limit=None)
        store_last_result(
            context,
            title="Неожиданная подборка",
            question="Случайная подборка интересных треков",
            sql=sql,
            rows=rows,
            filename_prefix="surprise_mix",
        )
        await update.message.reply_text(
            "Неожиданная подборка:\n<pre>" + html.escape(build_text_table(rows, max_rows=7)) + "</pre>",
            parse_mode="HTML",
            reply_markup=build_result_keyboard(),
        )
    except Exception as exc:
        logger.exception("Surprise command failed")
        await update.message.reply_text(f"Не удалось собрать подборку: {exc}")


async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None or update.message.text is None:
        return

    command_name = update.message.text.split()[0].lstrip("/").split("@")[0]
    logger.info("Command /%s from %s", command_name, get_user_label(update))
    report = REPORTS.get(command_name)
    if report is None:
        await update.message.reply_text("Неизвестный отчёт. Используйте /reports.")
        return

    await update.message.reply_text(f"Готовлю отчёт: {report.title}...")

    try:
        rows = run_sql(report.sql, limit=None)
        remember_question(context, f"/{report.command}", report.sql, len(rows))
        preview_table = build_text_table(rows)
        store_last_result(
            context,
            title=report.title,
            question=report.description,
            sql=report.sql,
            rows=rows,
            filename_prefix=report.filename_prefix,
        )
        preview_message = (
            f"{report.title}\n"
            f"{report.description}\n\n"
            f"Предпросмотр:\n<pre>{html.escape(preview_table)}</pre>"
        )
        await update.message.reply_text(
            preview_message,
            parse_mode="HTML",
            reply_markup=build_result_keyboard(),
        )
    except Error as exc:
        await update.message.reply_text(f"Ошибка базы данных при построении отчёта: {exc}")
    except Exception as exc:
        await update.message.reply_text(f"Не удалось сформировать отчёт: {exc}")


async def question_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None or not update.message.text:
        return

    show_sql = get_bool_env("SHOW_SQL", default=False)
    question = update.message.text.strip()
    user_label = get_user_label(update)
    logger.info("Message from %s: %s", user_label, truncate_text(question, 120))

    try:
        route = route_message(question)
        logger.info("Route selected: user=%s route=%s", user_label, route)
        if route != "DATABASE":
            await update.message.reply_text(build_chat_answer(question))
            return

        await update.message.reply_text("Обрабатываю запрос к Spotify-базе...")

        sql = generate_sql(question)
        rows = run_sql(sql)
        remember_question(context, question, sql, len(rows))
        result_text = build_human_answer(question, sql, rows)
        store_last_result(
            context,
            title="Результат запроса",
            question=question,
            sql=sql,
            rows=rows,
            filename_prefix="query_result",
        )

        if show_sql:
            answer = f"SQL:\n{sql}\n\nРезультат:\n{result_text}"
        else:
            answer = result_text

        await update.message.reply_text(answer, reply_markup=build_result_keyboard())
    except requests.HTTPError as exc:
        logger.exception("OpenRouter HTTP error")
        details = exc.response.text[:500] if exc.response is not None else str(exc)
        await update.message.reply_text(f"Ошибка OpenRouter: {details}")
    except Error as exc:
        logger.exception("Database error")
        await update.message.reply_text(f"Ошибка базы данных: {exc}")
    except Exception as exc:
        logger.exception("Question handling failed")
        await update.message.reply_text(f"Не удалось обработать запрос: {exc}")


async def result_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None or query.message is None:
        return

    result = get_last_result(context)
    if result is None:
        await query.answer("Сначала выполните запрос или отчёт.", show_alert=True)
        return

    if query.data == SQL_CALLBACK_DATA:
        logger.info("Callback SQL requested")
        await query.answer("Отправляю SQL.")
        sql_text = html.escape(result["sql"])
        await query.message.reply_text(
            f"SQL последнего запроса:\n<pre>{sql_text}</pre>",
            parse_mode="HTML",
        )
        return

    if query.data == CSV_CALLBACK_DATA:
        logger.info("Callback CSV requested")
        await query.answer("Готовлю CSV-файл.")
        file_path = ""
        try:
            file_path, filename = create_csv_report(result["rows"], result["filename_prefix"])
            with open(file_path, "rb") as report_file:
                await query.message.reply_document(
                    document=InputFile(report_file, filename=filename),
                    caption=f"{result['title']}. CSV-файл.",
                    read_timeout=TELEGRAM_READ_TIMEOUT,
                    write_timeout=TELEGRAM_WRITE_TIMEOUT,
                    connect_timeout=TELEGRAM_CONNECT_TIMEOUT,
                    pool_timeout=TELEGRAM_POOL_TIMEOUT,
                )
        finally:
            if file_path and os.path.exists(file_path):
                os.unlink(file_path)
        return

    if query.data == VISUALIZATION_CALLBACK_DATA:
        logger.info("Callback chart requested")
        await query.answer("Строю график.")
        image_path = ""
        try:
            image_path, filename = build_visualization(
                question=result["question"],
                title=result["title"],
                rows=result["rows"],
                filename_prefix=result["filename_prefix"] + "_chart",
            )
            with open(image_path, "rb") as image_file:
                await query.message.reply_photo(
                    photo=InputFile(image_file, filename=filename),
                    caption=f"График: {result['title']}",
                    read_timeout=TELEGRAM_READ_TIMEOUT,
                    write_timeout=TELEGRAM_WRITE_TIMEOUT,
                    connect_timeout=TELEGRAM_CONNECT_TIMEOUT,
                    pool_timeout=TELEGRAM_POOL_TIMEOUT,
                )
        except Exception as exc:
            await query.message.reply_text(f"Не удалось построить график: {exc}")
        finally:
            if image_path and os.path.exists(image_path):
                os.unlink(image_path)
        return

    await query.answer()


async def post_init(application: Application) -> None:
    commands = [
        BotCommand("start", "Краткая справка по боту"),
        BotCommand("reports", "Список готовых отчётов"),
        BotCommand("examples", "Примеры умных вопросов"),
        BotCommand("vibe", "Музыкальный портрет базы"),
        BotCommand("surprise", "Случайная подборка треков"),
        BotCommand("history", "Последние запросы"),
        BotCommand("health", "Проверка базы"),
    ]
    for report in REPORTS.values():
        commands.append(BotCommand(report.command, report.title[:32]))
    await application.bot.set_my_commands(commands)
    logger.info("Telegram command menu updated: commands=%s", len(commands))


def main() -> None:
    load_env_file()
    setup_logging()
    telegram_token = require_env("TELEGRAM_BOT_TOKEN")
    logger.info("Booting Spotify Data Studio bot")

    app = (
        ApplicationBuilder()
        .token(telegram_token)
        .read_timeout(TELEGRAM_READ_TIMEOUT)
        .write_timeout(TELEGRAM_WRITE_TIMEOUT)
        .connect_timeout(TELEGRAM_CONNECT_TIMEOUT)
        .pool_timeout(TELEGRAM_POOL_TIMEOUT)
        .post_init(post_init)
        .build()
    )

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", start_command))
    app.add_handler(CommandHandler("reports", reports_command))
    app.add_handler(CommandHandler("examples", examples_command))
    app.add_handler(CommandHandler("vibe", vibe_command))
    app.add_handler(CommandHandler("surprise", surprise_command))
    app.add_handler(CommandHandler("history", history_command))
    app.add_handler(CommandHandler("health", health_command))
    app.add_handler(CommandHandler(list(REPORTS.keys()), report_command))
    app.add_handler(CallbackQueryHandler(result_button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, question_handler))

    logger.info("Polling started")
    app.run_polling()


if __name__ == "__main__":
    main()
