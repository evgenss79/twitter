import openai
import json
import random
import time
import unicodedata
import sqlite3
import requests
from requests_oauthlib import OAuth1Session
import logging
import schedule
from threading import Thread
from logging.handlers import RotatingFileHandler
from difflib import SequenceMatcher
import shutil
from datetime import datetime, timedelta
from functools import partial
import functools
from threading import Lock
import sys
import asyncio
from typing import List
import aiosqlite

sys.setrecursionlimit(2000)

# Настройка ротации логов
log_file = "bot.log"

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=3)  # 5 MB на файл, 3 резервных копии
    ],
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Logging is set up. Log file: %s", log_file)

# Загружаем конфигурацию
def load_config(file_path="config.json"):
    try:
        with open(file_path, "r") as config_file:
            return json.load(config_file)
    except Exception as e:
        logging.error(f"Failed to load configuration: {e}")
        raise

config = load_config()

def reload_config():
    global config
    try:
        config = load_config()
        logging.info("Configuration reloaded successfully.")
    except Exception as e:
        logging.error(f"Failed to reload configuration: {e}")

def write_config(config):
    with open("config.json", "w") as file:
        json.dump(config, file, indent=4)

# Устанавливаем API-ключ OpenAI
def set_openai_api_key():
    try:
        openai.api_key = config["openai"]["api_key"]
        openai.api_base = "https://api.openai.com/v1"
        logging.info("OpenAI API key set successfully.")
    except KeyError as e:
        logging.error(f"Missing OpenAI API key in config: {e}")
        raise

def initialize_oauth_session():
    """
    Инициализирует OAuth1Session для взаимодействия с Twitter API.

    Returns:
        OAuth1Session: Сессия Twitter API.
    """
    try:
        session = OAuth1Session(
            client_key=config["twitter"]["api_key"],
            client_secret=config["twitter"]["api_secret_key"],
            resource_owner_key=config["twitter"]["access_token"],
            resource_owner_secret=config["twitter"]["access_token_secret"],
        )
        logging.info("OAuth1 session initialized successfully.")
        return session
    except KeyError as e:
        logging.error(f"Missing Twitter API keys in configuration: {e}")
        return None

def send_twitter_request(session, url, method="GET", params=None, data=None):
    """
    Унифицированная отправка запросов с учетом обработки лимитов.

    Args:
        session: OAuth1Session объект.
        url (str): URL для отправки запроса.
        method (str): HTTP метод (GET или POST).
        params (dict): Параметры запроса.
        data (dict): Данные запроса (для POST).

    Returns:
        dict: JSON-ответ от API.
    """
    try:
        # Задержка между запросами
        delay = config.get("api", {}).get("request_delay", 1)
        time.sleep(delay)

        if method.upper() == "GET":
            response = session.get(url, params=params)
        elif method.upper() == "POST":
            response = session.post(url, json=data)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        # Проверка лимитов
        check_rate_limit(response)

        # Обработка ответа
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error during Twitter API request: {e}")
        return None

# Универсальная функция работы с базой данных
def get_db_connection():
    conn = sqlite3.connect("bot_data.db")
    conn.execute("PRAGMA busy_timeout = 2000")
    return conn

db_lock = Lock()

def execute_db_query(query, params=(), commit=False):
    """
    Выполняет запрос к базе данных и возвращает результат.

    Args:
        query (str): SQL-запрос.
        params (tuple): Параметры для выполнения запроса.
        commit (bool): Нужно ли фиксировать изменения в базе данных.

    Returns:
        list: Результат выполнения запроса (список кортежей).
    """
    try:
        with db_lock, get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            if commit:
                conn.commit()
            return cursor.fetchall()
    except Exception as e:
        logging.error(f"Error executing query: {query} with params {params}. Error: {e}")
        return []

def setup_database():
    """
    Создаёт таблицы в базе данных, если их ещё нет.
    """
    queries = [
        """
        CREATE TABLE IF NOT EXISTS tweets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT UNIQUE,
            likes INTEGER DEFAULT 0,
            retweets INTEGER DEFAULT 0,
            comments INTEGER DEFAULT 0,
            is_published INTEGER DEFAULT 0,
            metrics_updated INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS liked_tweets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tweet_id TEXT UNIQUE,
            content TEXT UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT UNIQUE,
            type TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS keywords (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ideas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            idea TEXT UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS retweeted_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tweet_id TEXT UNIQUE,
            content TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS popular_accounts (
            account_id TEXT PRIMARY KEY,  -- Добавлено account_id
            username TEXT,
            followers_count INTEGER,
            description TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS popular_tweets (
            tweet_id TEXT PRIMARY KEY,
            content TEXT,
            likes INTEGER,
            retweets INTEGER,
            comments INTEGER,
            created_at TEXT,
            author_id TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS followed_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT UNIQUE,
            account_name TEXT,
            followed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    ]
    try:
        for query in queries:
            execute_db_query(query)
        logging.info("Database setup completed successfully.")
    except Exception as e:
        logging.error(f"Error setting up database: {e}")

# Пример функции, использующей execute_db_query
def save_tweet_to_db(content):
    try:
        query = "INSERT OR IGNORE INTO tweets (content) VALUES (?)"
        execute_db_query(query, (content,), commit=True)
        logging.info(f"Tweet saved to database: {content}")
    except Exception as e:
        logging.error(f"Error saving tweet to database: {e}")

def get_unpublished_tweet():
    try:
        query = """
            SELECT id, content
            FROM tweets
            WHERE is_published = 0
            ORDER BY created_at ASC
            LIMIT 1
        """
        result = execute_db_query(query)
        return result[0] if result else (None, None)
    except Exception as e:
        logging.error(f"Error fetching unpublished tweet: {e}")
        return None, None

def mark_tweet_as_published(tweet_id):
    """
    Обновляет статус твита как опубликованного.
    """
    try:
        query = "UPDATE tweets SET is_published = 1 WHERE id = ?"
        execute_db_query(query, (tweet_id,), commit=True)
        logging.info(f"Marked tweet ID {tweet_id} as published.")
    except Exception as e:
        logging.error(f"Error marking tweet as published: {e}")

def is_already_published(tweet_id):
    """
    Проверяет, опубликован ли твит.
    """
    try:
        query = "SELECT is_published FROM tweets WHERE id = ?"
        result = execute_db_query(query, (tweet_id,))
        return result[0][0] == 1 if result else False
    except Exception as e:
        logging.error(f"Error checking if tweet is published: {e}")
        return True

def is_tweet_relevant(tweet_text):
    """
    Проверяет, соответствует ли текст твита тематике и риторике аккаунта
    на основе опубликованных твитов и лайкнутых ранее твитов.

    Args:
        tweet_text (str): Текст твита.

    Returns:
        bool: True, если твит релевантен, иначе False.
    """
    try:
        # Получаем контекст из базы данных
        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()
            
            # Извлечение опубликованных твитов
            cursor.execute("SELECT content FROM tweets ORDER BY created_at DESC LIMIT 50")
            published_tweets = [row[0] for row in cursor.fetchall()]
            
            # Извлечение лайкнутых твитов
            cursor.execute("SELECT content FROM liked_tweets ORDER BY created_at DESC LIMIT 50")
            liked_tweets = [row[0] for row in cursor.fetchall()]

        # Формируем контекст из опубликованных и лайкнутых твитов
        account_context = "\n".join(published_tweets + liked_tweets)

        # Оцениваем релевантность
        prompt = (
            f"Determine if the following tweet is relevant to the account's context and themes:\n\n"
            f"Tweet: \"{tweet_text}\"\n\n"
            f"Account Context:\n\"{account_context}\"\n\n"
            f"Reply with 'Yes' if it is relevant, otherwise reply with 'No'."
        )

        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are an assistant that evaluates the relevance of content."},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )

        evaluation = response["choices"][0]["message"]["content"].strip().lower()
        return evaluation == "yes"
    except Exception as e:
        logging.error(f"Error in is_tweet_relevant: {e}")
        return False
    
def post_tweet(session, content):
    """
    Публикует твит, используя переданную сессию.

    Args:
        session (OAuth1Session): Twitter API session.
        content (str): Текст твита.

    Returns:
        str: ID опубликованного твита или None в случае ошибки.
    """
    url = "https://api.twitter.com/2/tweets"
    data = {"text": content}
    try:
        response = session.post(url, json=data)
        response.raise_for_status()
        result = response.json()
        if "data" in result and "id" in result["data"]:
            tweet_id = result["data"]["id"]
            logging.info(f"Tweet published successfully: {tweet_id}")
            return tweet_id
        else:
            logging.error(f"Unexpected response format: {result}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error posting tweet: {e}")
        return None

def post_unpublished_tweet(session):
    """
    Извлекает и публикует первый непубликованный твит из базы данных.
    """
    try:
        tweet_id, tweet_content = get_unpublished_tweet()
        if tweet_content:
            logging.info(f"Attempting to post tweet: {tweet_content}")
            if is_already_published(tweet_id):
                logging.warning(f"Tweet ID {tweet_id} is already marked as published. Skipping.")
                return
            tweet_result = post_tweet(session, tweet_content)
            if tweet_result:
                mark_tweet_as_published(tweet_id)
                logging.info(f"Successfully posted unpublished tweet: {tweet_content}")
            else:
                logging.error(f"Failed to post tweet: {tweet_content}")
        else:
            logging.info("No unpublished tweets to post.")
    except Exception as e:
        logging.error(f"Error in post_unpublished_tweet: {e}")

def comment_on_tweets(session):
    """
    Поиск твитов и добавление комментариев.
    """
    try:
        max_comments = config["schedule"].get("max_comments_per_cycle", 5)
        comments_posted = 0

        tweets = search_tweets(session)  # Ищем твиты
        if not tweets:
            logging.info("No tweets found for commenting.")
            return

        logging.info(f"Total tweets retrieved: {len(tweets)}")

        # Убираем уже прокомментированные твиты
        already_commented_query = "SELECT tweet_id FROM comments"
        already_commented = {row[0] for row in execute_db_query(already_commented_query)}

        filtered_tweets = [tweet for tweet in tweets if tweet["id"] not in already_commented]
        logging.info(f"Filtered tweets for commenting: {len(filtered_tweets)}")

        if not filtered_tweets:
            logging.info("No tweets available for commenting after filtering.")
            return

        for tweet in filtered_tweets:
            if comments_posted >= max_comments:
                break

            tweet_id = tweet["id"]
            tweet_text = tweet["text"]

            is_relevant = is_tweet_relevant(tweet_text)
            if not is_relevant:
                logging.info(f"Tweet ID {tweet_id} is not relevant. Skipping.")
                continue

            logging.info(f"Attempting to comment on Tweet ID: {tweet_id}, Text: {tweet_text[:50]}...")

            comment = generate_comment(session, tweet_id)
            if comment:
                success = post_comment(session, tweet_id, comment)
                if success:
                    # Записываем в базу данных только после успешной публикации
                    query = "INSERT OR IGNORE INTO comments (tweet_id, comment_id, created_at) VALUES (?, ?, ?)"
                    execute_db_query(query, (tweet_id, success, datetime.utcnow()), commit=True)
                    comments_posted += 1
                else:
                    logging.warning(f"Failed to post comment for Tweet ID {tweet_id}.")
            else:
                logging.warning(f"Failed to generate comment for Tweet ID {tweet_id}.")

            # Установка задержки между комментариями
            time.sleep(config["schedule"].get("comment_delay", 5))

        logging.info(f"Total comments posted: {comments_posted}")
    except Exception as e:
        logging.error(f"Error during comment on tweets: {e}")

def generate_chat_completion(prompt, model="gpt-4", max_tokens=150, temperature=0.7):
    """
    Генерирует завершение чата с использованием OpenAI ChatCompletion API.
    """
    try:
        response = openai.ChatCompletion.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            temperature=temperature
        )
        return response["choices"][0]["message"]["content"].strip()
    except openai.error.OpenAIError as e:
        logging.error(f"Error generating chat completion: {e}")
        return None

def generate_tweet():
    """
    Генерирует уникальный твит с использованием OpenAI GPT-4 на основе шаблонов, ключевых слов и "открывающих" фраз из конфигурации.
    Обеспечивает лаконичность, уникальность и добавляет хэштеги.

    Возвращает:
        str: Сгенерированный твит или None, если генерация не удалась.
    """
    try:
        max_length = config.get("generation", {}).get("max_tweet_length", 250)

        # Получаем последние 20 твитов из базы данных для проверки на уникальность
        query = "SELECT content FROM tweets ORDER BY created_at DESC LIMIT 20"
        existing_tweets = [row[0] for row in execute_db_query(query)]

        while True:
            # Проверка наличия необходимых элементов в конфигурации
            if not config.get("prompts") or not config["prompts"].get("general_tweets") or not config["prompts"].get("promotional_tweets"):
                logging.error("Шаблоны твитов отсутствуют в конфигурации. Пропуск генерации.")
                return None

            if not config.get("openers"):
                logging.error("В конфигурации отсутствуют открывающие фразы (openers).")
                return None

            if not config.get("keywords"):
                logging.error("В конфигурации отсутствуют ключевые слова (keywords).")
                return None

            # Выбор случайной категории, шаблона, открывающей фразы и ключевого слова
            category = random.choice(["general_tweets", "promotional_tweets"])
            prompt = random.choice(config["prompts"][category])
            opener = random.choice(config["openers"])
            keyword = random.choice(config["keywords"])

            # Генерация хэштегов из конфигурации
            hashtags = random.sample(config["hashtags"], min(len(config["hashtags"]), 2))
            hashtags_str = " ".join(hashtags)

            # Формирование полного промпта для OpenAI
            full_prompt = (
                f"{opener} {prompt} {keyword}. Убедитесь, что вывод лаконичен и не превышает "
                f"{max_length - len(hashtags_str) - 1} символов, включая хэштеги: {hashtags_str}."
            )

            # Вызов OpenAI API для генерации твита
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "Вы бот, который пишет креативные и увлекательные твиты."},
                    {"role": "user", "content": full_prompt}
                ],
                temperature=0.7
            )
            new_tweet = response["choices"][0]["message"]["content"].strip()

            # Проверка уникальности и валидности твита
            if len(new_tweet) <= max_length and not is_similar(new_tweet, existing_tweets):
                # Добавляем хэштеги и сохраняем твит в базу данных
                new_tweet_with_hashtags = f"{new_tweet} {hashtags_str}"
                save_tweet_to_db(new_tweet_with_hashtags)
                logging.info(f"Сгенерирован уникальный твит: {new_tweet_with_hashtags}")
                return new_tweet_with_hashtags
            else:
                logging.warning("Сгенерированный твит либо слишком длинный, либо слишком похож на существующие. Повтор попытки...")

            time.sleep(1)  # Небольшая задержка перед повторной попыткой
    except Exception as e:
        logging.error(f"Ошибка при генерации твита: {e}")
        return None

def get_existing_templates() -> List[str]:
    """
    Получает существующие шаблоны из базы данных.
    """
    with sqlite3.connect('your_database.db') as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT template FROM templates WHERE type = 'general'")
        templates = cursor.fetchall()
    return [template[0] for template in templates]

async def generate_new_templates(num_templates: int = 3) -> None:
    try:
        topics = await generate_topics()
        
        prompt = (
            f"Create {num_templates} unique template requests for generating cryptocurrency "
            f"and self-development related tweets. Each template should include a placeholder "
            f"for a specific topic (use {{TOPIC}}) and instructions for including motivational content, "
            f"calls to action, and relevant hashtags. The templates should be general enough to "
            f"create a variety of tweets but specific enough to guide the content creation."
        )

        response = await asyncio.to_thread(
            openai.ChatCompletion.create,
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a bot that creates tweet template requests."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7
        )

        new_templates = response["choices"][0]["message"]["content"].strip().split("\n")
        new_templates = [
            clean_unicode(template.strip().lstrip("1234567890.- ").strip())
            for template in new_templates if template.strip()
        ]

        # Удаление дубликатов и проверка уникальности
        existing_templates = get_existing_templates()
        unique_new_templates = list(set(new_templates) - set(existing_templates))

        if unique_new_templates:
            for template in unique_new_templates:
                save_template_to_db(template, "general")
            
            # Обновление config.json
            with open('config.json', 'r+') as f:
                config = json.load(f)
                config['templates'] = unique_new_templates
                config['topics'] = topics
                f.seek(0)
                json.dump(config, f, indent=4)
                f.truncate()

            logging.info(f"Generated and saved {len(unique_new_templates)} new unique template requests and {len(topics)} topics.")
        else:
            logging.info("No new unique template requests were generated.")

    except Exception as e:
        logging.error(f"Error generating template requests: {e}")

async def generate_topics() -> List[str]:
    prompt = (
        "Generate a list of 20 topics related to cryptocurrency, financial independence, "
        "and self-development. Each topic should be a short phrase or concept."
    )
    
    response = await asyncio.to_thread(
        openai.ChatCompletion.create,
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are a bot that creates relevant topics for tweets."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.7
    )

    topics = response["choices"][0]["message"]["content"].strip().split("\n")
    return [topic.strip().lstrip("1234567890.- ").strip() for topic in topics if topic.strip()]

async def generate_new_templates(num_templates: int = 3) -> None:
    try:
        topics = await generate_topics()
        
        prompt = (
            f"Create {num_templates} unique template requests for generating cryptocurrency "
            f"and self-development related tweets. Each template should include a placeholder "
            f"for a specific topic (use {{TOPIC}}) and instructions for including motivational content, "
            f"calls to action, and relevant hashtags. The templates should be general enough to "
            f"create a variety of tweets but specific enough to guide the content creation."
        )

        response = await asyncio.to_thread(
            openai.ChatCompletion.create,
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a bot that creates tweet template requests."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7
        )

        new_templates = response["choices"][0]["message"]["content"].strip().split("\n")
        new_templates = [
            clean_unicode(template.strip().lstrip("1234567890.- ").strip())
            for template in new_templates if template.strip()
        ]

        # Удаление дубликатов и проверка уникальности
        existing_templates = await get_existing_templates()
        unique_new_templates = list(set(new_templates) - set(existing_templates))

        if unique_new_templates:
            await asyncio.gather(*[save_template_to_db(template, "general") for template in unique_new_templates])
            
            # Обновление config.json
            with open('config.json', 'r+') as f:
                config = json.load(f)
                config['templates'] = unique_new_templates
                config['topics'] = topics
                f.seek(0)
                json.dump(config, f, indent=4)
                f.truncate()

            logging.info(f"Generated and saved {len(unique_new_templates)} new unique template requests and {len(topics)} topics.")
        else:
            logging.info("No new unique template requests were generated.")

    except Exception as e:
        logging.error(f"Error generating template requests: {e}")

def save_template_to_db(content, template_type):
    """
    Сохраняет шаблон в базу данных.
    """
    try:
        query = "INSERT OR IGNORE INTO templates (content, type) VALUES (?, ?)"
        execute_db_query(query, (content, template_type), commit=True)
        logging.info(f"Template saved to database: {content}")
    except Exception as e:
        logging.error(f"Error saving template to database: {e}")

def save_keyword_to_db(keyword):
    """
    Сохраняет ключевое слово в базу данных.
    """
    try:
        query = "INSERT OR IGNORE INTO keywords (keyword) VALUES (?)"
        execute_db_query(query, (keyword,), commit=True)
        logging.info(f"Keyword saved to database: {keyword}")
    except Exception as e:
        logging.error(f"Error saving keyword to database: {e}")

def update_tweet_metrics_in_db(session):
    """
    Обновляет метрики твитов в базе данных.
    """
    try:
        query = "SELECT id FROM tweets WHERE likes = 0 AND retweets = 0 AND comments = 0"
        tweets = execute_db_query(query)

        for tweet_id, in tweets:
            metrics = fetch_tweet_metrics(session, tweet_id)
            if metrics:
                update_query = """
                    UPDATE tweets
                    SET likes = ?, retweets = ?, comments = ?
                    WHERE id = ?
                """
                execute_db_query(update_query, (metrics["likes"], metrics["retweets"], metrics["comments"], tweet_id), commit=True)
                logging.info(f"Updated metrics for tweet ID {tweet_id}: {metrics}")
    except Exception as e:
        logging.error(f"Error updating tweet metrics: {e}")

def export_ideas_to_file(filename="ideas_export.txt"):
    """
    Экспортирует идеи из базы данных в текстовый файл.
    """
    try:
        query = "SELECT idea, created_at FROM ideas ORDER BY created_at DESC"
        ideas = execute_db_query(query)

        with open(filename, "w") as file:
            for idea, created_at in ideas:
                file.write(f"{idea} (Generated on: {created_at})\n")

        logging.info(f"Ideas exported to {filename} successfully.")
    except Exception as e:
        logging.error(f"Error exporting ideas: {e}")

def search_tweets(session):
    """
    Выполняет поиск твитов по ключевым словам и сохраняет их в базу данных.
    """
    try:
        # Получаем список ключевых слов
        keywords = config["criteria"].get("keywords", [])
        if not keywords:
            logging.error("No keywords available for search. Skipping.")
            return []

        # Выбираем случайные ключевые слова
        if len(keywords) > 10:
            keywords = random.sample(keywords, 10)

        # Формируем поисковый запрос
        query = " OR ".join(keywords)
        logging.info(f"Using keywords for search: {keywords}")

        max_results = config["search"].get("max_results", 10)
        total_limit = config["search"].get("total_limit", 50)

        url = "https://api.twitter.com/2/tweets/search/recent"
        params = {
            "query": query,
            "max_results": max_results,
            "tweet.fields": "id,text,author_id,public_metrics,created_at",
        }

        tweets = []
        total_count = 0
        unique_tweet_ids = set()

        while total_count < total_limit:
            response = send_twitter_request(session, url, method="GET", params=params)

            if response is None or "data" not in response:
                logging.warning("No tweets found in response.")
                break

            for tweet in response["data"]:
                tweet_id = tweet.get("id")
                tweet_text = tweet.get("text")
                author_id = tweet.get("author_id")
                created_at = tweet.get("created_at")
                metrics = tweet.get("public_metrics", {})

                # Проверяем наличие ключевых данных
                if not all([tweet_id, tweet_text, author_id, created_at]):
                    logging.warning(f"Skipping incomplete tweet data: {tweet}")
                    continue

                # Исключаем дубликаты
                if tweet_id in unique_tweet_ids:
                    continue

                # Сохраняем уникальный tweet_id
                unique_tweet_ids.add(tweet_id)
                total_count += 1

                # Сохраняем в базу данных
                insert_query = """
                    INSERT OR IGNORE INTO tweets (id, content, likes, retweets, comments, is_published, created_at)
                    VALUES (?, ?, ?, ?, ?, 0, ?)
                """
                execute_db_query(
                    insert_query,
                    (
                        tweet_id,
                        tweet_text,
                        metrics.get("like_count", 0),
                        metrics.get("retweet_count", 0),
                        metrics.get("reply_count", 0),
                        created_at,
                    ),
                    commit=True,
                )
                tweets.append(tweet)

            # Проверяем на наличие токена для следующей страницы
            if "meta" not in response or "next_token" not in response["meta"]:
                break

            params["next_token"] = response["meta"]["next_token"]

        logging.info(f"Search completed. Total tweets retrieved: {len(tweets)}")
        return tweets
    except Exception as e:
        logging.error(f"Error in search_tweets: {e}")
        return []

def get_account_context():
    """
    Генерирует контекст аккаунта для проверки релевантности.
    Извлекает последние успешные твиты или лайкнутые.
    """
    try:
        # Извлекаем только последние успешные твиты или лайкнутые
        query = """
            SELECT content 
            FROM tweets
            WHERE likes > 10 OR retweets > 5
            ORDER BY created_at DESC
            LIMIT 20
        """
        context = "\n".join([row[0] for row in execute_db_query(query)])
        max_length = 3000  # Ограничение длины контекста
        return context[:max_length] + "..." if len(context) > max_length else context
    except Exception as e:
        logging.error(f"Error generating account context: {e}")
        return ""

def comment_on_tweets(session):
    """
    Поиск твитов и добавление комментариев.
    """
    try:
        max_comments = config["schedule"].get("max_comments_per_cycle", 5)
        comments_posted = 0

        tweets = search_tweets(session)  # Ищем твиты
        if not tweets:
            logging.info("No tweets found for commenting.")
            return

        logging.info(f"Total tweets retrieved: {len(tweets)}")

        for tweet in tweets:
            if comments_posted >= max_comments:
                break

            tweet_id = tweet["id"]
            tweet_text = tweet["text"]

            # Передаем оба аргумента в is_tweet_relevant
            is_relevant = is_tweet_relevant(tweet_id, tweet_text)
            if not is_relevant:
                logging.info(f"Tweet ID {tweet_id} is not relevant. Skipping.")
                continue

            logging.info(f"Attempting to comment on Tweet ID: {tweet_id}, Text: {tweet_text[:50]}...")

            comment = generate_comment(session, tweet_id)
            if comment:
                success = post_comment(session, tweet_id, comment)
                if success:
                    # Записываем в базу данных только после успешной публикации
                    query = "INSERT OR IGNORE INTO comments (tweet_id, comment_id, created_at) VALUES (?, ?, ?)"
                    execute_db_query(query, (tweet_id, success, datetime.utcnow()), commit=True)
                    comments_posted += 1
                else:
                    logging.warning(f"Failed to post comment for Tweet ID {tweet_id}.")
            else:
                logging.warning(f"Failed to generate comment for Tweet ID {tweet_id}.")

            # Установка задержки между комментариями
            time.sleep(config["schedule"].get("comment_delay", 5))

        logging.info(f"Total comments posted: {comments_posted}")
    except Exception as e:
        logging.error(f"Error during comment on tweets: {e}")

def is_similar(new_tweet, existing_tweets, threshold=0.8):
    """
    Проверяет, схож ли новый твит с существующими твитами.
    """
    try:
        for tweet in existing_tweets:
            similarity = SequenceMatcher(None, new_tweet, tweet).ratio()
            if similarity >= threshold:
                return True
        return False
    except Exception as e:
        logging.error(f"Error in is_similar: {e}")
        return False

def get_top_tweets(limit=5):
    """
    Извлекает топовые твиты по лайкам и ретвитам.
    """
    try:
        query = """
            SELECT content, likes, retweets
            FROM tweets
            ORDER BY likes DESC, retweets DESC
            LIMIT ?
        """
        return execute_db_query(query, (limit,))
    except Exception as e:
        logging.error(f"Error in get_top_tweets: {e}")
        return []

def generate_comment(session, tweet_id):
    """
    Генерирует комментарий к указанному твиту.
    """
    try:
        original_tweet = get_original_tweet(session, tweet_id)
        if not original_tweet:
            return None

        tweet_text = original_tweet["text"]
        comment_category = "general_comments" if len(tweet_text) < 50 else "promotional_comments"
        template = random.choice(config["prompts"][comment_category])

        prompt = (
            f"{template.replace('{{tweet_text}}', tweet_text)}\n\n"
            "Generate a concise and engaging comment under 100 characters."
        )

        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a bot that writes relevant Twitter replies."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=50
        )

        comment = response["choices"][0]["message"]["content"].strip()
        return comment if len(comment) <= 100 else None
    except Exception as e:
        logging.error(f"Error generating comment: {e}")
        return None

def post_comment(session, tweet_id, comment_text):
    """
    Публикует комментарий к указанному твиту.
    """
    try:
        url = "https://api.twitter.com/2/tweets"
        data = {
            "text": comment_text,
            "reply": {"in_reply_to_tweet_id": tweet_id},
        }
        response = send_twitter_request(session, url, method="POST", data=data)
        if response and "data" in response:
            comment_id = response["data"]["id"]
            query = """
                INSERT INTO comments (tweet_id, comment_id, created_at)
                VALUES (?, ?, ?)
            """
            execute_db_query(query, (tweet_id, comment_id, datetime.utcnow()))
            logging.info(f"Comment successfully posted: {comment_id}")
            return comment_id
        return None
    except Exception as e:
        logging.error(f"Error posting comment: {e}")
        return None

def like_commented_tweets(session):
    """
    Ставит лайки на твиты, которые бот прокомментировал.
    """
    try:
        query = """
            SELECT tweet_id, content
            FROM tweets
            WHERE tweet_id NOT IN (
                SELECT tweet_id FROM liked_tweets
            )
        """
        tweets_to_like = execute_db_query(query)

        max_likes = config["schedule"].get("max_likes_per_cycle", 5)
        likes_posted = 0

        for tweet_id, content in tweets_to_like:
            if likes_posted >= max_likes:
                break
            try:
                url = f"https://api.twitter.com/2/users/{config['twitter']['user_id']}/likes"
                data = {"tweet_id": tweet_id}
                response = send_twitter_request(session, url, method="POST", data=data)

                if response:
                    insert_query = """
                        INSERT OR IGNORE INTO liked_tweets (tweet_id, content)
                        VALUES (?, ?)
                    """
                    execute_db_query(insert_query, (tweet_id, content), commit=True)
                    likes_posted += 1
                    logging.info(f"Liked tweet ID {tweet_id}: {content}")

            except Exception as e:
                logging.error(f"Failed to like tweet ID {tweet_id}: {e}")

        logging.info(f"Total likes posted on commented tweets: {likes_posted}")
    except Exception as e:
        logging.error(f"Error in like_commented_tweets: {e}")

def like_relevant_tweets(session):
    """
    Ставит лайки на релевантные твиты.
    """
    try:
        max_likes = config["schedule"].get("max_likes_per_cycle", 5)
        likes_posted = 0

        tweets = search_tweets(session)
        if not tweets:
            logging.info("No tweets found for liking.")
            return

        for tweet in tweets:
            if likes_posted >= max_likes:
                break

            tweet_id = tweet["id"]
            tweet_text = tweet["text"]

            query = "SELECT COUNT(*) FROM liked_tweets WHERE tweet_id = ?"
            if execute_db_query(query, (tweet_id,))[0][0] > 0:
                logging.info(f"Tweet ID {tweet_id} is already liked. Skipping.")
                continue

            try:
                url = f"https://api.twitter.com/2/users/{config['twitter']['user_id']}/likes"
                data = {"tweet_id": tweet_id}
                response = send_twitter_request(session, url, method="POST", data=data)
                if response:
                    logging.info(f"Liked tweet ID {tweet_id}: {tweet_text}")

                    query = """
                        INSERT OR IGNORE INTO liked_tweets (tweet_id, content)
                        VALUES (?, ?)
                    """
                    execute_db_query(query, (tweet_id, tweet_text))
                    likes_posted += 1
            except Exception as e:
                logging.error(f"Failed to like tweet ID {tweet_id}: {e}")

        logging.info(f"Total likes posted on relevant tweets: {likes_posted}")
    except Exception as e:
        logging.error(f"Error in like_relevant_tweets: {e}")

def fetch_tweet_metrics(session, tweet_id):
    """
    Извлекает метрики твита через Twitter API.
    """
    url = f"https://api.twitter.com/2/tweets/{tweet_id}"
    params = {"tweet.fields": "public_metrics"}
    try:
        response = send_twitter_request(session, url, method="GET", params=params)
        if response and "data" in response:
            metrics = response["data"].get("public_metrics", {})
            logging.info(f"Metrics fetched for tweet ID {tweet_id}: {metrics}")
            return metrics
        else:
            logging.warning(f"No metrics found for tweet ID {tweet_id}")
            return {}
    except Exception as e:
        logging.error(f"Error fetching tweet metrics for ID {tweet_id}: {e}")
        return {}

def analyze_tweet_performance(session, limit=15):
    """
    Анализирует производительность твитов и обновляет метрики для необработанных твитов.
    """
    try:
        query = "SELECT id FROM tweets WHERE metrics_updated = 0 LIMIT ?"
        tweets = execute_db_query(query, (limit,))

        if not tweets:
            logging.info("No tweets found for metric analysis.")
            return

        for tweet_id, in tweets:
            metrics = fetch_tweet_metrics(session, tweet_id)
            if metrics:
                logging.info(f"Updated metrics for tweet ID {tweet_id}: {metrics}")

        logging.info("Tweet performance analysis completed.")
    except Exception as e:
        logging.error(f"Error analyzing tweet performance: {e}")

def analyze_successful_tweets():
    """
    Анализирует успешные твиты на основе лайков и ретвитов.
    
    Returns:
        str: Контекст успешных твитов для использования в генерации рекомендаций.
    """
    try:
        query = """
            SELECT content, likes, retweets
            FROM tweets
            WHERE likes > 100 OR retweets > 50
            ORDER BY likes DESC, retweets DESC
            LIMIT 10
        """
        successful_tweets = execute_db_query(query)

        if not successful_tweets:
            logging.info("No successful tweets found for analysis.")
            return None

        context = "\n".join([content for content, _, _ in successful_tweets])
        logging.info("Successful tweets analyzed for keyword generation.")
        return context
    except Exception as e:
        logging.error(f"Error analyzing successful tweets: {e}")
        return None

def analyze_keyword_performance():
    """
    Анализирует ключевые слова и обновляет конфигурацию, оставляя только наиболее успешные.
    """
    try:
        query = """
            SELECT keyword, COUNT(*) AS usage_count
            FROM keywords
            GROUP BY keyword
            ORDER BY usage_count DESC
        """
        keywords = execute_db_query(query)

        max_keywords = config["generation"].get("max_keywords", 50)
        relevant_keywords = [k[0] for k in keywords if k[1] > 0][:max_keywords]

        config["criteria"]["keywords"] = relevant_keywords
        write_config(config)
        logging.info("Updated config with most successful keywords.")
    except Exception as e:
        logging.error(f"Error analyzing keyword performance: {e}")

def print_top_tweets():
    """
    Печатает топ-5 твитов по лайкам и ретвитам.
    """
    try:
        query = """
            SELECT content, likes, retweets
            FROM tweets
            ORDER BY likes DESC, retweets DESC
            LIMIT 5
        """
        top_tweets = execute_db_query(query)

        if not top_tweets:
            print("\nNo tweets available in the database.")
            return

        print("\nTop Tweets:")
        for idx, (content, likes, retweets) in enumerate(top_tweets, start=1):
            print(f"{idx}. {content}\n   Likes: {likes}, Retweets: {retweets}\n")
    except Exception as e:
        print(f"Error printing top tweets: {e}")

def backup_database():
    """
    Создает резервную копию базы данных.
    """
    try:
        shutil.copy("bot_data.db", "bot_data_backup.db")
        logging.info("Database backup created successfully.")
    except Exception as e:
        logging.error(f"Error creating database backup: {e}")

def generate_ideas():
    """
    Генерирует новые идеи для контента или функций и сохраняет их в базу данных.
    """
    try:
        prompt = (
            "Suggest 5 new features or content ideas for a Twitter bot focused on cryptocurrency, "
            "financial independence, and self-development."
        )

        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a bot that suggests innovative ideas."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7
        )

        ideas = response["choices"][0]["message"]["content"].strip().split("\n")
        for idea in ideas:
            idea_text = idea.strip()
            query = "INSERT OR IGNORE INTO ideas (idea) VALUES (?)"
            execute_db_query(query, (idea_text,), commit=True)
            logging.info(f"Idea saved to database: {idea_text}")

        logging.info("New ideas generated successfully.")
    except Exception as e:
        logging.error(f"Error generating ideas: {e}")

def generate_new_keywords_dynamic(session):
    """
    Генерация новых ключевых слов с проверкой уникальности и релевантности.
    """
    try:
        # Генерация контекста на основе успешных твитов
        context = analyze_successful_tweets()
        if not context:
            logging.warning("No successful tweets found for keyword generation.")
            return

        # Формируем промпт для OpenAI
        prompt = (
            f"Based on the following successful tweets, generate 10 unique keywords "
            f"related to cryptocurrency, financial independence, and self-development:\n\n"
            f"{context}\n\n"
            f"Each keyword should be concise, under 20 characters, and relevant to the themes."
        )
        
        # Запрос к OpenAI
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a bot creating keywords."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7
        )

        # Обработка ответа
        new_keywords = response["choices"][0]["message"]["content"].strip().split("\n")
        unique_keywords = [
            clean_keyword(keyword.lstrip("1234567890.-# ").strip())
            for keyword in new_keywords if keyword.strip()
        ]

        # Проверка уникальности и запись в базу данных
        for keyword in set(unique_keywords):  # Используем set для удаления дубликатов
            save_keyword_to_db(keyword)
        
        # Обновляем конфигурацию config.json
        config["criteria"]["keywords"] = list(set(unique_keywords))  # Уникальные слова для записи в config.json
        write_config(config)  # Записываем обновленный config.json

        logging.info(f"Generated and saved {len(unique_keywords)} new keywords.")
    except Exception as e:
        logging.error(f"Error in generate_new_keywords_dynamic: {e}")

def clean_keyword(keyword):
    """
    Удаляет спецсимволы, номера и хэштеги из ключевых слов.
    """
    try:
        # Убираем символы #, номера и лишние пробелы
        cleaned = "".join(char for char in keyword if char.isalnum() or char.isspace()).strip()
        return cleaned.lower()
    except Exception as e:
        logging.error(f"Error in clean_keyword: {e}")
        return ""

def analyze_ideas_keywords():
    """
    Анализирует ключевые слова в идеях для выявления популярных тем.
    """
    try:
        query = "SELECT idea FROM ideas"
        ideas = execute_db_query(query)

        keyword_count = {}
        for idea, in ideas:
            words = idea.lower().split()
            for word in words:
                keyword_count[word] = keyword_count.get(word, 0) + 1

        sorted_keywords = sorted(keyword_count.items(), key=lambda x: x[1], reverse=True)

        logging.info("Top Keywords in Ideas:")
        for keyword, count in sorted_keywords[:10]:
            logging.info(f"{keyword}: {count}")
    except Exception as e:
        logging.error(f"Error analyzing ideas: {e}")

def retweet_task(session):
    """
    Выполняет ретвит постов, которые еще не были ретвитнуты, с учетом ограничения количества ретвитов за цикл.
    """
    try:
        # Получение максимального количества ретвитов за цикл из конфигурации
        max_retweets = config.get("schedule", {}).get("max_retweets_per_cycle", 5)
        retweets_posted = 0

        # Извлечение твитов для ретвита
        query = """
            SELECT id, content
            FROM tweets
            WHERE id NOT IN (
                SELECT tweet_id FROM retweeted_posts
            )
            AND is_published = 1
            LIMIT ?
        """
        tweets_to_retweet = execute_db_query(query, (max_retweets,))

        for tweet_id, content in tweets_to_retweet:
            if retweets_posted >= max_retweets:
                break

            # Проверка на дублирование
            duplicate_check_query = "SELECT COUNT(*) FROM retweeted_posts WHERE tweet_id = ?"
            duplicate_count = execute_db_query(duplicate_check_query, (tweet_id,))[0][0]
            if duplicate_count > 0:
                logging.warning(f"Duplicate retweet detected for tweet ID {tweet_id}. Skipping.")
                continue

            try:
                # URL для ретвита
                url = f"https://api.twitter.com/2/tweets/{tweet_id}/retweets"

                # Отправка запроса на ретвит
                response = send_twitter_request(session, url, method="POST")

                if response and "data" in response:
                    # Успешный ретвит
                    insert_query = """
                        INSERT OR IGNORE INTO retweeted_posts (tweet_id, content, created_at)
                        VALUES (?, ?, CURRENT_TIMESTAMP)
                    """
                    execute_db_query(insert_query, (tweet_id, content), commit=True)

                    # Логирование успешного ретвита
                    retweets_posted += 1
                    logging.info(f"Successfully retweeted tweet ID {tweet_id}: {content}")
                else:
                    raise Exception("Empty response from Twitter API.")
            except requests.exceptions.HTTPError as http_err:
                # Если ошибка 404, помечаем твит как недоступный
                if "404" in str(http_err):
                    logging.warning(f"Tweet ID {tweet_id} not found. Marking as unavailable.")
                    status_query = "UPDATE tweets SET metrics_updated = 2 WHERE id = ?"
                    execute_db_query(status_query, (tweet_id,), commit=True)
                else:
                    logging.warning(f"HTTP error for tweet ID {tweet_id}: {http_err}")
            except Exception as e:
                # Логирование общей ошибки
                logging.warning(f"Failed to retweet tweet ID {tweet_id}: {e}")
                continue  # Переход к следующему твиту

        logging.info(f"Total retweets posted this cycle: {retweets_posted}")
    except Exception as e:
        logging.error(f"Error in retweet_task: {e}")

def get_original_tweet(session, tweet_id):
    """
    Получает исходный твит, если указанный твит является ответом на другой твит.

    Args:
        session: OAuth1Session объект.
        tweet_id (str): ID твита.

    Returns:
        dict: Данные исходного твита (id, text) или данные текущего твита, если он не является ответом.
    """
    try:
        # Получаем данные о текущем твите
        url = f"https://api.twitter.com/2/tweets/{tweet_id}"
        params = {"tweet.fields": "in_reply_to_user_id,text"}
        response = send_twitter_request(session, url, method="GET", params=params)

        if response and "data" in response:
            tweet_data = response["data"]

            # Проверяем, является ли твит ответом
            if "in_reply_to_user_id" in tweet_data:
                # Получаем ID исходного твита
                original_tweet_id = tweet_data.get("in_reply_to_status_id")
                if original_tweet_id:
                    original_url = f"https://api.twitter.com/2/tweets/{original_tweet_id}"
                    original_response = send_twitter_request(session, original_url, method="GET")

                    if original_response and "data" in original_response:
                        return {
                            "id": original_response["data"]["id"],
                            "text": original_response["data"]["text"]
                        }
            # Если это не ответ, возвращаем текущий твит
            return {"id": tweet_data["id"], "text": tweet_data["text"]}
        else:
            logging.error(f"Failed to retrieve tweet data for ID {tweet_id}")
            return None
    except Exception as e:
        logging.error(f"Error retrieving original tweet for ID {tweet_id}: {e}")
        return None

def generate_short_comment(tweet_text):
    """
    Генерирует короткий комментарий к указанному твиту.
    """
    try:
        prompt = f"Generate a short, relevant comment (30-50 characters) based on the following tweet: {tweet_text}"
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a bot that writes concise and relevant comments for tweets."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7
        )
        comment = response["choices"][0]["message"]["content"].strip()
        if 30 <= len(comment) <= 50:
            return comment
        else:
            logging.warning(f"Generated comment is not within the desired length. Retrying...")
            return generate_short_comment(tweet_text)
    except Exception as e:
        logging.error(f"Error generating comment text: {e}")
        return "Engaging tweet! Great thoughts!"  # Фоллбэк-значение

def find_popular_accounts(session):
    """
    Находит популярные аккаунты, соответствующие тематике, и сохраняет их в базу данных.
    """
    try:
        keywords = config["criteria"]["keywords"]
        min_followers = config["criteria"]["min_followers"]
        max_followers = config["criteria"]["max_followers"]

        for keyword in keywords:
            url = "https://api.twitter.com/2/users/by/username"
            params = {
                "query": keyword,
                "user.fields": "public_metrics,description",
                "max_results": 50
            }
            response = send_twitter_request(session, url, method="GET", params=params)

            if response and "data" in response:
                for user in response["data"]:
                    followers_count = user["public_metrics"]["followers_count"]
                    if min_followers <= followers_count <= max_followers:
                        insert_query = """
                            INSERT OR IGNORE INTO popular_accounts (id, username, followers_count, description)
                            VALUES (?, ?, ?, ?)
                        """
                        execute_db_query(insert_query, (user["id"], user["username"], followers_count, user["description"]), commit=True)

        logging.info("Popular accounts successfully saved to the database.")
    except Exception as e:
        logging.error(f"Error finding popular accounts: {e}")

def collect_tweets_from_accounts(session):
    """
    Собирает твиты популярных аккаунтов и их метрики.
    """
    try:
        query = "SELECT id FROM popular_accounts"
        accounts = execute_db_query(query)

        for account_id, in accounts:
            url = f"https://api.twitter.com/2/users/{account_id}/tweets"
            params = {"tweet.fields": "public_metrics,created_at", "max_results": 5}
            response = send_twitter_request(session, url, method="GET", params=params)

            if response and "data" in response:
                for tweet in response["data"]:
                    insert_query = """
                        INSERT OR IGNORE INTO popular_tweets (tweet_id, content, likes, retweets, comments, created_at, author_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """
                    execute_db_query(insert_query, (
                        tweet["id"], tweet["text"], tweet["public_metrics"]["like_count"],
                        tweet["public_metrics"]["retweet_count"], tweet["public_metrics"]["reply_count"],
                        tweet["created_at"], account_id), commit=True)

        logging.info("Collected tweets from popular accounts successfully.")
    except Exception as e:
        logging.error(f"Error collecting tweets from accounts: {e}")

def analyze_popular_tweets():
    """
    Анализирует успешные твиты и возвращает рекомендации по темам и ключевым словам.
    """
    try:
        query = """
            SELECT content, likes, retweets, comments
            FROM popular_tweets
            WHERE likes > 100 OR retweets > 50
            ORDER BY likes DESC, retweets DESC, comments DESC
            LIMIT 20
        """
        successful_tweets = execute_db_query(query)

        if not successful_tweets:
            logging.warning("No successful tweets found for analysis.")
            return []

        # Формируем контекст для анализа
        tweet_texts = [content for content, _, _, _ in successful_tweets]
        context = "\n".join(tweet_texts)

        # Используем GPT-4 для анализа
        prompt = (
            f"Based on the following successful tweets, provide recommendations for improving "
            f"Twitter posts related to cryptocurrency, financial independence, and self-development:\n\n"
            f"{context}\n\n"
            f"Suggest new topics, styles, and keywords."
        )
        recommendations = generate_chat_completion(prompt)
        if recommendations:
            logging.info(f"Recommendations generated: {recommendations}")
            return recommendations.split("\n")
        else:
            logging.warning("No recommendations generated.")
            return []
    except Exception as e:
        logging.error(f"Error analyzing popular tweets: {e}")
        return []

def get_recommendations():
    """
    Генерирует рекомендации на основе анализа популярных твитов.
    """
    try:
        recommendations = analyze_popular_tweets()
        if not recommendations:
            logging.warning("No recommendations generated.")
        return recommendations
    except Exception as e:
        logging.error(f"Error generating recommendations: {e}")
        return []

def transform_recommendations_to_templates(recommendations):
    """
    Преобразует рекомендации в шаблоны для генерации текстов твитов.
    """
    try:
        if not isinstance(recommendations, list):
            logging.error("Recommendations must be a list.")
            return []

        context = "\n".join(recommendations)
        prompt = (
            f"Based on the following recommendations:\n\n"
            f"{context}\n\n"
            f"Generate 5 tweet templates that focus on cryptocurrency, financial independence, and self-development. "
            f"Each template must start with 'Write tweet about ...' and be under 200 characters."
        )

        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a bot that creates concise, inspiring, and effective tweet templates."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7
        )

        templates = response["choices"][0]["message"]["content"].strip().split("\n")
        templates = [
            clean_unicode(template.strip().lstrip("1234567890.- ").strip())
            for template in templates if template.startswith("Write tweet about")
        ]

        logging.info(f"Generated {len(templates)} templates from recommendations.")
        return templates
    except Exception as e:
        logging.error(f"Error transforming recommendations to templates: {e}")
        return []

def update_config_with_recommendations(recommendations):
    """
    Обновляет конфигурацию ключевых слов и шаблонов твитов на основе рекомендаций.
    """
    try:
        new_templates = transform_recommendations_to_templates(recommendations)

        if not new_templates:
            logging.warning("No templates generated from recommendations. Skipping config update.")
            return

        existing_templates = set(
            config["prompts"]["general_tweets"] + config["prompts"]["promotional_tweets"]
        )
        unique_templates = [template for template in new_templates if template not in existing_templates]

        if not unique_templates:
            logging.info("All generated templates are already in the configuration. No updates needed.")
            return

        for template in unique_templates:
            config["prompts"]["general_tweets"].append(template)
            save_template_to_db(template, "general")

        write_config(config)
        logging.info("Configuration successfully updated with new templates.")
    except Exception as e:
        logging.error(f"Error updating configuration with recommendations: {e}")

def follow(session):
    """
    Выполняет подписку на популярные аккаунты, исключая повторения.
    """
    try:
        max_follows = config["schedule"].get("max_follows_per_cycle", 5)
        follows_made = 0

        query = """
            SELECT account_id, username
            FROM popular_accounts
            WHERE account_id NOT IN (
                SELECT account_id FROM followed_accounts
            )
            LIMIT ?
        """
        accounts_to_follow = execute_db_query(query, (max_follows,))

        for account_id, username in accounts_to_follow:
            if follows_made >= max_follows:
                break

            try:
                url = f"https://api.twitter.com/2/users/{config['twitter']['user_id']}/following"
                data = {"target_user_id": account_id}
                response = send_twitter_request(session, url, method="POST", data=data)

                if response and "data" in response:
                    insert_query = """
                        INSERT OR IGNORE INTO followed_accounts (account_id, account_name, followed_at)
                        VALUES (?, ?, CURRENT_TIMESTAMP)
                    """
                    execute_db_query(insert_query, (account_id, username), commit=True)
                    follows_made += 1
                    logging.info(f"Successfully followed account: {username} (ID: {account_id})")
            except Exception as e:
                logging.error(f"Error following account {username} (ID: {account_id}): {e}")

        logging.info(f"Total follows made this cycle: {follows_made}")
    except Exception as e:
        logging.error(f"Error in follow function: {e}")

def check_rate_limit(response):
    """
    Проверяет лимит запросов к API.
    """
    try:
        remaining = int(response.headers.get("x-rate-limit-remaining", -1))
        reset_time = int(response.headers.get("x-rate-limit-reset", time.time()))
        current_time = int(time.time())

        if remaining == -1 or reset_time == time.time():
            logging.warning("Rate limit headers not found or invalid. Skipping rate limit check.")
            return

        if remaining == 0:
            wait_time = reset_time - current_time
            logging.warning(f"Rate limit reached. Sleeping for {wait_time} seconds.")
            time.sleep(max(wait_time, 1))
        else:
            logging.info(f"Rate limit remaining: {remaining}. Reset at {datetime.fromtimestamp(reset_time)}.")
    except Exception as e:
        logging.error(f"Error in rate limit check: {e}")

def safe_task(task):
    """
    Оборачивает задачу для безопасного выполнения с обработкой исключений.
    """
    def wrapper():
        try:
            task()
        except Exception as e:
            func_name = task.func.__name__ if isinstance(task, functools.partial) else task.__name__
            logging.error(f"Task {func_name} failed: {e}")
    return wrapper

def backup_database():
    """
    Создаёт резервную копию базы данных.
    """
    try:
        shutil.copy("bot_data.db", "bot_data_backup.db")
        logging.info("Database backup created successfully.")
    except Exception as e:
        logging.error(f"Error creating database backup: {e}")

def generate_daily_report():
    """
    Генерирует ежедневный отчёт о выполненных задачах.
    """
    try:
        with open("daily_report.log", "w") as report_file:
            report_file.write("Daily Report of Completed Tasks:\n")
            # Логика для извлечения данных о задачах из логов
        logging.info("Daily report generated successfully.")
    except Exception as e:
        logging.error(f"Error generating daily report: {e}")

def setup_tasks(session):
    """
    Настраивает задачи с использованием конфигурации, привязывает их к планировщику schedule.
    """
    schedule.every(config["schedule"]["generate_tweet_interval"]).seconds.do(
        safe_task(generate_tweet)
    )
    schedule.every(config["schedule"]["post_tweet_interval"]).seconds.do(
        safe_task(partial(post_unpublished_tweet, session))
    )
    schedule.every(5).minutes.do(safe_task(reload_config))
    schedule.every(config["schedule"]["generate_templates_interval"]).seconds.do(
        safe_task(generate_new_templates)
    )
    schedule.every(config["schedule"]["analyze_successful_tweets_interval"]).seconds.do(
        safe_task(analyze_successful_tweets)
    )
    schedule.every(config["schedule"]["generate_keywords_interval"]).seconds.do(
        safe_task(partial(generate_new_keywords_dynamic, session))
    )
    schedule.every(config["schedule"]["analyze_keywords_interval"]).seconds.do(
        safe_task(analyze_keyword_performance)
    )
    schedule.every(config["schedule"]["generate_ideas_interval"]).seconds.do(
        safe_task(generate_ideas)
    )
    schedule.every(config["schedule"]["update_metrics_interval"]).seconds.do(
        safe_task(partial(update_tweet_metrics_in_db, session))
    )
    schedule.every(config["schedule"]["comment_on_tweets_interval"]).seconds.do(
        safe_task(partial(comment_on_tweets, session))
    )
    schedule.every(config["schedule"]["analyze_ideas_interval"]).seconds.do(
        safe_task(analyze_ideas_keywords)
    )
    schedule.every(config["schedule"]["analyze_performance_interval"]).seconds.do(
        safe_task(partial(analyze_tweet_performance, session, limit=15))
    )
    schedule.every().day.at("00:00").do(safe_task(export_ideas_to_file))
    schedule.every(6).hours.do(safe_task(print_top_tweets))
    schedule.every().day.at("01:00").do(safe_task(backup_database))
    schedule.every().day.at("23:59").do(safe_task(generate_daily_report))
    schedule.every(config["schedule"]["like_relevant_tweets_interval"]).seconds.do(
        safe_task(partial(like_relevant_tweets, session))
    )
    schedule.every(config["schedule"]["like_commented_tweets_interval"]).seconds.do(
        safe_task(partial(like_commented_tweets, session))
    )
    schedule.every(config["schedule"]["retweet_task_interval"]).seconds.do(
        safe_task(partial(retweet_task, session))
    )
    schedule.every().day.at("02:00").do(safe_task(analyze_keyword_performance))
    schedule.every(24).hours.do(safe_task(partial(find_popular_accounts, session)))
    schedule.every(12).hours.do(safe_task(partial(collect_tweets_from_accounts, session)))
    schedule.every(24).hours.do(safe_task(analyze_popular_tweets))
    schedule.every(24).hours.do(safe_task(update_config_with_recommendations))
    schedule.every(config["schedule"]["transform_recommendations_interval"]).seconds.do(
        safe_task(partial(transform_recommendations_to_templates, recommendations=get_recommendations()))
    )
    schedule.every(config["schedule"].get("follow_accounts_interval", 3600)).seconds.do(
        safe_task(partial(follow, session))
    )
    logging.info("All tasks have been successfully scheduled.")

def manage_tasks(session):
    """
    Запускает задачи с использованием отдельного потока.
    """
    logging.info("Starting manage_tasks()")
    setup_tasks(session)

    def run_schedule():
        logging.info("Running scheduled tasks loop")
        while True:
            schedule.run_pending()
            time.sleep(1)

    thread = Thread(target=run_schedule)
    thread.start()

    try:
        thread.join()
    except KeyboardInterrupt:
        logging.info("Bot stopped by user.")

if __name__ == "__main__":
    set_openai_api_key()  # Устанавливаем API-ключ OpenAI
    
    session = initialize_oauth_session()  # Функция инициализирует OAuth сессию
    if not session or not hasattr(session, 'get'):  # Проверка корректности сессии
        logging.error("Failed to initialize Twitter session. Exiting program.")
        exit(1)
    
    execute_db_query("DROP TABLE IF EXISTS popular_accounts", commit=True)
    setup_database()

    setup_database()  # Настраиваем базу данных
    manage_tasks(session)  # Запускаем управление задачами
