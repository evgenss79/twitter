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
import os
import aiohttp
import asyncio
import aiosqlite
from asyncio import Lock
import aiohttp
from cachetools import TTLCache


# Настройка ротации логов
log_file = "bot.log"

# Инициализация блокировки для базы данных
db_lock = Lock()

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=3)  # 5 MB на файл, 3 резервных копии
    ],
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Logging is set up. Log file: %s", log_file)


# Загружаем конфигурацию
def load_config():
    config_path = 'config.json'
    if not os.path.exists(config_path):
        logging.error(f"Config file not found: {config_path}")
        return {}
    try:
        with open(config_path, 'r') as config_file:
            return json.load(config_file)
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing config file: {e}")
        return {}
    except Exception as e:
        logging.error(f"Error loading config: {e}")
        return {}

def reload_config():
    """
    Перезагружает файл конфигурации и обновляет глобальный объект config.
    """
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

# Инициализация OAuth1Session
def initialize_oauth_session():
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

# Унифицированная отправка запросов
async def send_twitter_request(method, url, **kwargs):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.request(method, url, **kwargs) as response:
                if response.status == 429:  # Rate limit exceeded
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logging.warning(f"Rate limit exceeded. Retrying after {retry_after} seconds.")
                    await asyncio.sleep(retry_after)
                    return await send_twitter_request(method, url, **kwargs)
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientResponseError as e:
            logging.error(f"Twitter API request failed: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error in Twitter API request: {e}")
            return None

def setup_database():
    """
    Создаёт базу данных для хранения твитов, шаблонов, ключевых слов и идей.
    """
    try:
        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()

            # Таблица для твитов
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tweets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    content TEXT UNIQUE,  -- Уникальный контент твита
                    likes INTEGER DEFAULT 0,
                    retweets INTEGER DEFAULT 0,
                    comments INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Таблица для лайкнутых твитов
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS liked_tweets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tweet_id TEXT UNIQUE,
                    content TEXT UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)            

            # Таблица для шаблонов
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS templates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    content TEXT UNIQUE,  -- Уникальный шаблон
                    type TEXT,  -- promotional или general
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Таблица для ключевых слов
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS keywords (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    keyword TEXT UNIQUE,  -- Уникальное ключевое слово
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Таблица для идей
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ideas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    idea TEXT UNIQUE,  -- Уникальная идея
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Таблица для ретвитов
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS retweeted_posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tweet_id TEXT UNIQUE,
                    content TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            conn.commit()
            logging.info("Database setup completed successfully.")
    except Exception as e:
        logging.error(f"Error setting up database: {e}")

async def execute_db_query(query, params=(), fetch=False, commit=False):
    async with db_lock:
        try:
            async with aiosqlite.connect('twitter_bot.db') as conn:
                async with conn.execute(query, params) as cursor:
                    if commit:
                        await conn.commit()
                    if fetch:
                        return await cursor.fetchall()
        except aiosqlite.Error as e:
            logging.error(f"Database error: {e}")
            return None

def get_unpublished_tweet():
    """
    Извлекает первый непубликованный твит из базы данных.

    Returns:
        str: Текст твита или None, если таких твитов нет.
    """
    try:
        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, content
                FROM tweets
                WHERE is_published = 0
                ORDER BY created_at ASC
                LIMIT 1
            """)
            result = cursor.fetchone()
            if result:
                tweet_id, content = result
                return tweet_id, content
            else:
                logging.info("No unpublished tweets found.")
                return None, None
    except Exception as e:
        logging.error(f"Error fetching unpublished tweet: {e}")
        return None, None

def mark_tweet_as_published(tweet_id):
    """
    Обновляет статус твита как опубликованного в рамках транзакции.
    
    Args:
        tweet_id (int): ID твита.
    """
    try:
        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()
            conn.execute("BEGIN")  # Начинаем транзакцию
            cursor.execute("""
                UPDATE tweets
                SET is_published = 1
                WHERE id = ?
            """, (tweet_id,))
            conn.commit()  # Фиксируем изменения
            logging.info(f"Marked tweet ID {tweet_id} as published.")
    except Exception as e:
        conn.rollback()  # Откат транзакции в случае ошибки
        logging.error(f"Error marking tweet as published: {e}")

def is_already_published(tweet_id):
    """
    Проверяет, опубликован ли твит.
    
    Args:
        tweet_id (int): ID твита.

    Returns:
        bool: True, если твит уже опубликован.
    """
    try:
        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT is_published FROM tweets WHERE id = ?
            """, (tweet_id,))
            result = cursor.fetchone()
            return result[0] == 1 if result else False
    except Exception as e:
        logging.error(f"Error checking if tweet is published: {e}")
        return True  # Безопасный возврат значения

def post_unpublished_tweet(session):
    """
    Извлекает и публикует первый непубликованный твит из базы данных.
    """
    try:
        tweet_id, tweet_content = get_unpublished_tweet()
        if tweet_content:
            logging.info(f"Attempting to post tweet: {tweet_content}")
            if is_already_published(tweet_id):  # Проверка
                logging.warning(f"Tweet ID {tweet_id} is already marked as published. Skipping.")
                return
            # Публикуем твит
            tweet_result = post_tweet(session, tweet_content)
            
            if tweet_result:  # Если публикация прошла успешно
                mark_tweet_as_published(tweet_id)
                logging.info(f"Successfully posted unpublished tweet: {tweet_content}")
            else:
                logging.error(f"Failed to post tweet: {tweet_content}")
        else:
            logging.info("No unpublished tweets to post.")
    except Exception as e:
        logging.error(f"Error in post_unpublished_tweet: {e}")

def generate_chat_completion(prompt, model="gpt-4", max_tokens=150, temperature=0.7):
    """
    Генерирует завершение чата с использованием OpenAI ChatCompletion API.

    Args:
        prompt (str): Текст подсказки для модели.
        model (str): Используемая модель (например, "gpt-4").
        max_tokens (int): Максимальное количество токенов в ответе.
        temperature (float): Степень случайности.

    Returns:
        str: Сгенерированный текст ответа.
    """
    try:
        response = openai.ChatCompletion.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            temperature=temperature
        )
        logging.info("Chat completion successfully generated.")
        return response["choices"][0]["message"]["content"].strip()
    except openai.error.OpenAIError as e:
        logging.error(f"Error generating chat completion: {e}")
        return None

def save_tweet_to_db(content):
    """
    Сохраняет сгенерированный твит в базу данных.
    """
    try:
        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR IGNORE INTO tweets (content)
                VALUES (?)
            """, (content,))
            conn.commit()
            logging.info(f"Tweet saved to database: {content}")
    except Exception as e:
        logging.error(f"Error saving tweet to database: {e}")

def save_template_to_db(content, template_type):
    """
    Сохраняет шаблон в базу данных.
    """
    try:
        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR IGNORE INTO templates (content, type)
                VALUES (?, ?)
            """, (content, template_type))
            conn.commit()
            logging.info(f"Template saved to database: {content}")
    except Exception as e:
        logging.error(f"Error saving template to database: {e}")

def save_keyword_to_db(keyword):
    """
    Сохраняет ключевое слово в базу данных.
    """
    try:
        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR IGNORE INTO keywords (keyword)
                VALUES (?)
            """, (keyword,))
            conn.commit()
            logging.info(f"Keyword saved to database: {keyword}")
    except Exception as e:
        logging.error(f"Error saving keyword to database: {e}")

def save_idea_to_db(idea):
    """
    Сохраняет идею в базу данных.
    """
    try:
        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR IGNORE INTO ideas (idea)
                VALUES (?)
            """, (idea,))
            conn.commit()
            logging.info(f"Idea saved to database: {idea}")
    except Exception as e:
        logging.error(f"Error saving idea to database: {e}")

def get_top_tweets(limit=5):
    """
    Извлекает топовые твиты по лайкам и ретвитам из базы данных.
    """
    try:
        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT content, likes, retweets
                FROM tweets
                ORDER BY likes DESC, retweets DESC
                LIMIT ?
            """, (limit,))
            top_tweets = cursor.fetchall()
            logging.info(f"Fetched top {limit} tweets from database.")
            return top_tweets
    except Exception as e:
        logging.error(f"Error fetching top tweets: {e}")
        return []

def update_tweet_metrics_in_db(session):
    """
    Обновляет метрики твитов в базе данных.
    """
    try:
        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM tweets WHERE likes = 0 AND retweets = 0 AND comments = 0")
            tweets = cursor.fetchall()

            for tweet_id, in tweets:
                metrics = fetch_tweet_metrics(session, tweet_id)  # Передаем session и tweet_id
                if metrics:
                    cursor.execute("""
                        UPDATE tweets
                        SET likes = ?, retweets = ?, comments = ?
                        WHERE id = ?
                    """, (metrics["likes"], metrics["retweets"], metrics["comments"], tweet_id))
            
            conn.commit()
            logging.info("Tweet metrics updated successfully.")
    except Exception as e:
        logging.error(f"Error updating tweet metrics: {e}")

def export_ideas_to_file(filename="ideas_export.txt"):
    """
    Экспортирует идеи из базы данных в текстовый файл.
    """
    try:
        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT idea, created_at
                FROM ideas
                ORDER BY created_at DESC
            """)
            ideas = cursor.fetchall()

        with open(filename, "w") as file:
            for idea, created_at in ideas:
                file.write(f"{idea} (Generated on: {created_at})\n")

        logging.info(f"Ideas exported to {filename} successfully.")
    except Exception as e:
        logging.error(f"Error exporting ideas: {e}")

def is_similar(new_tweet, existing_tweets, threshold=0.8):
    """
    Проверяет, схож ли новый твит с существующими твитами.
    
    Args:
        new_tweet (str): Новый текст твита.
        existing_tweets (list): Список существующих твитов.
        threshold (float): Порог схожести (0-1).

    Returns:
        bool: True, если схожесть превышает порог, иначе False.
    """
    for tweet in existing_tweets:
        similarity = SequenceMatcher(None, new_tweet, tweet).ratio()
        if similarity >= threshold:
            return True
    return False

def generate_tweet():
    """
    Генерация твита с использованием OpenAI API с проверкой на однотипность и тавтологию.
    
    Returns:
        str: Сгенерированный твит или None в случае ошибки.
    """
    try:
        # Получаем максимальную длину твита из конфигурации
        max_length = config.get("generation", {}).get("max_tweet_length", 280)

        # Получаем последние опубликованные твиты из базы данных
        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT content FROM tweets ORDER BY created_at DESC LIMIT 20")
            existing_tweets = [row[0] for row in cursor.fetchall()]

        while True:  # Бесконечный цикл генерации до получения уникального твита
            # Выбираем случайную подсказку
            category = random.choice(["promotional_tweets", "general_tweets"])
            prompt = random.choice(config["prompts"][category])

            # Добавляем случайные хэштеги
            hashtags = random.sample(config["hashtags"], 2)
            hashtags_str = " ".join(hashtags)

            # Формируем запрос
            full_prompt = (
                f"{prompt} Ensure the output is concise and does not exceed {max_length - len(hashtags_str) - 1} characters, "
                f"including these hashtags: {hashtags_str}."
            )

            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are a bot that writes engaging and creative tweets."},
                    {"role": "user", "content": full_prompt}
                ],
                temperature=0.7
            )
            new_tweet = response["choices"][0]["message"]["content"].strip()

            # Проверяем на однотипность и тавтологию
            if len(new_tweet) <= max_length and not is_similar(new_tweet, existing_tweets):
                # Сохраняем твит в базу данных и возвращаем его
                save_tweet_to_db(new_tweet)
                logging.info(f"Generated unique tweet: {new_tweet}")
                return new_tweet
            else:
                logging.warning("Generated tweet is either too long or too similar to existing ones. Retrying...")

            # Добавляем задержку между попытками для снижения нагрузки на API
            time.sleep(1)

    except Exception as e:
        logging.error(f"Error generating tweet: {e}")
        return None

# Публикация твита
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

# Поиск твитов
def search_tweets(session):
    """
    Выполняет поиск твитов по ключевым словам из конфигурации, исключает дубликаты,
    учитывает лимит из config.json и сохраняет уникальные результаты в базу данных.

    Возвращает:
        list: Список найденных твитов.
    """

    # Загружаем параметры из config.json
    try:
        max_results = config["search"]["max_results"]
        total_limit = config["search"]["total_limit"]
    except KeyError as e:
        logging.error(f"Не найден ключ конфигурации: {e}. Проверьте 'max_results' и 'total_limit' в 'search'.")
        return []

    keywords = random.sample(config["criteria"]["keywords"], 5)
    query = " OR ".join(keywords)

    url = "https://api.twitter.com/2/tweets/search/recent"
    params = {
        "query": query,
        "max_results": max_results,
        "tweet.fields": "id,text,author_id",
    }

    tweets = []
    unique_tweet_ids = set()  # Для проверки уникальности твитов
    total_count = 0

    try:
        # Проверяем существующие tweet_id в базе данных, чтобы избежать дубликатов
        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT tweet_id FROM tweets")
            existing_ids = {row[0] for row in cursor.fetchall()}

        while total_count < total_limit:
            response = send_twitter_request(session, url, method="GET", params=params)
                
            if response is None:
                logging.error("Failed to search tweets: Response is None")
                break
            
            if response and "data" in response:
                for tweet in response["data"]:
                    tweet_id = tweet["id"]

                    # Пропускаем, если tweet_id уже существует
                    if tweet_id in existing_ids or tweet_id in unique_tweet_ids:
                        continue

                    tweets.append(tweet)
                    unique_tweet_ids.add(tweet_id)
                    total_count += 1

                    # Прерываем цикл, если достигнут лимит
                    if total_count >= total_limit:
                        break

                logging.info(f"Найдено {len(response['data'])} твитов. Всего собрано: {total_count}")

                # Проверяем наличие токена для следующей страницы
                next_token = response.get("meta", {}).get("next_token")
                if not next_token:
                    logging.info("Нет больше страниц для обработки.")
                    break  # Нет больше страниц
                params["next_token"] = next_token  # Устанавливаем токен для пагинации
                logging.info(f"Переход к следующей странице с токеном: {next_token}")

            else:
                logging.warning("Твиты не найдены или ответ пуст.")
                break

        # Сохраняем новые твиты в базу данных
        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()
            for tweet in tweets:
                try:
                    cursor.execute("""
                        INSERT INTO tweets (tweet_id, content, created_at)
                        VALUES (?, ?, ?)
                    """, (tweet["id"], tweet["text"], datetime.utcnow()))
                except sqlite3.IntegrityError:
                    # Пропускаем дубликаты
                    continue
            conn.commit()

    except Exception as e:
        logging.error(f"Ошибка при поиске твитов: {e}")

    return tweets


# Комментирование твита
def is_already_commented(tweet_id):
    """
    Проверяет, был ли твит уже прокомментирован.

    Args:
        tweet_id (str): ID твита.

    Returns:
        bool: True, если твит уже прокомментирован.
    """
    try:
        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM comments WHERE tweet_id = ?", (tweet_id,))
            return cursor.fetchone()[0] > 0
    except sqlite3.OperationalError as e:
        logging.error(f"Error checking comment status: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error in is_already_commented: {e}")
        return False

def save_comment_to_db(tweet_id, comment_id):
    """
    Сохраняет информацию о комментарии в базу данных.

    Args:
        tweet_id (str): ID твита.
        comment_id (str): ID комментария.
    """
    try:
        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO comments (tweet_id, comment_id, created_at)
                VALUES (?, ?, ?)
            """, (tweet_id, comment_id, datetime.utcnow()))
            conn.commit()
            logging.info(f"Comment saved to database: {comment_id}")
    except sqlite3.OperationalError as e:
        logging.error(f"Database error while saving comment: {e}")
    except Exception as e:
        logging.error(f"Unexpected error in save_comment_to_db: {e}")


    """
    Публикует комментарий к указанному твиту, проверяет на дубликаты
    и сохраняет результат в базу данных.

    Args:
        tweet_id (str): ID твита, к которому добавляется комментарий.
        comment_text (str): Текст комментария.

    Returns:
        str: ID комментария или None в случае ошибки.
    """
    try:
        # Проверяем, был ли твит уже прокомментирован
        if is_already_commented(tweet_id):
            logging.info(f"Tweet with ID {tweet_id} has already been commented. Skipping.")
            return None

        # Устанавливаем URL и данные для запроса
        url = "https://api.twitter.com/2/tweets"
        data = {
            "text": comment_text,
            "reply": {"in_reply_to_tweet_id": tweet_id},
        }

        # Отправляем запрос на публикацию комментария
        response = send_twitter_request(session, url, method="POST", data=data)

        if response and "data" in response:
            comment_id = response["data"]["id"]
            logging.info(f"Comment successfully published: {comment_id}")

            # Сохраняем информацию о комментарии в базу данных
            save_comment_to_db(tweet_id, comment_id)
            return comment_id
        else:
            logging.error(f"Failed to publish comment. Response: {response}")
            return None

    except sqlite3.Error as db_error:
        logging.error(f"Database error while saving comment: {db_error}")
        return None

    except requests.exceptions.RequestException as request_error:
        logging.error(f"Error while posting comment to Twitter API: {request_error}")
        return None

    except Exception as e:
        logging.error(f"Unexpected error in post_comment: {e}")
        return None

# Получение метрик твита
def fetch_tweet_metrics(session, tweet_id):
    url = f"https://api.twitter.com/2/tweets/{tweet_id}"
    params = {"tweet.fields": "public_metrics"}
    try:
        response = send_twitter_request(session, url, method="GET", params=params)
        if response is None:
            logging.error(f"Failed to fetch tweet metrics for ID {tweet_id}: Response is None")
            return {}
        if "data" in response:
            metrics = response["data"].get("public_metrics", {})
            logging.info(f"Metrics fetched for tweet ID {tweet_id}: {metrics}")
            return {
                "likes": metrics.get("like_count", 0),
                "retweets": metrics.get("retweet_count", 0),
                "comments": metrics.get("reply_count", 0),
            }
        else:
            logging.warning(f"No metrics found for tweet ID {tweet_id}")
            return {}
    except Exception as e:
        logging.error(f"Error fetching tweet metrics for ID {tweet_id}: {e}")
        return {}

def print_top_tweets():
    """
    Печатает топ-5 твитов по лайкам и ретвитам.
    """
    try:
        conn = sqlite3.connect("bot_data.db")
        cursor = conn.cursor()

        # Получаем топовые твиты
        cursor.execute("""
            SELECT content, likes, retweets, comments
            FROM tweets
            ORDER BY likes DESC, retweets DESC
            LIMIT 5
        """)
        top_tweets = cursor.fetchall()

        if not top_tweets:
            print("\nNo tweets available in the database.")
            return

        print("\nTop Tweets:")
        for idx, (content, likes, retweets, comments) in enumerate(top_tweets, start=1):
            print(f"{idx}. {content}\n   Likes: {likes}, Retweets: {retweets}, Comments: {comments}\n")

        conn.close()
    except Exception as e:
        print(f"Error printing top tweets: {e}")

def analyze_tweet_performance(session):
    """
    Анализирует производительность твитов.
    """
    try:
        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()

            # Получаем все твиты из базы данных
            cursor.execute("SELECT id FROM tweets")
            tweets = cursor.fetchall()

            for tweet_id, in tweets:
                metrics = fetch_tweet_metrics(session, tweet_id)  # Передаем session и tweet_id
                if metrics:
                    cursor.execute("""
                        UPDATE tweets
                        SET likes = ?, retweets = ?, comments = ?
                        WHERE id = ?
                    """, (metrics["likes"], metrics["retweets"], metrics["comments"], tweet_id))
            
            conn.commit()
            logging.info("Tweet performance analyzed successfully.")
    except Exception as e:
        logging.error(f"Error analyzing tweet performance: {e}")

def clean_unicode(text):
    """
    Убирает или преобразует Unicode-символы в читаемый формат.
    """
    return unicodedata.normalize("NFKC", text)

def get_original_tweet(session, tweet_id):
    """
    Получает исходный твит, если указанный твит является ответом на другой твит.
    
    Args:
        session: OAuth1Session объект.
        tweet_id (str): ID твита.

    Returns:
        dict: Данные исходного твита (id, text) или самого твита, если он не является ответом.
    """
    try:
        # Получаем данные о текущем твите
        url = f"https://api.twitter.com/2/tweets/{tweet_id}"
        params = {"tweet.fields": "in_reply_to_user_id,text"}
        response = send_twitter_request(session, url, method="GET", params=params)

        if response and "data" in response:
            tweet_data = response["data"]
            if "in_reply_to_user_id" in tweet_data:  # Проверяем, является ли это ответом
                # Получаем исходный твит
                original_tweet_id = tweet_data["in_reply_to_user_id"]
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

comment_cache = TTLCache(maxsize=100, ttl=3600)  # Кэш на 1 час

async def generate_comment(session, tweet_id):
    if tweet_id in comment_cache:
        return comment_cache[tweet_id]

    try:
        original_tweet = await get_original_tweet(session, tweet_id)
        if not original_tweet:
            return None

        tweet_text = original_tweet["text"]
        comment_category = "general_comments" if len(tweet_text) < 50 else "promotional_comments"
        template = random.choice(config["prompts"][comment_category])

        prompt = f"{template}\n\nTweet: {tweet_text}\n\nComment:"
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.openai.com/v1/engines/text-davinci-002/completions",
                headers={"Authorization": f"Bearer {openai.api_key}"},
                json={"prompt": prompt, "max_tokens": 100, "n": 1, "stop": None, "temperature": 0.7}
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    comment = data['choices'][0]['text'].strip()
                    clean_comment = clean_unicode(comment)
                    comment_cache[tweet_id] = clean_comment
                    return clean_comment
                else:
                    logging.error(f"OpenAI API error: {response.status}")
                    return None
    except Exception as e:
        logging.error(f"Error generating comment: {e}")
        return None

def comment_on_tweets(session):
    """
    Поиск твитов и добавление комментариев с контролем частоты запросов.
    """
    try:
        # Проверяем, что значение max_comments_per_cycle задано в конфигурации
        max_comments = config["schedule"].get("max_comments_per_cycle")
        if max_comments is None or not isinstance(max_comments, int) or max_comments <= 0:
            raise ValueError("The configuration key 'max_comments_per_cycle' is missing, not set, or invalid.")

        comments_posted = 0

        # Поиск твитов
        tweets = search_tweets(session)  # Исправлено

        if not tweets:
            logging.info("No tweets found for commenting.")
            return

        for tweet in tweets:
            if comments_posted >= max_comments:
                break

            tweet_id = tweet["id"]
            logging.info(f"Generating comment for tweet ID {tweet_id}...")
            comment = generate_comment(session, tweet_id)

            if comment:
                post_comment(session, tweet_id, comment)
                comments_posted += 1
                time.sleep(config["schedule"].get("comment_delay", 5))  # Задержка между комментариями

        logging.info(f"Total comments posted: {comments_posted}")
    except ValueError as ve:
        logging.error(f"Configuration error: {ve}")
    except Exception as e:
        logging.error(f"Error during comment on tweets: {e}")

def generate_ideas():
    """
    Генерирует новые идеи для контента или функций.
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
        print("New ideas:")
        for idea in ideas:
            print(f"- {idea.strip()}")
            save_idea_to_db(idea.strip())  # Сохраняем идею в базу данных
        return ideas

    except Exception as e:
        print(f"Error generating ideas: {e}")
        return []

def analyze_ideas_keywords():
    """
    Анализирует ключевые слова в идеях для выявления популярных тем.
    """
    try:
        conn = sqlite3.connect("bot_data.db")
        cursor = conn.cursor()

        cursor.execute("""
            SELECT idea
            FROM ideas
        """)
        ideas = cursor.fetchall()
        conn.close()

        keyword_count = {}
        for idea, in ideas:
            words = idea.lower().split()
            for word in words:
                keyword_count[word] = keyword_count.get(word, 0) + 1

        sorted_keywords = sorted(keyword_count.items(), key=lambda x: x[1], reverse=True)
        print("\nTop Keywords in Ideas:")
        for keyword, count in sorted_keywords[:10]:
            print(f"{keyword}: {count}")

    except Exception as e:
        print(f"Error analyzing ideas: {e}")

def generate_new_templates():
    """
    Генерирует новые шаблоны твитов, учитывая успешные примеры из базы данных.
    """
    config = load_config()

    try:
        # Получаем топовые твиты
        top_tweets = get_top_tweets()

        # Инструкция для GPT: создавать шаблоны на основе успешных твитов
        prompt = (
            f"Based on these successful tweets, generate 3 new motivational templates "
            f"related to cryptocurrency, financial independence, and self-development:\n"
            f"as instructions for generating tweets. Each template should describe the theme or style "
            f"without being a ready tweet. Avoid numbers or bullet points."
        )
        for tweet, likes, retweets in top_tweets:
            prompt += f"- {tweet} (Likes: {likes}, Retweets: {retweets})\n"

        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a bot that creates creative tweet templates."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7
        )

        # Обработка ответа
        new_templates = response["choices"][0]["message"]["content"].strip().split("\n")
        new_templates = [
            clean_unicode(template.strip().lstrip("1234567890.- ").strip("\""))  # Убираем номера, лишние символы, Unicode
            for template in new_templates if template.strip() and "#" not in template  # Исключаем шаблоны с хэштегами
        ]

        # Проверяем уникальность и добавляем в config
        existing_templates = set(config["prompts"]["general_tweets"] + config["prompts"]["promotional_tweets"])
        unique_templates = [template for template in new_templates if template not in existing_templates]

        if unique_templates:
            for template in unique_templates:
                save_template_to_db(template, "general")
                config["prompts"]["general_tweets"].append(template)

            write_config(config)
            print(f"Added {len(unique_templates)} new templates in correct format.")
        else:
            print("No unique templates generated.")

    except Exception as e:
        print(f"Error generating templates: {e}")

def analyze_successful_tweets(session):
    """
    Анализирует успешные твиты и возвращает текст с упором на тематику и риторику.
    """
    try:
        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()

            # Извлекаем успешные твиты на основе метрик
            cursor.execute("""
                SELECT content, likes, retweets, comments
                FROM tweets
                WHERE likes > 100 OR retweets > 50
                ORDER BY likes DESC, retweets DESC, comments DESC
                LIMIT 10
            """)
            successful_tweets = cursor.fetchall()

            if not successful_tweets:
                logging.info("No successful tweets found for analysis.")
                return None

            # Формируем текст для анализа
            tweet_texts = [content for content, _, _, _ in successful_tweets]
            context = "\n".join(tweet_texts)

            logging.info("Successful tweets analyzed for keyword generation.")
            return context

    except Exception as e:
        logging.error(f"Error analyzing successful tweets: {e}")
        return None

def generate_keyword_templates(context):
    """
    Генерирует шаблоны ключевых слов на основании успешных твитов.
    """
    try:
        if not context:
            logging.warning("No context provided for generating keyword templates.")
            return []

        # Формируем запрос для генерации шаблонов
        prompt = (
            f"Based on the following successful tweets, create 5 keyword templates "
            f"that are concise and related to cryptocurrency, self-development, and financial independence:\n\n"
            f"{context}\n\n"
            f"Each template should be general and under 30 characters."
        )

        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a bot that creates creative and effective keyword templates."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7
        )

        templates = response["choices"][0]["message"]["content"].strip().split("\n")
        templates = [
            clean_unicode(template.strip().lstrip("1234567890.- ").strip("\""))  # Убираем номера, лишние символы
            for template in templates if template.strip()
        ]

        logging.info("Generated keyword templates successfully.")
        return templates

    except Exception as e:
        logging.error(f"Error generating keyword templates: {e}")
        return []

def generate_new_keywords_dynamic(session):
    """
    Генерирует новые ключевые фразы на основе шаблонов, извлеченных из успешных твитов.
    """
    config = load_config()

    try:
        # Шаг 1: Анализируем успешные твиты
        context = analyze_successful_tweets(session)
        if not context:
            logging.warning("No successful tweets found, generating default keywords.")
            context = "self-development, cryptocurrency, financial growth, and personal success."

        # Шаг 2: Генерируем шаблоны ключевых слов
        templates = generate_keyword_templates(context, session)
        if not templates:
            logging.warning("No keyword templates generated. Using fallback templates.")
            templates = config.get("fallback_templates", [])

        # Шаг 3: Генерация ключевых слов на основе шаблонов
        new_keywords = []
        for template in templates:
            prompt = (
                f"Using the template '{template}', generate 5 unique and concise keywords "
                f"related to cryptocurrency, financial growth, and self-development."
            )
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are a bot that creates creative and effective keywords."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7
            )
            generated_keywords = response["choices"][0]["message"]["content"].strip().split("\n")
            new_keywords.extend([
                clean_unicode(keyword.strip().lstrip("1234567890.- ").strip("\""))
                for keyword in generated_keywords if keyword.strip()
            ])

        # Шаг 4: Проверка уникальности и добавление в конфигурацию
        existing_keywords = set(config["criteria"]["keywords"])
        unique_keywords = [
            keyword for keyword in new_keywords
            if 3 <= len(keyword) <= 30 and keyword.lower() not in existing_keywords
        ]

        if unique_keywords:
            for keyword in unique_keywords:
                save_keyword_to_db(keyword)
                config["criteria"]["keywords"].append(keyword)

            write_config(config)
            logging.info(f"Added {len(unique_keywords)} new dynamically generated keywords.")
        else:
            logging.info("No unique keywords were generated.")

    except Exception as e:
        logging.error(f"Error generating dynamic keywords: {e}")

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

def like_commented_tweets(session):
    """
    Ставит лайки на твиты, которые бот прокомментировал, и исключает повторные лайки.
    """
    try:
        # Загружаем лимит лайков за цикл из конфигурации
        max_likes = config["schedule"].get("max_likes_per_cycle", 5)
        likes_posted = 0

        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()

            # Извлекаем твиты, которые были прокомментированы, но ещё не лайкнуты
            cursor.execute("""
                SELECT tweet_id, content
                FROM tweets
                WHERE tweet_id NOT IN (
                    SELECT tweet_id FROM liked_tweets
                )
            """)
            tweets_to_like = cursor.fetchall()

        # Перемешиваем порядок твитов для более естественного поведения
        random.shuffle(tweets_to_like)

        for tweet_id, content in tweets_to_like:
            if likes_posted >= max_likes:
                break

            try:
                # Отправляем запрос на лайк твита
                url = f"https://api.twitter.com/2/users/{config['twitter']['user_id']}/likes"
                data = {"tweet_id": tweet_id}
                response = send_twitter_request(session, url, method="POST", data=data)
                if response:
                    logging.info(f"Liked tweet ID {tweet_id}: {content}")

                    # Сохраняем информацию о лайкнутом твите в таблицу liked_tweets
                    with sqlite3.connect("bot_data.db") as conn:
                        cursor = conn.cursor()
                        cursor.execute("""
                            INSERT OR IGNORE INTO liked_tweets (tweet_id, content)
                            VALUES (?, ?)
                        """, (tweet_id, content))
                        conn.commit()

                    likes_posted += 1

            except Exception as e:
                logging.error(f"Failed to like tweet ID {tweet_id}: {e}")

        logging.info(f"Total likes posted on commented tweets: {likes_posted}")
    except Exception as e:
        logging.error(f"Error in like_commented_tweets: {e}")

def like_relevant_tweets(session):
    """
    Ставит лайки на релевантные твиты, найденные по ключевым словам, и исключает повторные лайки.
    """
    try:

        # Загружаем лимит лайков за цикл из конфигурации
        max_likes = config["schedule"].get("max_likes_per_cycle", 5)
        likes_posted = 0

        # Ищем твиты для лайков
        tweets = search_tweets(session)  # Исправлено

        if not tweets:
            logging.info("No tweets found for liking.")
            return

        for tweet in tweets:
            if likes_posted >= max_likes:
                break

            tweet_id = tweet["id"]
            tweet_text = tweet["text"]

            # Проверяем, был ли этот твит уже лайкнут
            with sqlite3.connect("bot_data.db") as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*) FROM liked_tweets WHERE tweet_id = ?
                """, (tweet_id,))
                if cursor.fetchone()[0] > 0:
                    logging.info(f"Tweet ID {tweet_id} is already liked. Skipping.")
                    continue

            try:
                # Отправляем запрос на лайк твита
                url = f"https://api.twitter.com/2/users/{config['twitter']['user_id']}/likes"
                data = {"tweet_id": tweet_id}
                response = send_twitter_request(session, url, method="POST", data=data)
                if response:
                    logging.info(f"Liked tweet ID {tweet_id}: {tweet_text}")

                    # Сохраняем лайкнутый твит в базу данных
                    with sqlite3.connect("bot_data.db") as conn:
                        cursor = conn.cursor()
                        cursor.execute("""
                            INSERT OR IGNORE INTO liked_tweets (tweet_id, content)
                            VALUES (?, ?)
                        """, (tweet_id, tweet_text))
                        conn.commit()

                    likes_posted += 1

            except Exception as e:
                logging.error(f"Failed to like tweet ID {tweet_id}: {e}")

        logging.info(f"Total likes posted on relevant tweets: {likes_posted}")
    except Exception as e:
        logging.error(f"Error in like_relevant_tweets: {e}")

def retweet_task(session):
    """
    Выполняет ретвиты в зависимости от настроек и записывает результаты в базу данных.
    Добавлена проверка релевантности через OpenAI.
    """
    try:
        # Загружаем настройку количества ретвитов за цикл
        max_retweets = config["schedule"].get("max_retweets_per_cycle")
        if max_retweets is None:
            logging.error("Параметр 'max_retweets_per_cycle' не задан в config.json.")
            return

        retweets_posted = 0

        # Проверяем метод выбора (рандомно)
        method = random.choice(["commented", "liked", "search"])

        logging.info(f"Выбран метод ретвита: {method}")

        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()

            if method == "commented":
                # Выбор из прокомментированных твитов
                cursor.execute("""
                    SELECT tweet_id, content FROM tweets
                    WHERE tweet_id NOT IN (SELECT tweet_id FROM retweeted_posts)
                    ORDER BY created_at DESC
                    LIMIT ?
                """, (max_retweets,))
                tweets_to_retweet = cursor.fetchall()

            elif method == "liked":
                # Выбор из лайкнутых твитов
                cursor.execute("""
                    SELECT tweet_id, content FROM liked_tweets
                    WHERE tweet_id NOT IN (SELECT tweet_id FROM retweeted_posts)
                    ORDER BY created_at DESC
                    LIMIT ?
                """, (max_retweets,))
                tweets_to_retweet = cursor.fetchall()

            elif method == "search":
                # Поиск твитов через API
                tweets = search_tweets(session)  
                tweets_to_retweet = [(tweet["id"], tweet["text"]) for tweet in tweets]

            # Обрабатываем выбранные твиты
            for tweet_id, tweet_text in tweets_to_retweet:
                if retweets_posted >= max_retweets:
                    break

                # Проверяем релевантность твита
                is_relevant = is_tweet_relevant(tweet_text)
                if not is_relevant:
                    logging.info(f"Твит не прошёл проверку релевантности и будет пропущен: {tweet_text}")
                    continue

                # Генерация короткого комментария (30-50 символов)
                comment = generate_short_comment(tweet_text)

                try:
                    # Выполняем ретвит с комментарием
                    url = "https://api.twitter.com/2/tweets"
                    data = {
                        "text": comment,
                        "quote_tweet_id": tweet_id
                    }
                    response = send_twitter_request(session, url, method="POST", data=data)
                    if response:
                        logging.info(f"Ретвит опубликован для твита ID {tweet_id} с комментарием: {comment}")

                        # Записываем в базу данных
                        cursor.execute("""
                            INSERT OR IGNORE INTO retweeted_posts (tweet_id, content)
                            VALUES (?, ?)
                        """, (tweet_id, tweet_text))
                        conn.commit()

                        retweets_posted += 1

                except Exception as e:
                    logging.error(f"Ошибка при ретвите твита ID {tweet_id}: {e}")

        logging.info(f"Всего опубликовано ретвитов: {retweets_posted}")
    except Exception as e:
        logging.error(f"Ошибка в retweet_task: {e}")

def generate_short_comment(tweet_text):
    """
    Генерирует релевантный короткий комментарий (30-50 символов) к исходному твиту.
    
    Args:
        tweet_text (str): Текст исходного твита.

    Returns:
        str: Сгенерированный комментарий.
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

def ensure_retweeted_posts_table():
    """
    Проверяет наличие таблицы retweeted_posts и создаёт её при необходимости.
    """
    try:
        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS retweeted_posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tweet_id TEXT UNIQUE,
                    content TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
            logging.info("Ensured 'retweeted_posts' table exists.")
    except Exception as e:
        logging.error(f"Error ensuring retweeted_posts table: {e}")

def fix_liked_tweets_table():
    """
    Убедиться, что таблица liked_tweets имеет правильную структуру.
    """
    try:
        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()
            # Проверяем, есть ли столбец tweet_id
            cursor.execute("PRAGMA table_info(liked_tweets)")
            columns = [row[1] for row in cursor.fetchall()]
            if "tweet_id" not in columns:
                cursor.execute("ALTER TABLE liked_tweets ADD COLUMN tweet_id TEXT UNIQUE")
                conn.commit()
                logging.info("Added 'tweet_id' column to 'liked_tweets' table.")
    except sqlite3.OperationalError as e:
        logging.error(f"Error fixing liked_tweets table: {e}")

def ensure_liked_tweets_table():
    try:
        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(liked_tweets)")
            columns = [row[1] for row in cursor.fetchall()]
            if "tweet_id" not in columns:
                cursor.execute("ALTER TABLE liked_tweets ADD COLUMN tweet_id TEXT UNIQUE")
                conn.commit()
                logging.info("Added 'tweet_id' column to 'liked_tweets' table.")
    except Exception as e:
        logging.error(f"Error ensuring liked_tweets table structure: {e}")

def check_rate_limit(response):
    remaining = int(response.headers.get("x-rate-limit-remaining", -1))
    reset_time = int(response.headers.get("x-rate-limit-reset", time.time()))
    
    if remaining == -1:
        logging.warning("Rate limit headers not found. Skipping rate limit check.")
        return
    
    if remaining == 0:
        wait_time = reset_time - int(time.time())
        logging.warning(f"Rate limit reached. Sleeping for {wait_time} seconds.")
        time.sleep(max(wait_time, 1))

def safe_task(max_retries=3, retry_delay=1):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    logging.error(f"Error in {func.__name__} (attempt {attempt + 1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_delay)
                    else:
                        logging.error(f"Max retries reached for {func.__name__}")
        return wrapper
    return decorator

def backup_database():
    try:
        shutil.copy("bot_data.db", "bot_data_backup.db")
        logging.info("Database backup created successfully.")
    except Exception as e:
        logging.error(f"Error creating database backup: {e}")

def generate_daily_report():
    try:
        with open("daily_report.log", "w") as report_file:
            report_file.write("Daily Report of Completed Tasks:\n")
            # Логика для извлечения данных о задачах из логов
        logging.info("Daily report generated successfully.")
    except Exception as e:
        logging.error(f"Error generating daily report: {e}")

def setup_tasks(session):
    """
    Настраивает задачи с использованием конфигурации, передавая сессию Twitter API в нужные функции.

    Args:
        session: OAuth1Session, инициализированная сессия Twitter API.
    """
    # 1. Генерация твитов
    schedule.every(config["schedule"]["generate_tweet_interval"]).seconds.do(
        safe_task(generate_tweet)
    )

    # 2. Публикация твита
    schedule.every(config["schedule"]["post_tweet_interval"]).seconds.do(
        safe_task(partial(post_unpublished_tweet, session))
    )

    # 3. Перезагрузка конфигурации каждые 5 минут
    schedule.every(5).minutes.do(safe_task(reload_config))

    # 4. Генерация новых шаблонов
    schedule.every(config["schedule"]["generate_templates_interval"]).seconds.do(
        safe_task(generate_new_templates)
    )

    # 5. Анализ успешных твитов перед генерацией ключевых слов
    schedule.every(config["schedule"]["analyze_successful_tweets_interval"]).seconds.do(
        safe_task(partial(analyze_successful_tweets, session))
    )

    # 6. Генерация новых ключевых слов
    schedule.every(config["schedule"]["generate_keywords_interval"]).seconds.do(
        safe_task(partial(generate_new_keywords_dynamic, session))
    )

    # 7. Генерация идей
    schedule.every(config["schedule"]["generate_ideas_interval"]).seconds.do(
        safe_task(generate_ideas)
    )

    # 8. Обновление метрик твитов
    schedule.every(config["schedule"]["update_metrics_interval"]).seconds.do(
        safe_task(partial(update_tweet_metrics_in_db, session))
    )

    # 9. Комментирование твитов
    schedule.every(config["schedule"]["comment_on_tweets_interval"]).seconds.do(
        safe_task(partial(comment_on_tweets, session))
    )

    # 10. Анализ идей по ключевым словам
    schedule.every(config["schedule"]["analyze_ideas_interval"]).seconds.do(
        safe_task(analyze_ideas_keywords)
    )

    # 11. Анализ производительности твитов
    schedule.every(config["schedule"]["analyze_performance_interval"]).seconds.do(
        safe_task(partial(analyze_tweet_performance, session))
    )

    # 12. Экспорт идей в файл (раз в день)
    schedule.every().day.at("00:00").do(safe_task(export_ideas_to_file))

    # 13. Печать топ-5 твитов (каждые 6 часов)
    schedule.every(6).hours.do(safe_task(print_top_tweets))

    # 14. Резервное копирование базы данных (раз в день)
    schedule.every().day.at("01:00").do(safe_task(backup_database))

    # 15. Генерация дневного отчета (раз в день)
    schedule.every().day.at("23:59").do(safe_task(generate_daily_report))

    # 16. Лайки на релевантные твиты
    schedule.every(config["schedule"]["like_relevant_tweets_interval"]).seconds.do(
        safe_task(partial(like_relevant_tweets, session))
    )

    # 17. Лайки для прокомментированных твитов
    schedule.every(config["schedule"]["like_commented_tweets_interval"]).seconds.do(
        safe_task(partial(like_commented_tweets, session))
    )

    # 18. Ретвит
    schedule.every(config["schedule"]["retweet_task_interval"]).seconds.do(
        safe_task(partial(retweet_task, session))
    )

def manage_tasks(session):
    logging.info("Starting manage_tasks()")
    setup_tasks(session)
    
    def run_schedule():
        logging.info("Running scheduled tasks loop")
        while True:
            schedule.run_pending()
            time.sleep(1)
        

    # Запускаем задачи в отдельном потоке
    thread = Thread(target=run_schedule)
    thread.start()

    try:
        thread.join()
    except KeyboardInterrupt:
        logging.info("Bot stopped by user.")

if __name__ == "__main__":
    set_openai_api_key()  # Устанавливаем API-ключ OpenAI
    session = initialize_oauth_session()  # Функция инициализирует её
    if not session or not hasattr(session, 'get'):  # Проверка корректности сессии
        logging.error("Failed to initialize Twitter session. Exiting program.")
        exit(1)
    fix_liked_tweets_table()
    ensure_liked_tweets_table()
    # Инициализация Tweepy API и OAuth1Session
    
    # Убедимся, что таблица существует перед запуском задачи
    ensure_retweeted_posts_table()
    setup_database()  # Настраиваем базу данных
    manage_tasks(session)
