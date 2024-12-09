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
import random


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
        raise

# Унифицированная отправка запросов
def send_twitter_request(session, url, method="GET", params=None, data=None):
    try:
        # Задержка между запросами (например, 1 секунда)
        time.sleep(1)
        
        if method.upper() == "GET":
            response = session.get(url, params=params)
        elif method.upper() == "POST":
            response = session.post(url, json=data)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
        
        check_rate_limit(response)
        
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error during Twitter API request: {e}")
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

            conn.commit()
            logging.info("Database setup completed successfully.")
    except Exception as e:
        logging.error(f"Error setting up database: {e}")

def add_is_published_column():
    try:
        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()
            cursor.execute("""
                ALTER TABLE tweets ADD COLUMN is_published INTEGER DEFAULT 0
            """)
            conn.commit()
            logging.info("Added 'is_published' column to 'tweets' table.")
    except sqlite3.OperationalError as e:
        # Если столбец уже существует, это не ошибка
        if "duplicate column name" in str(e).lower():
            logging.info("'is_published' column already exists.")
        else:
            logging.error(f"Error adding 'is_published' column: {e}")

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

def post_unpublished_tweet():
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
            tweet_result = post_tweet(twitter_session, tweet_content)
            
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

def update_tweet_metrics_in_db():
    """
    Обновляет метрики твитов в базе данных.
    """
    try:
        with sqlite3.connect("bot_data.db") as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM tweets WHERE likes = 0 AND retweets = 0 AND comments = 0")
            tweets = cursor.fetchall()

            for tweet_id, in tweets:
                metrics = fetch_tweet_metrics(twitter_session, tweet_id)  # Передаем session и tweet_id
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

            # Максимальная длина твита
            max_length = config.get("generation", {}).get("max_length", 200)

            # Формируем запрос
            full_prompt = (
                f"{prompt} Ensure the output is concise and does not exceed {max_length} characters, "
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
            if not is_similar(new_tweet, existing_tweets):
                # Сохраняем твит в базу данных и возвращаем его
                save_tweet_to_db(new_tweet)
                logging.info(f"Generated unique tweet: {new_tweet}")
                return new_tweet
            else:
                logging.warning("Generated tweet is too similar to existing ones. Retrying...")

            # Добавляем задержку между попытками для снижения нагрузки на API
            time.sleep(1)

    except Exception as e:
        logging.error(f"Error generating tweet: {e}")
        return None


# Публикация твита
def post_tweet(session, content):
    url = "https://api.twitter.com/2/tweets"
    data = {"text": content}
    headers = {"Content-Type": "application/json"}
    try:
        response = session.post(url, json=data, headers=headers)
        response.raise_for_status()  # Добавьте проверку статуса
        result = response.json()
        if "data" in result and "id" in result["data"]:
            logging.info(f"Tweet published: {result['data']['id']}")
            return result["data"]["id"]
        else:
            logging.error(f"Unexpected response format: {result}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error posting tweet: {e}")
        return None

# Поиск твитов
def search_tweets(session, config, max_results=10):
    """
    Ищет твиты на основе случайных 5 ключевых слов из конфигурации.

    Args:
        session: OAuth1Session объект.
        config: Конфигурация с ключевыми словами.
        max_results: Максимальное количество твитов для возврата.

    Returns:
        list: Список найденных твитов.
    """
    url = "https://api.twitter.com/2/tweets/search/recent"

    # Выбираем случайные 5 ключевых слов
    keywords = random.sample(config["criteria"]["keywords"], 5)
    query = " OR ".join(keywords)

    params = {
        "query": query,
        "max_results": 10,
        "tweet.fields": "id,text,author_id",
    }

    try:
        response = send_twitter_request(session, url, method="GET", params=params)
        if response and "data" in response:
            logging.info(f"Found {len(response['data'])} tweets for query: {query}")
            return response["data"]
        else:
            logging.warning(f"No tweets found for query: {query}")
            return []
    except Exception as e:
        logging.error(f"Error searching tweets: {e}")
        return []


# Комментирование твита
def post_comment(session, tweet_id, comment_text):
    url = "https://api.twitter.com/2/tweets"
    data = {
        "text": comment_text,
        "reply": {"in_reply_to_tweet_id": tweet_id},
    }
    try:
        response = send_twitter_request(session, url, method="POST", data=data)
        if response and "data" in response:
            comment_id = response["data"]["id"]
            logging.info(f"Comment posted: {comment_id}")
            return comment_id
        else:
            logging.error(f"Failed to post comment: {response}")
            return None
    except Exception as e:
        logging.error(f"Error posting comment: {e}")
        return None


# Получение метрик твита
def fetch_tweet_metrics(session, tweet_id):
    url = f"https://api.twitter.com/2/tweets/{tweet_id}"
    params = {"tweet.fields": "public_metrics"}
    try:
        response = send_twitter_request(session, url, method="GET", params=params)
        if response and "data" in response:
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


def analyze_tweet_performance():
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
                metrics = fetch_tweet_metrics(twitter_session, tweet_id)  # Передаем session и tweet_id
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

def generate_comment(tweet_id):
    """
    Генерирует комментарий к указанному твиту, учитывая его текст или текст исходного твита.

    Args:
        tweet_id (str): ID твита, к которому требуется сгенерировать комментарий.

    Returns:
        str: Сгенерированный комментарий или None в случае ошибки.
    """
    try:
        global twitter_session, config  # Используем глобальную сессию и конфигурацию

        # Получаем исходный твит или текущий текст
        original_tweet = get_original_tweet(twitter_session, tweet_id)
        if not original_tweet:
            return None

        tweet_text = original_tweet["text"]

        # Определяем категорию комментария на основе длины текста твита
        comment_category = "general_comments" if len(tweet_text) < 50 else "promotional_comments"
        template = random.choice(config["prompts"][comment_category])  # Берем шаблон из конфигурации

        # Формируем подсказку для GPT
        prompt = (
            f"{template.replace('{{tweet_text}}', tweet_text)}\n\n"
            "Please generate a concise and engaging comment based on this template. The comment must not exceed 100 characters."
        )

        # Генерация комментария через OpenAI API
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a bot that writes relevant and engaging Twitter replies."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=50  # Устанавливаем ограничение на длину в токенах
        )

        comment = response["choices"][0]["message"]["content"].strip()

        # Проверяем длину сгенерированного комментария
        max_length = config.get("generation", {}).get("max_comment_length", 100)  # Берем ограничение из конфигурации
        if len(comment) > max_length:
            logging.warning(f"Generated comment exceeds {max_length} characters: {comment}. Discarding.")
            return None

        logging.info(f"Generated comment for tweet ID {tweet_id}: {comment}")
        return comment
    except Exception as e:
        logging.error(f"Error generating comment for tweet ID {tweet_id}: {e}")
        return None

def comment_on_tweets():
    """
    Поиск твитов и добавление комментариев с контролем частоты запросов.
    """
    try:
        global twitter_session  # Используем глобальную сессию

        # Проверяем, что значение max_comments_per_cycle задано в конфигурации
        max_comments = config["schedule"].get("max_comments_per_cycle")
        if max_comments is None or not isinstance(max_comments, int) or max_comments <= 0:
            raise ValueError("The configuration key 'max_comments_per_cycle' is missing, not set, or invalid.")

        comments_posted = 0

        # Поиск твитов
        tweets = search_tweets(session=twitter_session, config=config, max_results=10)
        if not tweets:
            logging.info("No tweets found for commenting.")
            return

        for tweet in tweets:
            if comments_posted >= max_comments:
                break

            tweet_id = tweet["id"]
            logging.info(f"Generating comment for tweet ID {tweet_id}...")
            comment = generate_comment(tweet_id)

            if comment:
                post_comment(twitter_session, tweet_id, comment)
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
            "Based on these successful tweets, generate 3 new motivational templates "
            "related to cryptocurrency, financial independence, and self-development:\n"
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


def generate_new_keywords():
    """
    Генерирует новые ключевые фразы и добавляет их в config.json в корректном формате.
    """
    config = load_config()

    try:
        prompt = (
            "Generate 5 unique keywords related to cryptocurrency, financial growth, and self-development. "
            "Each keyword should be a single word or phrase, concise, and without numbering or special formatting."
        )

        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a bot that creates creative keywords."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7
        )

        # Извлекаем ключевые слова и очищаем их
        new_keywords = response["choices"][0]["message"]["content"].strip().split("\n")
        new_keywords = [
            clean_unicode(keyword.strip().lstrip("1234567890.- ").strip("\""))  # Убираем номера, лишние символы, кавычки
            for keyword in new_keywords if keyword.strip()
        ]

        # Проверяем уникальность и добавляем в config
        existing_keywords = set(config["criteria"]["keywords"])
        unique_keywords = [keyword for keyword in new_keywords if keyword not in existing_keywords]

        if unique_keywords:
            for keyword in unique_keywords:
                # Сохраняем ключевое слово в базу данных
                save_keyword_to_db(keyword)
                config["criteria"]["keywords"].append(keyword)

            write_config(config)
            print(f"Added {len(unique_keywords)} new keywords in correct format.")
        else:
            print("No unique keywords generated.")

    except Exception as e:
        print(f"Error generating keywords: {e}")

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

def like_commented_tweets():
    """
    Ставит лайки на твиты, которые бот прокомментировал, и исключает повторные лайки.
    """
    try:
        global twitter_session  # Используем глобальную сессию

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

        for tweet_id, content in tweets_to_like:
            if likes_posted >= max_likes:
                break

            try:
                # Отправляем запрос на лайк твита
                url = f"https://api.twitter.com/2/users/{config['twitter']['user_id']}/likes"
                data = {"tweet_id": tweet_id}
                response = send_twitter_request(twitter_session, url, method="POST", data=data)
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


def like_relevant_tweets():
    """
    Ставит лайки на релевантные твиты, найденные по ключевым словам, и исключает повторные лайки.
    """
    try:
        global twitter_session  # Используем глобальную сессию

        # Загружаем лимит лайков за цикл из конфигурации
        max_likes = config["schedule"].get("max_likes_per_cycle", 5)
        likes_posted = 0

        # Ищем твиты для лайков
        tweets = search_tweets(session=twitter_session, config=config, max_results=10)
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
                response = send_twitter_request(twitter_session, url, method="POST", data=data)
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

def safe_task(task):
    def wrapper():
        try:
            task()
        except Exception as e:
            logging.error(f"Task {task.__name__} failed: {e}")
    return wrapper

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

def setup_tasks():
    """
    Настраивает задачи с использованием конфигурации.
    """
    # 1. Генерация твитов
    schedule.every(config["schedule"]["generate_tweet_interval"]).seconds.do(safe_task(generate_tweet))

    # 2. Публикация твита
    schedule.every(config["schedule"]["post_tweet_interval"]).seconds.do(safe_task(post_unpublished_tweet))

    # 3. Перезагрузка конфигурации каждые 5 минут
    schedule.every(5).minutes.do(safe_task(reload_config))

    # 4. Генерация новых шаблонов
    schedule.every(config["schedule"]["generate_templates_interval"]).seconds.do(safe_task(generate_new_templates))
    
    # 5. Генерация новых ключевых слов
    schedule.every(config["schedule"]["generate_keywords_interval"]).seconds.do(safe_task(generate_new_keywords))
    
    # 6. Генерация идей
    schedule.every(config["schedule"]["generate_ideas_interval"]).seconds.do(safe_task(generate_ideas))
    
    # 7. Обновление метрик твитов
    schedule.every(config["schedule"]["update_metrics_interval"]).seconds.do(safe_task(update_tweet_metrics_in_db))
    
    # 8. Комментирование твитов
    schedule.every(config["schedule"]["comment_on_tweets_interval"]).seconds.do(safe_task(comment_on_tweets))
    
    # 9. Анализ идей по ключевым словам
    schedule.every(config["schedule"]["analyze_ideas_interval"]).seconds.do(safe_task(analyze_ideas_keywords))
    
    # 10. Анализ производительности твитов
    schedule.every(config["schedule"]["analyze_performance_interval"]).seconds.do(safe_task(analyze_tweet_performance))
    
    # 11. Экспорт идей в файл (раз в день)
    schedule.every().day.at("00:00").do(safe_task(export_ideas_to_file))
    
    # 12. Печать топ-5 твитов (каждые 6 часов)
    schedule.every(6).hours.do(safe_task(print_top_tweets))
    
    # 13. Резервное копирование базы данных (раз в день)
    schedule.every().day.at("01:00").do(safe_task(backup_database))
    
    # 14. Генерация дневного отчета (раз в день)
    schedule.every().day.at("23:59").do(safe_task(generate_daily_report))

    # 15. Лайки на релевантные твиты
    schedule.every(config["schedule"]["like_relevant_tweets_interval"]).seconds.do(safe_task(like_relevant_tweets))
    
    # 16. Лайки для прокомментированных твитов
    schedule.every(config["schedule"]["like_commented_tweets_interval"]).seconds.do(safe_task(like_commented_tweets))

def manage_tasks():
    logging.info("Starting manage_tasks()")
    setup_tasks()
    
    def run_schedule():
        logging.info("Running scheduled tasks loop")
        while True:
            schedule.run_pending()
            time.sleep(1)
            logging.info("Task loop running...")

    # Запускаем задачи в отдельном потоке
    thread = Thread(target=run_schedule)
    thread.start()

    try:
        thread.join()
    except KeyboardInterrupt:
        logging.info("Bot stopped by user.")


if __name__ == "__main__":
    set_openai_api_key()  # Устанавливаем API-ключ OpenAI
    fix_liked_tweets_table()
    ensure_liked_tweets_table()
    # Инициализация Tweepy API и OAuth1Session
    twitter_session = initialize_oauth_session()

    setup_database()  # Настраиваем базу данных
    manage_tasks()




