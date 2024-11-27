import os

from dotenv import load_dotenv

load_dotenv()


class MongoDBConfig:
    HOST = os.environ.get("MONGODB_HOST", "0.0.0.0")
    PORT = os.environ.get("MONGODB_PORT", "27017")
    USERNAME = os.environ.get("MONGODB_USERNAME", "")
    PASSWORD = os.environ.get("MONGODB_PASSWORD", "")
    CONNECTION_URL = (
        os.getenv("CONNECTION_URL")
        or f"mongodb@{USERNAME}:{PASSWORD}@http://{HOST}:{PORT}"
    )
    DATABASE = os.getenv("DATABASE", "project_BA")
    CDP_CONNECTION_URL = os.getenv("CDP_CONNECTION_URL")
    CDP_DATABASE = os.getenv("CDP_DATABASE")


class TwitterConfig:
    USERNAME = os.environ.get("TWITTER_USER_NAME")
    PASSWORD = os.environ.get("TWITTER_PASSWORD")
    EMAIL = os.environ.get("EMAIL")
    EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD")

    TELE_API_ID = os.environ.get("TELE_API_ID")
    TELE_API_HASH = os.environ.get("TELE_API_HASH")


class MonitoringConfig:
    MONITOR_ROOT_PATH = os.getenv("MONITOR_ROOT_PATH", "/home/monitor/.log/")
