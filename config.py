"""
Configuration settings for AmoCRM exporter
"""
import os
from dotenv import load_dotenv

# Load environment variables from .env file
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

# AmoCRM OAuth2 credentials
AUTHORIZATION_CODE = os.getenv('AUTHORIZATION_CODE')
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI')

# AmoCRM domain - используем wecheap.amocrm.ru как домен аккаунта
AMOCRM_DOMAIN = 'wecheap.amocrm.ru'  # Подтверждено техподдержкой

# API domain from token - используем значение из токена
API_DOMAIN = os.getenv('API_DOMAIN', 'api-a.amocrm.ru')

# API endpoints
BASE_URL = f'https://{AMOCRM_DOMAIN}'
API_URL = f'https://{API_DOMAIN}/api/v4'
AUTH_URL = f'{BASE_URL}/oauth2/access_token'

# File paths
DATA_DIR = 'data'
TOKEN_FILE = os.path.join(DATA_DIR, 'token.json')
DEALS_FILE = os.path.join(DATA_DIR, 'deals.json')
CONTACTS_FILE = os.path.join(DATA_DIR, 'contacts.json')
COMPANIES_FILE = os.path.join(DATA_DIR, 'companies.json')
EVENTS_FILE = os.path.join(DATA_DIR, 'events.json')
LOG_FILE = os.path.join(DATA_DIR, 'log.json')
STATE_FILE = os.path.join(DATA_DIR, 'export_state.json')

# API rate limits
MAX_REQUESTS_PER_SECOND = 5  # Будьте консервативны - макс лимит 50 запросов/сек

# Pagination
PAGE_SIZE = 50  # Максимальное количество сущностей в одном запросе

# Log retention (in days)
LOG_RETENTION_DAYS = 7

# Token refresh buffer (in seconds) - refresh token if it expires in less than this time
TOKEN_REFRESH_BUFFER = 3600  # 1 hour

# Включаем расширенное логирование для диагностики проблем с авторизацией
DEBUG_LOG = True