"""
Configuración para el pipeline de datos
"""
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde archivo .env
load_dotenv()

# Configuración de Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'crypto_data')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'crypto_consumers')

# Configuración de la API
CRYPTO_API_URL = "https://api.coingecko.com/api/v3/coins/markets"
CRYPTO_API_PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 20,
    "page": 1,
    "sparkline": False
}

# Configuración del web scraping
CRYPTO_NEWS_URL = "https://cryptonews.com/"

# Configuración de MongoDB
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
MONGO_DB = os.getenv('MONGO_DB', 'crypto_data')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'crypto_prices')

# Configuración de MySQL
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'password')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'crypto_data')

# Intervalo de actualización de datos (en segundos)
DATA_UPDATE_INTERVAL = 60