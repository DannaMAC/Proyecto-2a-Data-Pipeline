"""
Consumidor Kafka: Recibe datos de criptomonedas y los almacena en MongoDB/MySQL
"""
import json
import logging
import signal
import sys
from kafka import KafkaConsumer
from pymongo import MongoClient
import mysql.connector
from mysql.connector import Error
from datetime import datetime

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('crypto_consumer')

# Configuración de conexiones
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'crypto_data'
MONGO_URI = 'mongodb://localhost:27017/'
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'crypto_data'
}

# Conectar a MongoDB
def connect_to_mongodb():
    try:
        client = MongoClient(MONGO_URI)
        db = client['crypto_data']
        logger.info("Conectado a MongoDB: %s", MONGO_URI)
        return db
    except Exception as e:
        logger.error("Error al conectar a MongoDB: %s", str(e))
        return None

# Conectar a MySQL
def connect_to_mysql():
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        if conn.is_connected():
            logger.info("Conectado a MySQL en la base de datos 'crypto_data'")
            return conn
    except Error as e:
        logger.error("Error al conectar a MySQL: %s", str(e))
    return None

# Guardar datos de precios en MongoDB
def save_price_data_to_mongodb(db, data):
    try:
        collection = db['crypto_prices']
        for item in data:
            item['timestamp'] = datetime.utcnow()
        collection.insert_many(data)
        logger.info("Datos de precios guardados en MongoDB")
    except Exception as e:
        logger.error("Error al guardar datos en MongoDB: %s", str(e))

# Guardar datos de noticias en MongoDB
def save_news_data_to_mongodb(db, data):
    try:
        collection = db['crypto_news']
        for item in data:
            item['timestamp'] = datetime.utcnow()
        collection.insert_many(data)
        logger.info("Noticias guardadas en MongoDB")
    except Exception as e:
        logger.error("Error al guardar noticias en MongoDB: %s", str(e))

# Guardar datos de precios en MySQL
def save_price_data_to_mysql(conn, data):
    try:
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS crypto_prices (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            symbol VARCHAR(10),
            price_usd FLOAT,
            market_cap_usd FLOAT,
            timestamp DATETIME
        )''')
        for item in data:
            cursor.execute('''INSERT INTO crypto_prices (name, symbol, price_usd, market_cap_usd, timestamp)
                              VALUES (%s, %s, %s, %s, %s)''',
                           (item['name'], item['symbol'], item['price_usd'], item['market_cap_usd'], datetime.utcnow()))
        conn.commit()
        logger.info("Datos de precios guardados en MySQL")
    except Error as e:
        logger.error("Error al guardar datos en MySQL: %s", str(e))

# Guardar noticias en MySQL
def save_news_data_to_mysql(conn, data):
    try:
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS crypto_news (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title TEXT,
            source VARCHAR(255),
            url TEXT,
            published_at DATETIME,
            timestamp DATETIME
        )''')
        for item in data:
            cursor.execute('''INSERT INTO crypto_news (title, source, url, published_at, timestamp)
                              VALUES (%s, %s, %s, %s, %s)''',
                           (item['title'], item['source'], item['url'], item['published_at'], datetime.utcnow()))
        conn.commit()
        logger.info("Noticias guardadas en MySQL")
    except Error as e:
        logger.error("Error al guardar noticias en MySQL: %s", str(e))

# Cierre ordenado
def shutdown(signum, frame):
    logger.info("Señal de salida recibida: %s", signum)
    global consumer, mysql_conn
    try:
        if consumer is not None:
            consumer.close()
            logger.info("Consumidor cerrado correctamente")
    except:
        pass
    try:
        if mysql_conn is not None:
            mysql_conn.close()
            logger.info("Conexión a MySQL cerrada")
    except:
        pass
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# Conectar a MongoDB y MySQL
mongodb = connect_to_mongodb()
mysql_conn = connect_to_mysql()

# Iniciar consumidor de Kafka
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='crypto_consumers',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
logger.info("Consumidor conectado a Kafka en %s", KAFKA_BROKER)
logger.info("Escuchando en el topic: %s", KAFKA_TOPIC)

for message in consumer:
    try:
        content = message.value
        msg_type = content.get('type')
        data = content.get('data')

        if msg_type == 'price_data':
            logger.info(f"Recibidos datos de precios de {len(data)} criptomonedas")
            if mongodb is not None:
                save_price_data_to_mongodb(mongodb, data)
            if mysql_conn is not None:
                save_price_data_to_mysql(mysql_conn, data)

        elif msg_type == 'news_data':
            logger.info(f"Recibidos datos de {len(data)} noticias")
            if mongodb is not None:
                save_news_data_to_mongodb(mongodb, data)
            if mysql_conn is not None:
                save_news_data_to_mysql(mysql_conn, data)

    except Exception as e:
        logger.error("Error inesperado en el consumidor: %s", str(e))
