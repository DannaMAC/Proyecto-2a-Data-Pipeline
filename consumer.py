"""
Consumidor Kafka: Recibe datos de criptomonedas y noticias, y los almacena en MongoDB y MySQL
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
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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
        logger.info("Conectado a MongoDB")
        return db
    except Exception as e:
        logger.error("Error al conectar a MongoDB: %s", str(e))
        return None

# Conectar a MySQL
def connect_to_mysql():
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        if conn.is_connected():
            logger.info("Conectado a MySQL")
            return conn
    except Error as e:
        logger.error("Error al conectar a MySQL: %s", str(e))
    return None

# Guardar precios en MongoDB
def save_price_data_to_mongodb(db, data):
    try:
        collection = db['crypto_prices']
        processed = []
        for item in data:
            processed.append({
                'name': item.get('name'),
                'symbol': item.get('symbol'),
                'price_usd': item.get('current_price'),
                'market_cap_usd': item.get('market_cap'),
                'timestamp': datetime.utcnow()
            })
        if processed:
            collection.insert_many(processed)
            logger.info(f"{len(processed)} precios guardados en MongoDB")
    except Exception as e:
        logger.error("Error al guardar en MongoDB (precios): %s", str(e))

# Guardar precios en MySQL
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
                           (item.get('name'),
                            item.get('symbol'),
                            item.get('current_price'),
                            item.get('market_cap'),
                            datetime.utcnow()))
        conn.commit()
        logger.info(f"{len(data)} precios guardados en MySQL")
    except Error as e:
        logger.error("Error al guardar en MySQL (precios): %s", str(e))

# Guardar noticias en MongoDB
def save_news_data_to_mongodb(db, data):
    try:
        collection = db['crypto_news']
        processed = []
        for item in data:
            processed.append({
                'title': item.get('title'),
                'link': item.get('link'),
                'timestamp': datetime.utcnow()
            })
        if processed:
            collection.insert_many(processed)
            logger.info(f"{len(processed)} noticias guardadas en MongoDB")
    except Exception as e:
        logger.error("Error al guardar en MongoDB (noticias): %s", str(e))

# Guardar noticias en MySQL
def save_news_data_to_mysql(conn, data):
    try:
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS crypto_news (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title TEXT,
            url TEXT,
            timestamp DATETIME
        )''')
        for item in data:
            cursor.execute('''INSERT INTO crypto_news (title, url, timestamp)
                              VALUES (%s, %s, %s)''',
                           (item.get('title'),
                            item.get('link'),
                            datetime.utcnow()))
        conn.commit()
        logger.info(f"{len(data)} noticias guardadas en MySQL")
    except Error as e:
        logger.error("Error al guardar en MySQL (noticias): %s", str(e))

# Cierre ordenado
def shutdown(signum, frame):
    logger.info("Señal de cierre recibida: %s", signum)
    global consumer, mysql_conn
    try:
        if consumer is not None:
            consumer.close()
            logger.info("Consumidor cerrado")
    except Exception as e:
        logger.error("Error al cerrar consumidor: %s", str(e))
    try:
        if mysql_conn is not None:
            mysql_conn.close()
            logger.info("Conexión a MySQL cerrada")
    except Exception as e:
        logger.error("Error al cerrar conexión MySQL: %s", str(e))
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# Iniciar conexiones
mongodb = connect_to_mongodb()
mysql_conn = connect_to_mysql()

# Iniciar consumidor Kafka
try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='crypto_consumers',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    logger.info(f"👂 Escuchando en topic '{KAFKA_TOPIC}' en {KAFKA_BROKER}")
except Exception as e:
    logger.error("No se pudo iniciar el consumidor: %s", str(e))
    sys.exit(1)

# Bucle principal de consumo
for message in consumer:
    try:
        content = message.value
        logger.debug(f"Mensaje crudo recibido: {content}")

        msg_type = content.get('type')
        data = content.get('data')

        if not msg_type or not data:
            logger.warning("Mensaje incompleto o inválido")
            continue

        logger.info(f"📬 Tipo de mensaje: {msg_type}, Cantidad de registros: {len(data)}")

        if msg_type == 'price_data':
            if mongodb is not None:
                save_price_data_to_mongodb(mongodb, data)
            if mysql_conn is not None:
                save_price_data_to_mysql(mysql_conn, data)

        elif msg_type == 'news_data':
            if mongodb is not None:
                save_news_data_to_mongodb(mongodb, data)
            if mysql_conn is not None:
                save_news_data_to_mysql(mysql_conn, data)

        else:
            logger.warning(f"Tipo de mensaje desconocido: {msg_type}")

    except Exception as e:
        logger.error("Error procesando mensaje: %s", str(e), exc_info=True)