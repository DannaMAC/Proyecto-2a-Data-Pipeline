"""
Consumidor Kafka: Recibe datos de criptomonedas y los almacena en MongoDB/MySQL
"""
import json
import logging
from kafka import KafkaConsumer
from pymongo import MongoClient
import mysql.connector
from datetime import datetime

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_GROUP_ID,
    MONGO_URI,
    MONGO_DB,
    MONGO_COLLECTION,
    MYSQL_HOST,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE
)

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('crypto_consumer')

# Conexión a MongoDB
def connect_to_mongodb():
    """Crea y retorna una conexión a MongoDB"""
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        logger.info(f"Conectado a MongoDB: {MONGO_URI}")
        return db
    except Exception as e:
        logger.error(f"Error al conectar con MongoDB: {e}")
        return None

# Conexión a MySQL
def connect_to_mysql():
    """Crea y retorna una conexión a MySQL"""
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        logger.info(f"Conectado a MySQL: {MYSQL_HOST}")
        return conn
    except Exception as e:
        logger.error(f"Error al conectar con MySQL: {e}")
        return None

def setup_mysql_tables(conn):
    """Configura las tablas necesarias en MySQL"""
    try:
        cursor = conn.cursor()
        
        # Tabla para precios de criptomonedas
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS crypto_prices (
            id INT AUTO_INCREMENT PRIMARY KEY,
            coin_id VARCHAR(50) NOT NULL,
            name VARCHAR(100) NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            current_price DECIMAL(18, 8) NOT NULL,
            market_cap BIGINT,
            price_change_24h DECIMAL(18, 8),
            timestamp DATETIME NOT NULL
        )
        ''')
        
        # Tabla para noticias
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS crypto_news (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            link VARCHAR(255) NOT NULL,
            timestamp DATETIME NOT NULL
        )
        ''')
        
        conn.commit()
        logger.info("Tablas de MySQL configuradas correctamente")
    except Exception as e:
        logger.error(f"Error al configurar tablas de MySQL: {e}")

def save_price_data_to_mongodb(db, data):
    """Guarda los datos de precios en MongoDB"""
    try:
        collection = db[MONGO_COLLECTION]
        result = collection.insert_many(data)
        logger.info(f"Guardados {len(result.inserted_ids)} documentos en MongoDB")
    except Exception as e:
        logger.error(f"Error al guardar en MongoDB: {e}")

def save_news_data_to_mongodb(db, data):
    """Guarda los datos de noticias en MongoDB"""
    try:
        collection = db["crypto_news"]
        result = collection.insert_many(data)
        logger.info(f"Guardadas {len(result.inserted_ids)} noticias en MongoDB")
    except Exception as e:
        logger.error(f"Error al guardar noticias en MongoDB: {e}")

def save_price_data_to_mysql(conn, data):
    """Guarda los datos de precios en MySQL"""
    try:
        cursor = conn.cursor()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for coin in data:
            insert_query = '''
            INSERT INTO crypto_prices 
            (coin_id, name, symbol, current_price, market_cap, price_change_24h, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            '''
            
            values = (
                coin.get('id', ''),
                coin.get('name', ''),
                coin.get('symbol', ''),
                coin.get('current_price', 0),
                coin.get('market_cap', 0),
                coin.get('price_change_24h', 0),
                timestamp
            )
            
            cursor.execute(insert_query, values)
        
        conn.commit()
        logger.info(f"Guardados {len(data)} registros de precios en MySQL")
    except Exception as e:
        logger.error(f"Error al guardar en MySQL: {e}")

def save_news_data_to_mysql(conn, data):
    """Guarda los datos de noticias en MySQL"""
    try:
        cursor = conn.cursor()
        
        for news in data:
            insert_query = '''
            INSERT INTO crypto_news 
            (title, link, timestamp)
            VALUES (%s, %s, %s)
            '''
            
            timestamp = datetime.fromisoformat(news.get('timestamp')).strftime('%Y-%m-%d %H:%M:%S')
            
            values = (
                news.get('title', ''),
                news.get('link', ''),
                timestamp
            )
            
            cursor.execute(insert_query, values)
        
        conn.commit()
        logger.info(f"Guardadas {len(data)} noticias en MySQL")
    except Exception as e:
        logger.error(f"Error al guardar noticias en MySQL: {e}")

def create_kafka_consumer():
    """Crea y retorna un consumidor de Kafka"""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        return consumer
    except Exception as e:
        logger.error(f"Error al crear el consumidor de Kafka: {e}")
        return None

def run_consumer():
    """Función principal para ejecutar el consumidor"""
    # Inicializar conexiones a bases de datos
    mongodb = connect_to_mongodb()
    mysql_conn = connect_to_mysql()
    
    if mysql_conn:
        setup_mysql_tables(mysql_conn)
    
    consumer = create_kafka_consumer()
    if not consumer:
        logger.error("No se pudo crear el consumidor de Kafka. Saliendo...")
        return
    
    logger.info(f"Consumidor conectado a Kafka en {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Escuchando en el topic: {KAFKA_TOPIC}")
    
    try:
        for message in consumer:
            # Procesar el mensaje recibido
            value = message.value
            msg_type = value.get('type')
            data = value.get('data', [])
            
            if not data:
                logger.warning("Mensaje recibido sin datos")
                continue
            
            # Almacenar datos según su tipo
            if msg_type == 'price_data':
                logger.info(f"Recibidos datos de precios de {len(data)} criptomonedas")
                if mongodb:
                    save_price_data_to_mongodb(mongodb, data)
                if mysql_conn:
                    save_price_data_to_mysql(mysql_conn, data)
            
            elif msg_type == 'news_data':
                logger.info(f"Recibidos datos de {len(data)} noticias")
                if mongodb:
                    save_news_data_to_mongodb(mongodb, data)
                if mysql_conn:
                    save_news_data_to_mysql(mysql_conn, data)
            
            else:
                logger.warning(f"Tipo de mensaje desconocido: {msg_type}")
    
    except KeyboardInterrupt:
        logger.info("Deteniendo el consumidor...")
    except Exception as e:
        logger.error(f"Error inesperado en el consumidor: {e}")
    finally:
        if consumer:
            consumer.close()
            logger.info("Consumidor cerrado correctamente")
        if mysql_conn:
            mysql_conn.close()
            logger.info("Conexión a MySQL cerrada")

if __name__ == "__main__":
    run_consumer()