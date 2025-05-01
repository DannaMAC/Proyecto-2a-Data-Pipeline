"""
Productor Kafka: Obtiene datos de criptomonedas y noticias y los envía a Kafka
"""
import json
import time
import logging
import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer
from datetime import datetime

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    CRYPTO_API_URL,
    CRYPTO_API_PARAMS,
    CRYPTO_NEWS_URL,
    DATA_UPDATE_INTERVAL
)

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('crypto_producer')

def get_crypto_prices():
    """Obtiene precios de criptomonedas desde la API de CoinGecko"""
    try:
        response = requests.get(CRYPTO_API_URL, params=CRYPTO_API_PARAMS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error al obtener datos de la API: {e}")
        return []

def get_crypto_news():
    """Obtiene noticias mediante web scraping de un portal de criptomonedas"""
    try:
        response = requests.get(CRYPTO_NEWS_URL)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        news_items = []
        
        # Extraer los titulares de noticias y enlaces
        articles = soup.select('.cn-tile article')[:5]  # Obtener solo 5 noticias
        
        for article in articles:
            title_element = article.select_one('.cn-tile-header h4')
            link_element = article.select_one('a')
            
            if title_element and link_element:
                title = title_element.get_text(strip=True)
                link = link_element.get('href')
                if not link.startswith('http'):
                    link = CRYPTO_NEWS_URL.rstrip('/') + link
                
                news_items.append({
                    'title': title,
                    'link': link,
                    'timestamp': datetime.now().isoformat()
                })
        
        return news_items
    except Exception as e:
        logger.error(f"Error al hacer scraping de noticias: {e}")
        return []

def create_kafka_producer():
    """Crea y retorna un productor de Kafka"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        return producer
    except Exception as e:
        logger.error(f"Error al crear el productor de Kafka: {e}")
        return None

def run_producer():
    """Función principal para ejecutar el productor"""
    producer = create_kafka_producer()
    if not producer:
        logger.error("No se pudo crear el productor de Kafka. Saliendo...")
        return
    
    logger.info(f"Productor conectado a Kafka en {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Enviando datos al topic: {KAFKA_TOPIC}")
    
    try:
        while True:
            # Obtener datos de precios
            crypto_prices = get_crypto_prices()
            if crypto_prices:
                # Preparar el mensaje con timestamp
                message = {
                    'type': 'price_data',
                    'data': crypto_prices,
                    'timestamp': datetime.now().isoformat()
                }
                
                # Enviar el mensaje a Kafka
                producer.send(KAFKA_TOPIC, message)
                logger.info(f"Enviados datos de precios de {len(crypto_prices)} criptomonedas a Kafka")
            
            # Obtener noticias
            news_data = get_crypto_news()
            if news_data:
                # Preparar el mensaje con timestamp
                message = {
                    'type': 'news_data',
                    'data': news_data,
                    'timestamp': datetime.now().isoformat()
                }
                
                # Enviar el mensaje a Kafka
                producer.send(KAFKA_TOPIC, message)
                logger.info(f"Enviados datos de {len(news_data)} noticias a Kafka")
            
            # Esperar hasta la próxima actualización
            time.sleep(DATA_UPDATE_INTERVAL)
    
    except KeyboardInterrupt:
        logger.info("Deteniendo el productor...")
    except Exception as e:
        logger.error(f"Error inesperado en el productor: {e}")
    finally:
        if producer:
            producer.flush()
            producer.close()
            logger.info("Productor cerrado correctamente")

if __name__ == "__main__":
    run_producer()