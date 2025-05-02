# Proyecto-2a-Data-Pipeline
# Pipeline de Datos de Criptomonedas

Este proyecto implementa un pipeline de datos completo para obtener, procesar, almacenar y visualizar información sobre criptomonedas en tiempo real. El pipeline incluye web scraping, consumo de APIs, procesamiento con Apache Kafka, almacenamiento en bases de datos y visualización interactiva.

Autora: Danna Corral 

## Arquitectura

El pipeline consta de los siguientes componentes:

1. **Fuentes de datos**:
   - API de CoinGecko para precios de criptomonedas
   - Web scraping de noticias sobre criptomonedas

2. **Productor (Python)**:
   - Obtiene datos de las fuentes
   - Envía mensajes a Apache Kafka

3. **Apache Kafka**:
   - Sistema de mensajería para el procesamiento de flujos
   - Maneja la distribución de mensajes entre productor y consumidor

4. **Consumidor (Python)**:
   - Recibe mensajes de Kafka
   - Procesa y almacena los datos

5. **Bases de datos**:
   - MongoDB para almacenamiento NoSQL
   - MySQL para almacenamiento relacional

6. **Visualización**:
   - Dashboard interactivo con Plotly y Dash
   - Gráficos y tablas actualizados en tiempo real

## Requisitos previos

- Python 
- Apache Kafka
- MongoDB
- MySQL

## Instalación

### 1. Configurar el entorno

Ejecuta el script de configuración para instalar todas las dependencias:

```
setup.bat
```
### 2. Configurar el archivo .env

El script de instalación crea un archivo `.env` con configuración predeterminada. Edita este archivo según tu configuración:

```
# Configuración de Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=crypto_data
KAFKA_GROUP_ID=crypto_consumers

# Configuración de MongoDB
MONGO_URI=mongodb://localhost:27017/
MONGO_DB=crypto_data
MONGO_COLLECTION=crypto_prices

# Configuración de MySQL
MYSQL_HOST=localhost
MYSQL_USER=root
MYSQL_PASSWORD=tu_contraseña
MYSQL_DATABASE=crypto_data
```

## Uso

### Ejecución manual

1. Inicia el productor:
   ```
   python producer.py
   ```

2. Inicia el consumidor en otra terminal:
   ```
   python consumer.py
   ```

3. Inicia el dashboard en otra terminal:
   ```
   python visualization.py
   ```

4. Accede al dashboard en tu navegador: http://127.0.0.1:8050

### Ejecución automática

Usa el script de inicio para lanzar todos los componentes automáticamente:

```
start_pipeline.bat
```

## Componentes del Proyecto

### producer.py
- Obtiene datos de la API de CoinGecko
- Realiza web scraping de noticias sobre criptomonedas
- Envía los datos a un topic de Kafka

### consumer.py
- Recibe mensajes del topic de Kafka
- Procesa los datos
- Almacena los datos en MongoDB y MySQL

### visualization.py
- Crea un dashboard interactivo con Dash y Plotly
- Muestra gráficos de precios y capitalización de mercado
- Muestra una tabla con datos de criptomonedas
- Muestra noticias recientes

### config.py
- Contiene la configuración centralizada para todos los componentes

## Solución de problemas

### El productor no puede conectarse a Kafka
- Verifica que Kafka y Zookeeper estén en ejecución
- Comprueba la configuración de KAFKA_BOOTSTRAP_SERVERS en .env

### El consumidor no recibe mensajes
- Verifica que el topic existe en Kafka
- Comprueba que el productor está enviando mensajes

### Problemas con las bases de datos
- Verifica que MongoDB y MySQL estén en ejecución
- Comprueba las credenciales y configuraciones en .env

### El dashboard no muestra datos
- Asegúrate de que el consumidor está almacenando datos correctamente
- Verifica que las consultas en visualization.py son correctas
