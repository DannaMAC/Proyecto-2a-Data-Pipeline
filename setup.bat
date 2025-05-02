@echo off
REM Script para configurar el entorno de desarrollo en Windows

echo Configurando el entorno para el pipeline de datos...

REM Crear entorno virtual de Python
echo Creando entorno virtual...
python -m venv venv

REM Activar entorno virtual
call venv\Scripts\activate.bat

REM Instalar dependencias
echo Instalando dependencias de Python...
pip install requests beautifulsoup4 pandas numpy kafka-python pymongo mysql-connector-python matplotlib plotly dash python-dotenv

REM Crear archivo .env
echo Creando archivo de configuración .env...
(
echo # Configuración de Kafka
echo KAFKA_BOOTSTRAP_SERVERS=localhost:9092
echo KAFKA_TOPIC=crypto_data
echo KAFKA_GROUP_ID=crypto_consumers
echo.
echo # Configuración de MongoDB
echo MONGO_URI=mongodb://localhost:27017/
echo MONGO_DB=crypto_data
echo MONGO_COLLECTION=crypto_prices
echo.
echo # Configuración de MySQL
echo MYSQL_HOST=localhost
echo MYSQL_USER=root
echo MYSQL_PASSWORD=5647382910Da.
echo MYSQL_DATABASE=crypto_data
) > .env

echo.
echo Configuración completada! Para ejecutar el pipeline:
echo 1. Asegúrate de tener Kafka y Zookeeper corriendo
echo 2. Asegúrate de tener MongoDB y MySQL instalados y corriendo
echo 3. Ejecuta el productor: python producer.py
echo 4. Ejecuta el consumidor en otra terminal: python consumer.py
echo 5. Ejecuta la visualización en otra terminal: python visualization.py
echo.
echo Presiona cualquier tecla para salir...
pause > nul