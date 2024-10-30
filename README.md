# Tarea3BigData

#Importamos librerias necesarias 
from pyspark.sql import SparkSession, functions as F 
Importa SparkSession en las funciones de PySpark SQL con el alias F.
# Inicializa la sesión de Spark 
spark = SparkSession.builder.appName('Tarea3').getOrCreate() 
Crea o obtiene una sesión de Spark llamada “Tarea3”
# Define la ruta del archivo .csv en HDFS 
file_path = 'hdfs://localhost:9000/Tarea3/rows.csv' 
Especifica la ubicación del archivo CSV en HDFS
# Lee el archivo .csv 
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path) 
Lee el archive CSV, infiere el esquema y lo carga en un DataFrame df.
#imprimimos el esquema 
df.printSchema() 
Muestra la estructura del DataFrame, incluyendo nombres y tipos de columnas. 
# Muestra las primeras filas del DataFrame 
df.show() 
Muestra las primeras 20 filas del DataFrame df.
# Estadisticas básicas 
df.summary().show() 
Genera y muestra estadísticas descriptivas básicas para cada columna.
# Consulta: Filtrar por valor y seleccionar columnas 
print("Dias con valor mayor a 5000\n") 
dias = df.filter(F.col('VALOR') > 5000).select('VALOR','VIGENCIADESDE','VIGENCIAHASTA') 
dias.show() 
Filtra filas donde “VALOR” es mayor a 5000 y selecciona las columnas “VALOR” “VIGENCIADESDE” y “VIGENCIAHASTA”. Muestra las filas filtradas.
# Ordenar filas por los valores en la columna "VALOR" en orden descendente 
print("Valores ordenados de mayor a menor\n") 
sorted_df = df.sort(F.col("VALOR").desc()) 
sorted_df.show()
Ordenas las filas del DataFrame df por los valores de la columna “VALOR” en orden descendente y muestra las filas ordenadas.



Código que se utilizó para el análisis de datos en tiempo real con Spark Streaming y Kafka.
Implementación del productor(producer) de Kafka:
import time
Proporciona funciones relacionadas con el tiempo. 
import json
Para trabajar con datos en formato JSON.
import random
Para generar números aleatorios.
from confluent_kafka import Producer
Importa el productor de Kafka para enviar mensajes. 

def generate_sensor_data():
    return {
        "sensor_id": random.randint(1, 10), 
Genera un ID de sensor aleatorio entre 1 y 10.
        "temperature": round(random.uniform(20, 30), 2),
Genera un temperature aleatoria entre 20 y 30 grados.
        "humidity": round(random.uniform(30, 70), 2),
Genera un nivel de humedad aleatorio entre 30 y 70.
        "timestamp": int(time.time())
Obtiene la marca de tiempo actual.
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Sent: {json.loads(msg.value().decode('utf-8'))}")
Verifica si hay un error en la entrega del mensaje y lo imprime. Si no hay error, imprime el mensaje enviado.
producer = Producer({'bootstrap.servers': 'localhost:9092'})
Conecta el productor a un servidor Kafka en localhost:9092.
while True:
    sensor_data = generate_sensor_data()
Genera datos de sensor
    producer.produce('sensor_data', key=str(sensor_data['sensor_id']), value=json.dumps(sensor_data), callback=delivery_report)
Envía los datos al tema “sensor_data” de Kafka, usa sensor_id como clave y los datos como valor.
    producer.poll(1)
Verifica la entrega de mensajes pendientes.
    time.sleep(1)
Pausa el bucle durante 1 segundo antes de enviar los siguientes datos. 



Implementación del consumidor con Spark Streaming
from pyspark.sql import SparkSession 
from pyspark.sql.functions import from_json, col, window 
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType 
import logging 
Para trabajar con Spark Streaming y manipular datos, para definir el esquema de los datos y para configurar el nivel de registro.
# Configura el nivel de log a WARN para reducir los mensajes INFO 
spark = SparkSession.builder \ 
.appName("KafkaSparkStreaming") \ 
.getOrCreate() 
spark.sparkContext.setLogLevel("WARN") 
Reduce los mensajes de registro, crea una sesión de Spark y establece el nivel de resgitro WARN
# Definir el esquema de los datos de entrada 
schema = StructType([ 
StructField("sensor_id", IntegerType()), 
StructField("temperature", FloatType()), 
StructField("humidity", FloatType()), 
StructField("timestamp", TimestampType()) 
]) 
Define el esquemo con los campos sensor_id, temperatura, etc.
# Crear una sesión de Spark 
spark = SparkSession.builder \ 
.appName("SensorDataAnalysis") \ 
.getOrCreate() 
Crea una sesión de spark llamada SensorDataAnalusis
# Configurar el lector de streaming para leer desde Kafka 
df = spark \ 
.readStream \ 
.format("kafka") \ 
.option("kafka.bootstrap.servers", "localhost:9092") \ 
.option("subscribe", "sensor_data") \ 
.load() 
Configura el dataframe para leer los datos de Kafka desde el servidor locarlhost:9092
# Parsear los datos JSON 
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*") 
Convierte los datos según el esquema definido y los selecciona
# Calcular estadísticas por ventana de tiempo 
windowed_stats = parsed_df \ 
.groupBy(window(col("timestamp"), "1 minute"), "sensor_id") \ 
.agg({"temperature": "avg", "humidity": "avg"}) 
Agrupa los datos por ventana de 1 minuto
# Escribir los resultados en la consola 
query = windowed_stats \ 
.writeStream \ 
.outputMode("complete") \ 
.format("console") \ 
.start() 
query.awaitTermination()
Configura la salida del strema para mostrar los resultados en la consola y espera a que termine.

