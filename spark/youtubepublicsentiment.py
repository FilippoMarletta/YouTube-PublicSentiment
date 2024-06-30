import time
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import split
#from elasticsearch import Elasticsearch
from google.cloud import language_v2




# Initialize Spark Context and Session
#es = Elasticsearch("http://elasticsearch:9200")
sparkConf = SparkConf() \
                        #.set("es.nodes", "elasticsearch") \
                        #.set("es.port", "9200") \
                        #.set("es.index.auto.create", "true")

"""
mapping = {
    "mappings": {
        "properties": {
            "timestamp": {
                "type": "date",  # Define the field as date type
                "format": "epoch_millis"  # Accepted date formats
            }
        }
    }
}

elastic_index = "youtubecomments"

if not es.indices.exists(index=elastic_index):
    es.indices.create(index=elastic_index, body=mapping)
else:
    es.indices.put_mapping(index=elastic_index, body=mapping['mappings'])
"""

sc = SparkContext(appName="PythonStructuredStreamsKafka", conf=sparkConf)
spark = SparkSession(sc)
sc.setLogLevel("ERROR")

# Kafka configuration
kafkaServer = "host.docker.internal:9092"
inputTopic = "youtube-comments"
kafkaOutputServer = "host.docker.internal:9092"
outputTopic = "enriched-comments"

# Define the schema for the JSON data with the structure of the comment data
json_schema = StructType([
    StructField("type", StringType(), True),
    StructField("id", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("author", StringType(), True),
    StructField("text", StringType(), True),
    StructField("like_count", IntegerType(), True)
])


# Read data from Kafka without using streaming
df_prova = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", inputTopic) \
    .option("startingOffsets", "earliest") \
    .load()

#df_prova.show()

#print("Records loaded in df_prova:", df_prova.count())

# Read streaming data from Kafka with initial dataframe buffer df_prova
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", inputTopic) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 100000) \
    .option("initialDataFrame", df_prova) \
    .load()

# Select the value and timestamp fields and cast them to string
df = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")


# Parse the JSON content and extract fields
json_df = df.withColumn("json_data", from_json(col("value"), json_schema)) \
            .select("json_data.*", col("timestamp"))




def gcp_sentiment(text):
    if text is not None and text.strip() != "":
        # Initialize the Google Cloud Language client
        client = language_v2.LanguageServiceClient()
        document = language_v2.Document(content=text, type=language_v2.Document.Type.PLAIN_TEXT)
        sentiment = client.analyze_sentiment(document=document).document_sentiment
        sentiment_score = sentiment.score

        # Determine the sentiment label based on score
        if sentiment_score > 0.6:
            sentiment_label = 'very positive'
        elif sentiment_score > 0.2:
            sentiment_label = 'positive'
        elif sentiment_score >= -0.2:
            sentiment_label = 'neutral'
        elif sentiment_score >= -0.6:
            sentiment_label = 'negative'
        else:
            sentiment_label = 'very negative'

        return f"{sentiment_score}, {sentiment_label}"
    else:
        return "0, neutral"

gcp_sentiment_udf = udf(gcp_sentiment, StringType())

# Apply sentiment analysis using the Google Cloud Natural Language API
json_df = json_df.withColumn("gcp_sentiment", gcp_sentiment_udf(col("text")))

# Split the gcp_sentiment column into sentiment and emotion columns
split_col = split(col("gcp_sentiment"), ", ")
json_df = json_df.withColumn("sentiment", split_col.getItem(0).cast("float")) \
                 .withColumn("emotion", split_col.getItem(1))

# Remove the gcp_sentiment column
json_df = json_df.drop("gcp_sentiment")

kafka_compatible_df = json_df.select(to_json(struct(*[col for col in json_df.columns])).alias("value"))

# Write the DataFrame to Elasticsearch
"""
elasticQuery = json_df.writeStream \
   .option("checkpointLocation", "/tmp/") \
   .format("es") \
   .start(elastic_index)
"""
# Debugging output to console
debugQuery = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Write the DataFrame to Kafka
kafkaQuery = kafka_compatible_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaOutputServer) \
    .option("topic", outputTopic) \
    .option("checkpointLocation", "/tmp/kafka_checkpoint") \
    .start()

"""
elasticQuery.awaitTermination()
"""
debugQuery.awaitTermination()
kafkaQuery.awaitTermination()
