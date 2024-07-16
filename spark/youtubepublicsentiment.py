from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType
from elasticsearch import Elasticsearch
from google.cloud import language_v1
import random
import json
import re

# MLlib imports
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover

from transformers import pipeline




# Initialize Spark Context and Session
es = Elasticsearch("http://elasticsearch:9200")
sparkConf = SparkConf() \
                        .set("es.nodes", "elasticsearch") \
                        .set("es.port", "9200") \
                        .set("es.index.auto.create", "true")


mapping = {
    "mappings": {
        "properties": {
            "id": {"type": "keyword"},
            "type": {"type": "keyword"},
            "published_at": {"type": "date"},
            "author": {"type": "keyword"},
            "text": {"type": "text"},
            "like_count": {"type": "integer"},
            "timestamp": {"type": "date"},
        }
    }
}

elastic_index = "youtubecomments"
elastic_index_without_duplicates = "youtubecomments_without_duplicates"

if not es.indices.exists(index=elastic_index):
    es.indices.create(index=elastic_index, body=mapping)
else:
    es.indices.put_mapping(index=elastic_index, body=mapping['mappings'])
if not es.indices.exists(index=elastic_index_without_duplicates):
    es.indices.create(index=elastic_index_without_duplicates, body=mapping)
else:
    es.indices.put_mapping(index=elastic_index_without_duplicates, body=mapping['mappings'])

sc = SparkContext(appName="PythonStructuredStreamsKafka", conf=sparkConf)
spark = SparkSession(sc)
sc.setLogLevel("ERROR")

# Kafka configuration
kafkaServer = "broker:9092"
inputTopic = "youtube-comments"

# Define the schema for the JSON data with the structure of the comment data
json_schema = StructType([
    StructField("type", StringType(), True),
    StructField("id", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("author", StringType(), True),
    StructField("text", StringType(), True),
    StructField("like_count", IntegerType(), True)
])


# Read streaming data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", inputTopic) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 100000) \
    .load()

# Select the value and timestamp fields and cast them to string
df = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")


# Parse the JSON content and extract fields
json_df = df.withColumn("json_data", from_json(col("value"), json_schema)) \
            .select("json_data.*", col("timestamp"))


# Carica il modello di rilevamento delle emozioni
emotion_model = pipeline("text-classification", model="bhadresh-savani/bert-base-go-emotion", top_k=None)

# function to detect emotion
def detect_emotion(text):
    try:
        if text is not None and text.strip() != "":
            
            prediction = emotion_model(text, truncation=True, max_length=512)
            
            print(prediction)
            
            emotions = {
                "most_relevant_emotion": prediction[0][0]["label"]
            }
            
            for i in range(28):
                emotions[prediction[0][i]["label"]] = prediction[0][i]["score"]
            
            return json.dumps(emotions)
        else:
            return None
    except Exception as e:
        print(f"Error in detect_emotion: {e}")
        return None
    
# Function to determine sentiment label
def determine_sentiment_label(score):
    if score > 0.6:
        return 'very positive'
    elif score > 0.2:
        return 'positive'
    elif score >= -0.2:
        return 'neutral'
    elif score >= -0.6:
        return 'negative'
    else:
        return 'very negative'  

        
# Function to analyze text using Google Cloud Natural Language API
def gcp_analyze_text(text):
    if text is not None and text.strip() != "":
        client = language_v1.LanguageServiceClient()
        document = language_v1.Document(content=text, type_=language_v1.Document.Type.PLAIN_TEXT)

        try:
            response = client.annotate_text(
                document=document,
                features={
                    "extract_document_sentiment": True,
                    "extract_entity_sentiment": True,
                },
                encoding_type=language_v1.EncodingType.UTF8,
            )

            # Document sentiment
            sentiment = response.document_sentiment
            sentiment_score = sentiment.score
            sentiment_magnitude = sentiment.magnitude
            sentiment_label = determine_sentiment_label(sentiment_score)
            
            # Entity extraction
            entity_data = [
                {
                    "name": entity.name,
                    "type": entity.type_.name,
                    "salience": entity.salience,
                    "sentiment_label": determine_sentiment_label(entity.sentiment.score),
                    "sentiment_score": entity.sentiment.score,
                    "sentiment_magnitude": entity.sentiment.magnitude
                    
                }
                for entity in response.entities
            ]

            
            result = {
                "document_sentiment_score": sentiment_score,
                "document_sentiment_magnitude": sentiment_magnitude,
                "document_sentiment_label": sentiment_label,
                "entities": entity_data,
            }

            return json.dumps(result)
        except Exception as e:
            print(f"Error analyzing text: {e}")
            return json.dumps({"error": str(e)})
    else:
        return json.dumps({"error": "Empty text"})

# Function to not waste google API calls
def random_analyze_text(text):
    if text is not None and text.strip() != "":
        try:
            # Genera sentimenti casuali
            sentiment_score = random.uniform(-1, 1)
            sentiment_magnitude = random.uniform(0, 2)
            sentiment_label = determine_sentiment_label(sentiment_score)

            #random entities
            entities_count = random.randint(1, 5)
            entity_types = ['PERSON', 'LOCATION', 'ORGANIZATION', 'EVENT', 'WORK_OF_ART', 'CONSUMER_GOOD', 'OTHER']
            entity_data = [
                {
                    "name": f"Entity{random.randint(1, 100)}",
                    "type": random.choice(entity_types),
                    "salience": random.uniform(0, 1),
                    "sentiment_score": random.uniform(-1, 1),
                    "sentiment_magnitude": random.uniform(0, 2)
                }
                for _ in range(entities_count)
            ]

            result = {
                "document_sentiment_score": sentiment_score,
                "document_sentiment_magnitude": sentiment_magnitude,
                "document_sentiment_label": sentiment_label,
                "entities": entity_data,
            }

            return json.dumps(result)
        except Exception as e:
            print(f"Error analyzing text: {e}")
            return json.dumps({"error": str(e)})
    else:
        return json.dumps({"error": "Empty text"})


# Create a UDF for the text analysis
gcp_analyze_text_udf = udf(gcp_analyze_text, StringType())
# Create a UDF for the random text analysis
random_analyze_text_udf = udf(random_analyze_text, StringType())
# Create a UDF for the emotion detection
detect_emotion_udf = udf(detect_emotion, StringType())


# Apply text analysis to the DataFrame
json_df = json_df.withColumn("text_analysis", from_json(gcp_analyze_text_udf(col("text")),
    StructType([
        StructField("document_sentiment_score", FloatType(), True),
        StructField("document_sentiment_magnitude", FloatType(), True),
        StructField("document_sentiment_label", StringType(), True),
        StructField("entities", ArrayType(StructType([
            StructField("name", StringType(), True),
            StructField("type", StringType(), True),
            StructField("salience", FloatType(), True),
            StructField("sentiment_label", StringType(), True),
            StructField("sentiment_score", FloatType(), True),
            StructField("sentiment_magnitude", FloatType(), True),
        ])), True),
    ])
))

# Extract fields from the parsed JSON
json_df = json_df.select(
    "id", "type", "published_at", "author", "text", "like_count", "timestamp",
    col("text_analysis.document_sentiment_score").alias("sentiment_score"),
    col("text_analysis.document_sentiment_magnitude").alias("sentiment_magnitude"),
    col("text_analysis.document_sentiment_label").alias("sentiment_label"),
    col("text_analysis.entities").alias("entities"),
)


# Toknization and stop words removal
tokenizer = RegexTokenizer(inputCol="text", outputCol="tokenized_words", pattern="\\W")
tokenizer_df = tokenizer.transform(json_df)
stop_words_remover = StopWordsRemover(inputCol="tokenized_words", outputCol="filtered_tokens")
filtered_df = stop_words_remover.transform(tokenizer_df)

# Apply emotion detection to the DataFrame
filtered_df = filtered_df.withColumn("emotions_recognition", from_json(detect_emotion_udf(col("text")),
    StructType([
        StructField("most_relevant_emotion", StringType(), True),
        StructField("neutral", FloatType(), True),
        StructField("approval", FloatType(), True),
        StructField("realization", FloatType(), True),
        StructField("disapproval", FloatType(), True),
        StructField("annoyance", FloatType(), True),
        StructField("disappointment", FloatType(), True),
        StructField("admiration", FloatType(), True),
        StructField("anger", FloatType(), True),
        StructField("optimism", FloatType(), True),
        StructField("disgust", FloatType(), True),
        StructField("sadness", FloatType(), True),
        StructField("surprise", FloatType(), True),
        StructField("amusement", FloatType(), True),
        StructField("excitement", FloatType(), True),
        StructField("embarassment", FloatType(), True),
        StructField("confusion", FloatType(), True),
        StructField("fear", FloatType(), True),
        StructField("desire", FloatType(), True),
        StructField("caring", FloatType(), True),
        StructField("joy", FloatType(), True),
        StructField("curiosity", FloatType(), True),
        StructField("nervousness", FloatType(), True),
        StructField("pride", FloatType(), True),
        StructField("relief", FloatType(), True),
        StructField("remorse", FloatType(), True),
        StructField("grief", FloatType(), True),
        StructField("gratitude", FloatType(), True),
        StructField("love", FloatType(), True),
])))

# Select without duplicates
without_duplicates_df = filtered_df.select("id", "type", "published_at", "author", "text", "like_count", "timestamp",
    "sentiment_score", "sentiment_magnitude", "sentiment_label", "filtered_tokens",
    col("emotions_recognition.most_relevant_emotion").alias("most_relevant_emotion"),
    col("emotions_recognition.neutral").alias("neutral"),
    col("emotions_recognition.approval").alias("approval"),
    col("emotions_recognition.realization").alias("realization"),
    col("emotions_recognition.disapproval").alias("disapproval"),
    col("emotions_recognition.annoyance").alias("annoyance"),
    col("emotions_recognition.disappointment").alias("disappointment"),
    col("emotions_recognition.admiration").alias("admiration"),
    col("emotions_recognition.anger").alias("anger"),
    col("emotions_recognition.optimism").alias("optimism"),
    col("emotions_recognition.disgust").alias("disgust"),
    col("emotions_recognition.sadness").alias("sadness"),
    col("emotions_recognition.surprise").alias("surprise"),
    col("emotions_recognition.amusement").alias("amusement"),
    col("emotions_recognition.excitement").alias("excitement"),
    col("emotions_recognition.embarassment").alias("embarassment"),
    col("emotions_recognition.confusion").alias("confusion"),
    col("emotions_recognition.fear").alias("fear"),
    col("emotions_recognition.desire").alias("desire"),
    col("emotions_recognition.caring").alias("caring"),
    col("emotions_recognition.joy").alias("joy"),
    col("emotions_recognition.curiosity").alias("curiosity"),
    col("emotions_recognition.nervousness").alias("nervousness"),
    col("emotions_recognition.pride").alias("pride"),
    col("emotions_recognition.relief").alias("relief"),
    col("emotions_recognition.remorse").alias("remorse"),
    col("emotions_recognition.grief").alias("grief"),
    col("emotions_recognition.gratitude").alias("gratitude"),
    col("emotions_recognition.love").alias("love")
)
# Explode entities
exploded_entities_df = filtered_df.withColumn("exploded_entity", explode(col("entities")))

# Select with duplicates
with_duplicates_df = exploded_entities_df.select(
    "id", "type", "published_at", "author", "text", "like_count", "timestamp",
    "sentiment_score", "sentiment_magnitude", "sentiment_label", "filtered_tokens",
    col("exploded_entity.name").alias("entity_name"),
    col("exploded_entity.type").alias("entity_type"),
    col("exploded_entity.salience").alias("entity_salience"),
    col("exploded_entity.sentiment_label").alias("entity_sentiment_label"),
    col("exploded_entity.sentiment_score").alias("entity_sentiment_score"),
    col("exploded_entity.sentiment_magnitude").alias("entity_sentiment_magnitude"),
    col("emotions_recognition.most_relevant_emotion").alias("most_relevant_emotion"),
    col("emotions_recognition.neutral").alias("neutral"),
    col("emotions_recognition.approval").alias("approval"),
    col("emotions_recognition.realization").alias("realization"),
    col("emotions_recognition.disapproval").alias("disapproval"),
    col("emotions_recognition.annoyance").alias("annoyance"),
    col("emotions_recognition.disappointment").alias("disappointment"),
    col("emotions_recognition.admiration").alias("admiration"),
    col("emotions_recognition.anger").alias("anger"),
    col("emotions_recognition.optimism").alias("optimism"),
    col("emotions_recognition.disgust").alias("disgust"),
    col("emotions_recognition.sadness").alias("sadness"),
    col("emotions_recognition.surprise").alias("surprise"),
    col("emotions_recognition.amusement").alias("amusement"),
    col("emotions_recognition.excitement").alias("excitement"),
    col("emotions_recognition.embarassment").alias("embarassment"),
    col("emotions_recognition.confusion").alias("confusion"),
    col("emotions_recognition.fear").alias("fear"),
    col("emotions_recognition.desire").alias("desire"),
    col("emotions_recognition.caring").alias("caring"),
    col("emotions_recognition.joy").alias("joy"),
    col("emotions_recognition.curiosity").alias("curiosity"),
    col("emotions_recognition.nervousness").alias("nervousness"),
    col("emotions_recognition.pride").alias("pride"),
    col("emotions_recognition.relief").alias("relief"),
    col("emotions_recognition.remorse").alias("remorse"),
    col("emotions_recognition.grief").alias("grief"),
    col("emotions_recognition.gratitude").alias("gratitude"),
    col("emotions_recognition.love").alias("love")
)


# Write the DataFrame to Elasticsearch with duplicates
elasticQuery = with_duplicates_df.writeStream \
   .option("checkpointLocation", "/tmp/with_duplicates") \
   .format("es") \
   .start(elastic_index)
# Write the DataFrame to Elasticsearch without duplicates
elasticQuery = without_duplicates_df.writeStream \
    .option("checkpointLocation", "/tmp/without_duplicates") \
    .format("es") \
    .start(elastic_index_without_duplicates)

# Debugging output to console
debugQuery = with_duplicates_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

elasticQuery.awaitTermination()
debugQuery.awaitTermination()

