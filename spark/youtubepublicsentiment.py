from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType
from elasticsearch import Elasticsearch
from google.cloud import language_v2
import random
import json




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
            "sentiment_score": {"type": "float"},
            "sentiment_magnitude": {"type": "float"},
            "sentiment_label": {"type": "keyword"},
            "entities": {
                "type": "nested",
                "properties": {
                    "name": {"type": "keyword"},
                    "type": {"type": "keyword"}
                }
            },
            "moderation_categories": {
                "type": "nested",
                "properties": {
                    "name": {"type": "keyword"},
                    "confidence": {"type": "float"}
                }
            },
            "words": {"type": "keyword"}
        }
    }
}

elastic_index = "youtubecomments"

if not es.indices.exists(index=elastic_index):
    es.indices.create(index=elastic_index, body=mapping)
else:
    es.indices.put_mapping(index=elastic_index, body=mapping['mappings'])


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




def gcp_analyze_text(text):
    if text is not None and text.strip() != "":
        client = language_v2.LanguageServiceClient()
        document = language_v2.Document(content=text, type_=language_v2.Document.Type.PLAIN_TEXT)
        
        try:
            response = client.annotate_text(
                document=document,
                features={
                    "extract_entities": True,
                    "extract_document_sentiment": True,
                    "moderate_text": True,
                },
                encoding_type=language_v2.EncodingType.UTF8,
            )
            
            # Document sentiment
            sentiment = response.document_sentiment
            sentiment_score = sentiment.score
            sentiment_magnitude = sentiment.magnitude
            
            # Determine sentiment label
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
            
            # Entity extraction
            entity_data = [
                {
                    "name": entity.name,
                    "type": language_v2.Entity.Type(entity.type_).name,
                }
                for entity in response.entities
            ]
            
             # Moderation categories
            moderation_categories = [
                {
                    "name": category.name,
                    "confidence": category.confidence
                }
                for category in response.moderation_categories
            ]
            
            # Extract words for tag cloud
            words = text.lower().split()
            
            result = {
                "document_sentiment_score": sentiment_score,
                "document_sentiment_magnitude": sentiment_magnitude,
                "document_sentiment_label": sentiment_label,
                "entities": entity_data,
                "moderation_categories": moderation_categories,
                "words": words
            }
            
            return json.dumps(result)
        except Exception as e:
            print(f"Error analyzing text: {e}")
            return json.dumps({"error": str(e)})
    else:
        return json.dumps({"error": "Empty text"})
    
def random_analyze_text(text):
    if text is not None and text.strip() != "":
        # Generate random document sentiment
        sentiment_score = random.uniform(-1.0, 1.0)
        sentiment_magnitude = random.uniform(0.0, 2.0)
        
        # Determine sentiment label
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
        
        # Generate random entities (between 1 and 5)
        num_entities = random.randint(1, 5)
        entity_types = ['PERSON', 'LOCATION', 'ORGANIZATION', 'EVENT', 'WORK_OF_ART', 'CONSUMER_GOOD']
        entities = []
        
        words = text.split()
        for i in range(num_entities):
            if words:
                entity_name = random.choice(words)
            else:
                entity_name = f"Entity{i+1}"
            entities.append({
                "name": entity_name,
                "type": random.choice(entity_types),
                "salience": random.uniform(0.0, 1.0),
                "sentiment_score": random.uniform(-1.0, 1.0),
                "sentiment_magnitude": random.uniform(0.0, 2.0)
            })
        
        # Sort entities by salience
        entities = sorted(entities, key=lambda e: e['salience'], reverse=True)
        
        result = {
            "document_sentiment_score": sentiment_score,
            "document_sentiment_magnitude": sentiment_magnitude,
            "document_sentiment_label": sentiment_label,
            "entities": entities
        }
        
        return json.dumps(result)
    else:
        return json.dumps({"error": "Empty text"})
    
# Create a UDF for the text analysis
gcp_analyze_text_udf = udf(gcp_analyze_text, StringType())
# Create a UDF for the random text analysis
random_analyze_text_udf = udf(random_analyze_text, StringType())


# Apply text analysis to the DataFrame
json_df = json_df.withColumn("text_analysis", from_json(gcp_analyze_text_udf(col("text")), 
    StructType([
        StructField("document_sentiment_score", FloatType(), True),
        StructField("document_sentiment_magnitude", FloatType(), True),
        StructField("document_sentiment_label", StringType(), True),
        StructField("entities", ArrayType(StructType([
            StructField("name", StringType(), True),
            StructField("type", StringType(), True)
        ])), True),
        StructField("moderation_categories", ArrayType(StructType([
            StructField("name", StringType(), True),
            StructField("confidence", FloatType(), True)
        ])), True),
        StructField("words", ArrayType(StringType()), True)
    ])
))

# Extract fields from the parsed JSON
json_df = json_df.select(
    "id", "type", "published_at", "author", "text", "like_count", "timestamp",
    col("text_analysis.document_sentiment_score").alias("sentiment_score"),
    col("text_analysis.document_sentiment_magnitude").alias("sentiment_magnitude"),
    col("text_analysis.document_sentiment_label").alias("sentiment_label"),
    col("text_analysis.entities").alias("entities"),
    col("text_analysis.moderation_categories").alias("moderation_categories"),
    col("text_analysis.words").alias("words")
)



enriched_df = json_df.select(
    "id", "type", "published_at", "author", "text", "like_count", "timestamp",
    "sentiment_score", "sentiment_magnitude", "sentiment_label",
    "entities", "moderation_categories", "words"
)

# Write the DataFrame to Elasticsearch

elasticQuery = enriched_df.writeStream \
   .option("checkpointLocation", "/tmp/") \
   .format("es") \
   .start(elastic_index)
   

# Debugging output to console
debugQuery = enriched_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

elasticQuery.awaitTermination()
debugQuery.awaitTermination()

