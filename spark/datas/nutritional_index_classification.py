import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, size, udf, coalesce, array_contains, lit
from pyspark.sql.types import IntegerType, FloatType

def create_spark_session():
    mongodb_uri = "mongodb://off_admin:admin@off-db:27017/?authMechanism=SCRAM-SHA-256&authSource=off"
    
    spark = SparkSession.builder \
        .appName("OpenFoodFacts Classification Nutritionnelle") \
        .config("spark.mongodb.input.uri", mongodb_uri) \
        .config("spark.mongodb.output.uri", mongodb_uri) \
        .getOrCreate()
    
    print("Spark session created successfully.")
    return spark

def load_and_filter_data(spark):
    df = spark.read.format("mongo").option("database", "off").option("collection", "products").load()
    filtered_df = df.filter(
        ((col("product_name_fr").isNotNull()) & (col("product_name_fr") != "")) |
        ((col("generic_name_fr").isNotNull()) & (col("generic_name_fr") != ""))
    )
    print(f"Loaded {filtered_df.count()} products after filtering.")
    return filtered_df

def calculate_nutritional_index(df):
    def count_additives(additives_tags):
        if additives_tags:
            return len([tag for tag in additives_tags if tag.startswith("en:e")])
        return 0
    
    count_additives_udf = udf(count_additives, IntegerType())
    
    # Score basé sur les nutriments
    df = df.withColumn("nutrient_score",
        when(col("nutrition_grade_fr").isin("a", "b"), 5)
        .when(col("nutrition_grade_fr") == "c", 3)
        .when(col("nutrition_grade_fr").isin("d", "e"), 1)
        .otherwise(0)
    )
    print("Nutrient score calculated.")
    df.select("_id", "nutrition_grade_fr", "nutrient_score").show(5)
    
    # Score basé sur le groupe NOVA
    df = df.withColumn("nova_score",
        when(col("nova_group") == 1, 5)
        .when(col("nova_group") == 2, 4)
        .when(col("nova_group") == 3, 2)
        .when(col("nova_group") == 4, 0)
        .otherwise(3)  # Valeur par défaut si non spécifié
    )
    print("NOVA score calculated.")
    df.select("_id", "nova_group", "nova_score").show(5)
    
    # Pénalité basée sur les additifs
    df = df.withColumn("additives_count", count_additives_udf(col("additives_tags")))
    df = df.withColumn("additives_penalty", 
        when(col("additives_count") == 0, 0)
        .when(col("additives_count") <= 2, -1)
        .when(col("additives_count") <= 5, -2)
        .otherwise(-3)
    )
    print("Additives penalty calculated.")
    df.select("_id", "additives_count", "additives_penalty").show(5)
    
    # Bonus pour les produits bio
    df = df.withColumn("organic_bonus", 
        when(array_contains(col("labels_tags"), "en:organic"), 1).otherwise(0)
    )
    print("Organic bonus calculated.")
    df.select("_id", "labels_tags", "organic_bonus").show(5, truncate=False)
    
    # Pénalité pour les allergènes
    df = df.withColumn("allergens_penalty", 
        when(size(col("allergens_tags")) > 0, -1).otherwise(0)
    )
    print("Allergens penalty calculated.")
    df.select("_id", "allergens_tags", "allergens_penalty").show(5, truncate=False)
    
    # Calcul de l'indice nutritionnel final
    df = df.withColumn("nutritional_index", 
        (col("nutrient_score") + col("nova_score") + col("additives_penalty") + 
         col("organic_bonus") + col("allergens_penalty")) / 2
    )
    
    # Normalisation de l'indice entre 1 et 5
    df = df.withColumn("nutritional_index", 
        when(col("nutritional_index") < 1, 1)
        .when(col("nutritional_index") > 5, 5)
        .otherwise(col("nutritional_index"))
    )
    print("Final nutritional index calculated.")
    df.select("_id", "nutritional_index").show(5)
    
    return df

def process_data(spark):
    df = load_and_filter_data(spark)
    final_df = calculate_nutritional_index(df)
    
    result_df = final_df.select(
        "_id",
        coalesce(col("product_name_fr"), col("product_name")).alias("product_name"),
        coalesce(col("generic_name_fr"), col("generic_name")).alias("generic_name"),
        "nova_group",
        "nutrition_grade_fr",
        "additives_count",
        "labels_tags",
        "allergens_tags",
        "nutritional_index"
    )
    
    print("Final processed data:")
    result_df.show(10, truncate=False)
    
    return result_df

def main():
    spark = create_spark_session()
    output_df = process_data(spark)
    
    output_df.write.format("mongo") \
        .mode("overwrite") \
        .option("database", "off") \
        .option("collection", "processed_products") \
        .save()
    
    print("Data successfully written to MongoDB.")
    
    spark.stop()
    print("Spark session stopped.")

if __name__ == "__main__":
    main()