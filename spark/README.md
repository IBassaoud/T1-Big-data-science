# README : Classification Nutritionnelle OpenFoodFacts

## Introduction

Ce projet consiste à développer un système de classification nutritionnelle en utilisant Apache Spark pour traiter et analyser les données de produits provenant de MongoDB. Le système calcule un indice nutritionnel basé sur divers attributs et enregistre les données traitées dans MongoDB.

## Outils et Technologies Utilisés

- **Apache Spark** : Pour le traitement et l'analyse de données distribuées.
- **MongoDB** : Base de données NoSQL utilisée pour le stockage et la récupération des données.
- **Python** : Langage de programmation utilisé pour le script de traitement des données.
- **PySpark** : Bibliothèque Spark pour Python pour interagir avec Spark.

## Étapes et Processus

### 1. Création de la Session Spark

**Objectif** : Établir une connexion à MongoDB à l'aide de Spark.

**Code** :
```python
def create_spark_session():
    mongodb_uri = "mongodb://off_admin:admin@off-db:27017/?authMechanism=SCRAM-SHA-256&authSource=off"
    
    spark = SparkSession.builder \
        .appName("OpenFoodFacts Classification Nutritionnelle") \
        .config("spark.mongodb.input.uri", mongodb_uri) \
        .config("spark.mongodb.output.uri", mongodb_uri) \
        .config("spark.ui.port", "4040") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.shuffle.file.buffer", "32k") \
        .config("spark.reducer.maxSizeInFlight", "96m") \
        .config("spark.network.timeout", "800s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.executor.memory", "4G") \
        .config("spark.executor.cores", "2") \
        .config("spark.driver.memory", "2G") \
        .getOrCreate()
    
    print("Session Spark créée avec succès.")
    return spark
```

### 2. Chargement et Filtrage des Données

**Objectif** : Charger les données des produits depuis MongoDB et filtrer les enregistrements incomplets ou non pertinents.

**Code** :
```python
def load_and_filter_data(spark):
    df = spark.read.format("mongo").option("database", "off").option("collection", "products").load()
    
    filtered_df = df.filter(
        ((col("product_name_fr").isNotNull()) & (col("product_name_fr") != "")) |
        ((col("generic_name_fr").isNotNull()) & (col("generic_name_fr") != ""))
    )
    print(f"Chargement de {filtered_df.count()} produits après filtrage.")
    return filtered_df
```

### 3. Calcul du Score Nutritionnel

**Objectif** : Calculer un indice nutritionnel basé sur le Nutriscore, le score NOVA, les additifs, les labels bio, et les allergènes.

**Code** :
```python
def calculate_nutritional_index(df):
    def count_additives(additives_tags):
        if additives_tags:
            return len([tag for tag in additives_tags if tag.startswith("en:e")])
        return 0
    
    count_additives_udf = udf(count_additives, IntegerType())
    
    df = df.withColumn("nutriscore_score",
        when(col("nutrition_grade_fr").isin("a", "b"), 5)
        .when(col("nutrition_grade_fr") == "c", 3)
        .when(col("nutrition_grade_fr").isin("d", "e"), 1)
        .otherwise(0)
    )
    
    df = df.withColumn("nova_score",
        when(col("nova_group") == 1, 5)
        .when(col("nova_group") == 2, 4)
        .when(col("nova_group") == 3, 2)
        .when(col("nova_group") == 4, 0)
        .otherwise(3)
    )
    
    df = df.withColumn("additives_count", count_additives_udf(col("additives_tags")))
    df = df.withColumn("additives_penalty", 
        when(col("additives_count") == 0, 0)
        .when(col("additives_count") <= 2, -1)
        .when(col("additives_count") <= 5, -2)
        .otherwise(-3)
    )
    
    df = df.withColumn("organic_bonus", 
        when(array_contains(col("labels_tags"), "en:organic"), 1).otherwise(0)
    )
    
    df = df.withColumn("allergens_penalty", 
        when(size(col("allergens_tags")) > 0, -1).otherwise(0)
    )
    
    df = df.withColumn("nutritional_index", 
        (col("nutriscore_score") + col("nova_score") + col("additives_penalty") + 
         col("organic_bonus") + col("allergens_penalty")) / 2
    )
    
    df = df.withColumn("nutritional_index", 
        when(col("nutritional_index") < 1, 1)
        .when(col("nutritional_index") > 5, 5)
        .otherwise(col("nutritional_index"))
    )
    
    return df
```

### 4. Traitement des Données et Sauvegarde des Résultats

**Objectif** : Traiter les données filtrées pour calculer l'indice nutritionnel et enregistrer les résultats dans MongoDB.

**Code** :
```python
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
    
    return result_df

def main():
    spark = create_spark_session()
    output_df = process_data(spark)
    
    output_df.write.format("mongo") \
        .mode("overwrite") \
        .option("database", "off") \
        .option("collection", "processed_products") \
        .save()
    
    print("Données écrites avec succès dans MongoDB.")
    
    spark.stop()
    print("Session Spark arrêtée.")

if __name__ == "__main__":
    main()
```

## Résultats

- **Calcul de l'Indice Nutritionnel** : Le script calcule avec succès un indice nutritionnel et le normalise entre 1 et 5.
- **Sortie des Données** : Les données traitées sont enregistrées dans la collection `processed_products` dans MongoDB.

### Exemple de sortie
```textfile
2024-09-13 14:29:14 +-----------------+------------------+--------------+
2024-09-13 14:29:14 |              _id|nutrition_grade_fr|nutrient_score|
2024-09-13 14:29:14 +-----------------+------------------+--------------+
2024-09-13 14:29:14 |               00|                 c|             3|
2024-09-13 14:29:14 |            00000|           unknown|             0|
2024-09-13 14:29:14 |           000000|                 b|             5|
2024-09-13 14:29:14 |  000000000000000|           unknown|             0|
2024-09-13 14:29:14 |00000000000000225|           unknown|             0|
2024-09-13 14:29:14 +-----------------+------------------+--------------+
```

## Dépôt Git

- **URL du Dépôt** : [Classification Nutritionnelle OpenFoodFacts](https://github.com/IBassaoud/T1-Big-data-science)

## Conclusion et Perspectives

Ce projet démontre l'utilisation efficace d'Apache Spark pour le traitement de grandes quantités de données nutritionnelles. L'indice nutritionnel calculé fournit une évaluation multifactorielle de la qualité des aliments, prenant en compte non seulement les nutriments mais aussi le degré de transformation, la présence d'additifs et d'allergènes, ainsi que le caractère biologique des produits.

Pour améliorer ce projet, on pourrait envisager :
1. L'ajout de visualisations des résultats pour une meilleure compréhension des tendances.
2. L'intégration de techniques d'apprentissage automatique pour affiner la classification.
3. L'optimisation des performances de Spark pour traiter des volumes de données encore plus importants.