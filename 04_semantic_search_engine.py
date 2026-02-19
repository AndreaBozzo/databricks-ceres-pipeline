# Databricks notebook source
# COMMAND ----------
import logging

from pyspark.sql.functions import (
    array, array_except, col, concat_ws, lit, lower,
    regexp_replace, size, split,
)
from config import SILVER_TABLE, GOLD_ML_FEATURES, NUM_FEATURES, TITLE_WEIGHT

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ceres.search")

# Multilingual stopwords (hardcoded to avoid Spark ML on serverless)
STOPWORDS = sorted(set([
    # English core
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and",
    "any", "are", "as", "at", "be", "because", "been", "before", "being", "below",
    "between", "both", "but", "by", "can", "could", "did", "do", "does", "doing",
    "down", "during", "each", "few", "for", "from", "further", "get", "got", "had",
    "has", "have", "having", "he", "her", "here", "hers", "herself", "him",
    "himself", "his", "how", "i", "if", "in", "into", "is", "it", "its", "itself",
    "just", "me", "might", "more", "most", "must", "my", "myself", "no", "nor",
    "not", "now", "of", "off", "on", "once", "only", "or", "other", "our", "ours",
    "ourselves", "out", "over", "own", "re", "same", "she", "should", "so", "some",
    "such", "than", "that", "the", "their", "theirs", "them", "themselves", "then",
    "there", "these", "they", "this", "those", "through", "to", "too", "under",
    "until", "up", "very", "was", "we", "were", "what", "when", "where", "which",
    "while", "who", "whom", "why", "will", "with", "would", "you", "your", "yours",
    "yourself", "yourselves",
    # Italian
    "che", "chi", "ci", "come", "con", "contro", "cui", "da", "dal", "dalla",
    "dalle", "dallo", "dei", "del", "della", "delle", "dello", "di", "dopo",
    "e", "ed", "era", "fa", "gli", "ha", "hai", "hanno", "ho", "il", "io",
    "la", "le", "lei", "li", "lo", "lui", "ma", "mi", "mia", "miei", "mio",
    "ne", "nei", "nel", "nella", "nelle", "nello", "noi", "non", "nostra",
    "nostro", "ogni", "per", "poi", "prima", "quando", "quello", "questa",
    "questo", "qui", "se", "sei", "si", "sia", "sono", "sta", "stato", "su",
    "sua", "sue", "sui", "sul", "sulla", "sulle", "sullo", "suo", "ti", "tra",
    "tu", "tua", "tue", "tuo", "tuoi", "tutti", "tutto", "un", "una", "uno", "vi", "voi",
    # German
    "aber", "alle", "allem", "allen", "aller", "alles", "also", "auf", "aus",
    "bei", "bin", "bis", "da", "damit", "dann", "das", "dass", "dem", "den",
    "der", "des", "die", "dies", "diese", "diesem", "diesen", "dieser", "doch",
    "du", "durch", "ein", "eine", "einem", "einen", "einer", "er", "es",
    "euch", "hat", "ich", "ihm", "ihn", "ihnen", "ihr", "ihre", "ihrem",
    "ihren", "ihrer", "im", "ist", "ja", "kann", "kein", "keine", "man",
    "mein", "meine", "meinem", "meinen", "meiner", "mit", "nach", "nicht",
    "noch", "nur", "ob", "oder", "ohne", "sehr", "sein", "seine", "seinem",
    "seinen", "seiner", "sich", "sie", "sind", "so", "und", "uns", "unser",
    "unter", "vom", "von", "vor", "war", "was", "weil", "wenn", "wer", "wie",
    "wir", "wird", "wo", "zu", "zum", "zur",
    # French
    "au", "aux", "avec", "ce", "ces", "dans", "de", "des", "du", "elle",
    "en", "et", "eux", "il", "je", "les", "leur", "lui", "mais", "mes",
    "mon", "ne", "nos", "notre", "nous", "on", "ou", "par", "pas", "pour",
    "qu", "que", "qui", "sa", "se", "ses", "son", "sur", "ta", "te", "tes",
    "toi", "ton", "une", "vos", "votre", "vous",
    # Spanish
    "como", "del", "el", "en", "es", "esta", "este", "esto", "las", "lo",
    "los", "mas", "ni", "nos", "para", "pero", "por", "sin", "sobre", "sus",
    "unas", "unos", "ya", "yo",
    # Domain-specific high-frequency terms (act as manual IDF boost)
    "data", "dataset", "datasets", "open", "public", "information", "service",
    "services", "government", "national", "system", "report", "number",
    "dati", "dato", "aperto", "aperti", "informazioni", "servizio", "servizi",
    "daten", "offene", "donnees", "datos", "abiertos",
]))

# COMMAND ----------
# Load Silver data and build text_soup
df_source = spark.read.table(SILVER_TABLE)
source_count = df_source.count()
logger.info("Loaded %d records from %s.", source_count, SILVER_TABLE)

title_cols = [col("title")] * TITLE_WEIGHT
df_text = df_source.withColumn("text_soup",
    lower(concat_ws(" ", *title_cols, col("tags"), col("description")))
).withColumn("text_soup", regexp_replace(col("text_soup"), "[^\\w\\s]", ""))

# Tokenize and remove stopwords via SQL array functions
stopwords_arr = array(*[lit(w) for w in STOPWORDS])
df_words = df_text.withColumn("words_raw", split(col("text_soup"), "\\s+")) \
                  .withColumn("words", array_except(col("words_raw"), stopwords_arr)) \
                  .filter(size(col("words")) > 0)

df_words.createOrReplaceTempView("v_tokenized")
logger.info("Tokenized with %d stopwords removed.", len(STOPWORDS))

# COMMAND ----------
# TF-IDF via SQL: hash words to buckets, compute TF and IDF, store as map
total_docs = source_count

# Explode words, hash to feature buckets, compute TF per doc per bucket
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW v_tf AS
    SELECT title, portal, url, bucket, COUNT(*) as tf
    FROM (
        SELECT title, portal, url, word,
               abs(hash(word)) % {NUM_FEATURES} as bucket
        FROM v_tokenized
        LATERAL VIEW explode(words) t AS word
        WHERE length(word) > 1
    )
    GROUP BY title, portal, url, bucket
""")

# IDF per bucket
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW v_idf AS
    SELECT bucket, LN({total_docs}.0 / COUNT(DISTINCT concat(title, '||', url))) as idf_score
    FROM (
        SELECT title, url, abs(hash(word)) % {NUM_FEATURES} as bucket
        FROM v_tokenized
        LATERAL VIEW explode(words) t AS word
        WHERE length(word) > 1
    )
    GROUP BY bucket
""")

# Store TF-IDF as a sparse map: map<int, double> (bucket -> tfidf score)
df_features = spark.sql("""
    SELECT t.title, t.portal, t.url,
           map_from_arrays(
               collect_list(t.bucket),
               collect_list(t.tf * i.idf_score)
           ) as features_map
    FROM v_tf t
    JOIN v_idf i ON t.bucket = i.bucket
    GROUP BY t.title, t.portal, t.url
""")

df_features.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_ML_FEATURES)

ml_count = spark.read.table(GOLD_ML_FEATURES).count()
assert ml_count > 0, f"ML features table {GOLD_ML_FEATURES} is empty"
logger.info("ML features stored: %d records with sparse TF-IDF maps in %s.", ml_count, GOLD_ML_FEATURES)

# COMMAND ----------
# Search: vectorize query through the same SQL pipeline, compute dot product
def search_engine(query_text, top_k=10, portal_filter=None):
    # 1. Tokenize query using the same hash function (Spark SQL hash via temp view)
    query_words = [w.lower() for w in query_text.split() if len(w) > 1]
    query_words = [w for w in query_words if w not in set(STOPWORDS)]
    if not query_words:
        return spark.createDataFrame([], "title STRING, portal STRING, score DOUBLE, url STRING")

    # Hash query words using the same Spark hash() function
    word_literals = ", ".join(f"'{w}'" for w in query_words)
    q_buckets = spark.sql(f"""
        SELECT abs(hash(word)) % {NUM_FEATURES} as bucket, COUNT(*) as tf
        FROM (SELECT explode(array({word_literals})) as word)
        GROUP BY 1
    """)

    # Apply IDF weights and collect query vector
    q_rows = q_buckets.join(
        spark.sql("SELECT bucket, idf_score FROM v_idf"), "bucket"
    ).selectExpr("bucket", "tf * idf_score as weight").collect()

    if not q_rows:
        return spark.createDataFrame([], "title STRING, portal STRING, score DOUBLE, url STRING")

    # Build dot product expression: sum of features_map[bucket] * weight for each query bucket
    dot_terms = " + ".join(
        f"coalesce(features_map[{row['bucket']}], 0.0) * {row['weight']}"
        for row in q_rows
    )

    df_base = spark.read.table(GOLD_ML_FEATURES)
    if portal_filter:
        df_base = df_base.filter(col("portal") == portal_filter)
    df_base.createOrReplaceTempView("_search_base")

    results = spark.sql(f"""
        SELECT title, portal, url, score FROM (
            SELECT title, portal, url, ({dot_terms}) as score
            FROM _search_base
            WHERE features_map IS NOT NULL
        )
        WHERE score > 0
        ORDER BY score DESC
        LIMIT {top_k}
    """)

    return results

# COMMAND ----------
# Interactive Demo Widget
dbutils.widgets.text("search_query", "population health", "Search Dataset:")
user_query = dbutils.widgets.get("search_query")

if user_query:
    logger.info("Searching for: %s", user_query)
    display(search_engine(user_query))
