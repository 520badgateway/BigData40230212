# classify.py
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    RegexTokenizer,
    StopWordsRemover,
    Word2Vec,
    StringIndexer
)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("SteamSentimentWord2Vec") \
        .master("local[*]") \
        .getOrCreate()

    train_df = spark.read \
        .option("header", "true") \
        .csv("file:///home/hadoop/2358720_5000.csv") \
        .withColumnRenamed("推荐", "recommended") \
        .withColumnRenamed("评论", "comment") \
        .select("recommended", "comment") \
        .filter(col("comment").isNotNull() & (col("comment") != ""))

    test_df = spark.read \
        .option("header", "true") \
        .csv("file:///home/hadoop/1180320_1000.csv") \
        .withColumnRenamed("推荐", "recommended") \
        .withColumnRenamed("评论", "comment") \
        .select("recommended", "comment") \
        .filter(col("comment").isNotNull() & (col("comment") != ""))

    label_indexer = StringIndexer(
        inputCol="recommended",
        outputCol="label",
        stringOrderType="alphabetAsc"
    )

    tokenizer    = RegexTokenizer(inputCol="comment", outputCol="words", pattern="\\W+")
    stop_remover = StopWordsRemover(inputCol="words", outputCol="filtered")

    # Word2Vec: 100 维，忽略出现少于 5 次的词
    word2Vec = Word2Vec(
        inputCol="filtered",
        outputCol="features",
        vectorSize=100,
        minCount=5
    )

    lr = LogisticRegression(maxIter=20, regParam=0.01, elasticNetParam=0.0)

    pipeline = Pipeline(stages=[
        label_indexer,
        tokenizer,
        stop_remover,
        word2Vec,
        lr
    ])

    model = pipeline.fit(train_df)

    pred_train = model.transform(train_df)
    evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
    acc_train = evaluator.evaluate(pred_train)
    f1_train  = MulticlassClassificationEvaluator(metricName="f1").evaluate(pred_train)
    print(f"训练集准确率 = {acc_train:.4f}，F1 = {f1_train:.4f}")

    pred_test = model.transform(test_df)
    acc_test = evaluator.evaluate(pred_test)
    f1_test  = MulticlassClassificationEvaluator(metricName="f1").evaluate(pred_test)
    print(f"测试集准确率 = {acc_test:.4f}，F1 = {f1_test:.4f}")

    print("\n--- 预测错误示例 (前 5 条) ---")
    pred_test.select("comment", "recommended", "prediction") \
        .where(col("label") != col("prediction")) \
        .show(5, truncate=100)

    spark.stop()

if __name__ == "__main__":
    main()
