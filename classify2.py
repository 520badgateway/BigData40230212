# classify2.py
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
from pyspark.sql.functions import col, rand

def main():
    spark = SparkSession.builder \
        .appName("SteamSentimentCombined") \
        .master("local[*]") \
        .getOrCreate()

    def load(path):
        return spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .csv(f"file://{path}") \
            .withColumnRenamed("推荐", "recommended") \
            .withColumnRenamed("评论", "comment") \
            .select("recommended", "comment") \
            .filter(col("comment").isNotNull() & (col("comment") != ""))

    df1 = load("/home/hadoop/2358720_5000.csv")
    df2 = load("/home/hadoop/1180320_1000.csv")

    all_df = df1.union(df2)
    train_df, test_df = all_df.randomSplit([0.8, 0.2], seed=42)

    label_indexer = StringIndexer(
        inputCol="recommended",
        outputCol="label",
        stringOrderType="alphabetAsc"
    )

    tokenizer    = RegexTokenizer(inputCol="comment", outputCol="words", pattern="\\W+")
    stop_remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    word2Vec     = Word2Vec(
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

    # 9. 测试集评估
    pred_test = model.transform(test_df)
    acc_test = evaluator.evaluate(pred_test)
    f1_test  = MulticlassClassificationEvaluator(metricName="f1").evaluate(pred_test)
    print(f"测试集准确率 = {acc_test:.4f}，F1 = {f1_test:.4f}")

    print("\n--- 随机 10 条预测示例 ---")
    pred_test.select("comment", "recommended", "prediction") \
        .orderBy(rand())  \
        .limit(10) \
        .show(truncate=100)

    spark.stop()

if __name__ == "__main__":
    main()
