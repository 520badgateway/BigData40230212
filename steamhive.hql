-- ① 创建并使用数据库
CREATE DATABASE IF NOT EXISTS dbpy;
USE dbpy;

-- ② 创建外部表并加载 HDFS 下的数据
CREATE EXTERNAL TABLE IF NOT EXISTS steam_reviews (
  username STRING,              -- 用户名
  recommendation STRING,   -- 推荐
  playtime STRING,                -- 游戏时长
  comment STRING               -- 评论内容
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/2358720/dataset/'
TBLPROPERTIES (
  "skip.header.line.count"="1"  -- 跳过 CSV 首行表头
);

-- ③ 数据清洗视图
CREATE OR REPLACE VIEW steam_reviews_clean AS
SELECT
  username,
  
  -- 1) recommendation，只保留两种值，其他全部映射为 NULL
  CASE
    WHEN recommendation RLIKE '^Recommended$' THEN 'Recommended'
    WHEN recommendation RLIKE '^Not Recommended$' THEN 'Not Recommended'
    ELSE NULL
  END  AS recommendation_clean,
  
  -- 2) 从 playtime 字段里提取数字部分，转成 DOUBLE,如果没提取到，会是 NULL
  CAST(
    REGEXP_EXTRACT(playtime, '([0-9]+\\.?[0-9]*)', 1)
    AS DOUBLE
  ) AS hours_played,

  comment
FROM steam_reviews;

-- ④ 查看表中前 10 行数据
SELECT * 
FROM steam_reviews_clean
LIMIT 10;

-- ⑤ 统计好评 vs 差评 数量
SELECT
  recommendation_clean,
  COUNT(*) AS cnt
FROM steam_reviews_clean
WHERE recommendation_clean IS NOT NULL
GROUP BY recommendation_clean
ORDER BY cnt DESC;

-- ⑥ 计算平均游戏时长
SELECT
  ROUND(AVG(hours_played),2) AS avg_hours_played
FROM steam_reviews_clean
WHERE hours_played IS NOT NULL;

-- ⑦ 评论长度分布
SELECT
  CASE
    WHEN LENGTH(comment)<50   THEN '<50'
    WHEN LENGTH(comment)<100  THEN '50-100'
    WHEN LENGTH(comment)<200  THEN '100-200'
    ELSE '>=200'
  END AS length_bucket,
  COUNT(*) AS cnt
FROM steam_reviews_clean
GROUP BY
  CASE
    WHEN LENGTH(comment)<50   THEN '<50'
    WHEN LENGTH(comment)<100  THEN '50-100'
    WHEN LENGTH(comment)<200  THEN '100-200'
    ELSE '>=200'
  END
ORDER BY length_bucket;

-- ⑧ 列举积极评论
SELECT
  username,
  comment
FROM steam_reviews_clean
WHERE recommendation_clean = 'Recommended'
  AND LENGTH(comment) > 15
  AND comment LIKE '%好%'
LIMIT 5;

