#!/bin/bash
INPUT=/2358720/dataset/2358720_5000.csv
OUTPUT=/2358720/cooccurrence_out

hdfs dfs -rm -r $OUTPUT

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files mapper.py,reducer.py,stopwords.txt \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input $INPUT \
  -output $OUTPUT

