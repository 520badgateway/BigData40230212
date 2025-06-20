#!/home/hadoop/anaconda3/bin/python
# -*- coding: utf-8 -*-
import sys
import csv
import jieba

# 停用词
stopwords = set()
with open('stopwords.txt', encoding='utf-8') as f:
    for line in f:
        w = line.strip()
        if w:
            stopwords.add(w)

def bucket_pairs(words):
    """对排序后的词列表两两组合，保证字典序 w[i]<w[j]"""
    n = len(words)
    for i in range(n):
        for j in range(i+1, n):
            yield words[i], words[j]

reader = csv.reader(sys.stdin)
# 跳过 header
next(reader, None)

for cols in reader:
    comment = cols[3].strip()
    if not comment:
        continue

    # 分词
    tokens = [tk for tk in jieba.lcut(comment)
              if tk.strip() and tk not in stopwords and len(tk) > 1]

    # 去重并排序
    uniq = sorted(set(tokens))

    for w1, w2 in bucket_pairs(uniq):
        print(f"{w1},{w2}\t1")
