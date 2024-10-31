
```
hdfs dfs -put log_data.txt /user/hadoop/
hdfs dfs -put logistic_data.txt /user/hadoop/
hdfs dfs -put ratings_data.txt /user/hadoop/

```

# path

file_rdd = spark.read.text("hdfs:///user/hadoop/log_data.txt").rdd
