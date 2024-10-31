
## path
file_rdd = spark.read.text("hdfs:///user/hadoop/log_data.txt").rdd

##  comando para copiar al hdfs
```
hdfs dfs -put log_data.txt /user/hadoop/
hdfs dfs -put logistic_data.txt /user/hadoop/
hdfs dfs -put ratings_data.txt /user/hadoop/
```

## ls hdfs
```
hdfs dfs -ls /user/hadoop/

```

## requerimientos
```
pip3 install pyspark

pip3 install numpy
```