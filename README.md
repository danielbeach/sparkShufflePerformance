# sparkShufflePerformance
testing the performance of Spark shuffle configurations

read full blog post here. https://www.confessionsofadataguy.com/how-do-spark-shuffle-configurations-affect-performance/

Using open source data, in single machine and 3 node cluster settings, test 25 and 50GB of data while changing `spark.sql.shuffle.partitions` too see how Spark reacts running a simple PySpark script.
