#!/bin/bash 

USER=$1

if [ -z ${USER} ]; then
    echo "Please provide your HDFS user name"
    exit 1
fi 

spark-submit --master yarn-client --driver-memory 1g --num-executors 3 --executor-memory 1g --conf spark.executor.cores=5 --class com.epam.hubd.spark.scala.core.homework.MotelsHomeRecommendation spark-core-homework-1.0.0.jar /user/$USER/spark_input/bids /user/$USER/spark_input/motels /user/maria_dev/$USER/exchange_rates /user/$USER/spark-core-output
