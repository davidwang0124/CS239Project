aws s3 cp s3://xgwang-spark-demo/ml-20m/ratings.csv .
aws s3 cp s3://xgwang-spark-demo/SparkCFR.jar .
hadoop fs -put ratings.csv

# m4.2xlarge
spark-submit --master yarn  --driver-memory 20g  --driver-cores 6 --num-executors 4 --executor-cores 6 --executor-memory 20g --conf spark.driver.maxResultSize=20g SparkCFR-20m.jar 1

# 3 * m4.xlarge
spark-submit --master yarn  --driver-memory 10g  --driver-cores 3 --num-executors 2 --executor-cores 3 --executor-memory 10g --conf spark.driver.maxResultSize=10g SparkCFR.jar 1
