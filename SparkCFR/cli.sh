aws s3 cp s3://xgwang-spark-demo/ml-20m/ratings.csv .
aws s3 cp s3://xgwang-spark-demo/SparkCFR.jar .
hadoop fs -put ratings.csv

# m4.2xlarge
spark-submit --master yarn  --driver-memory 20g  --driver-cores 7 --num-executors 2 --executor-cores 7 --executor-memory 20g --conf spark.driver.maxResultSize=20g SparkCFR.jar 1

# m4.xlarge
spark-submit --master yarn  --driver-memory 10g  --driver-cores 3 --num-executors 2 --executor-cores 3 --executor-memory 10g --conf spark.driver.maxResultSize=10g SparkCFR.jar 1

# m4.4xlarge
spark-submit --master yarn  --driver-memory 50g  --driver-cores 14 --num-executors 2 --executor-cores 14 --executor-memory 50g --conf spark.driver.maxResultSize=50g SparkCFR.jar 1

# m4.10xlarge
spark-submit --master yarn  --driver-memory 120g  --driver-cores 35 --num-executors 3 --executor-cores 35 --executor-memory 120g --conf spark.driver.maxResultSize=120g SparkCFR.jar

# m4.16xlarge
spark-submit --master yarn  --driver-memory 200g  --driver-cores 60 --num-executors 2 --executor-cores 60 --executor-memory 200g --conf spark.driver.maxResultSize=200g SparkCFR.jar 1