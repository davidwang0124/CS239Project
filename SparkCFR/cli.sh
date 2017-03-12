aws s3 cp s3://xgwang-spark-demo/ml-20m/movies.csv .
aws s3 cp s3://xgwang-spark-demo/ml-20m/ratings.csv .
aws s3 cp s3://xgwang-spark-demo/SparkCFR.jar .
hadoop fs -put ratings.csv
spark-submit SparkCFR.jar


