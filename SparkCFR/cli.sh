aws s3 cp s3://xgwang-spark-demo/ml-20m/movies.csv .
aws s3 cp s3://xgwang-spark-demo/SparkCFR.jar .
spark-submit SparkCFR.jar
