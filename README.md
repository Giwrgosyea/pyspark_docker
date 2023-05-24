## Spark task

Build on MACOS using docker

What is included:
- Pyspark 3.0.2
- Hadoop
- mongo:latest as noSQL DB

The project is developed using docker-compose
to start do:
```
docker build -t cluster-apache-spark:3.0.2 .
```
After building:
```
docker-compose up --build

to view logs of the docker: 
docker-compose logs -f  

wait until all containers are up ... while take a bit.
```


While building all the neccessary jars will be downloaded to establish mongo connection

To execute the task:
```
docker exec -it docker-spark-cluster-spark-master-1 bash

Then do

bin/spark-submit --master spark://spark-master:7077 ../spark-apps/exercise.py

```

To view data stored in mongodb :
connect via compass in : 
```
mongodb://localhost:27017
```
under local.task we can view the stored data


To view the stored data in hdfs we can do:
```
docker exec -it docker-spark-cluster-spark-master-1 bash

/spark/bin/pyspark --master spark://spark-master:7077

spark.read.parquet("hdfs://namenode:9000/merged.parquet").show(1000)
```