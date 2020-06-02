gcloud dataproc jobs submit pyspark sessions.py --cluster=clustername --region=us-central1 --jars=gs://webev
ents/jars/spark-streaming-kafka-0-10-assembly_2.11-2.4.5.jar,gs://webevents/jars/spark-sql-kafka-0-10_2.11-2.4.5.jar  --properties spark.jars.packages=org.apache.
spark:spark-avro_2.11:2.4.2
