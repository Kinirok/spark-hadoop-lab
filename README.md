Порядок команд для запуска:

- mkdir -p results

- python generate_data.py


- docker compose up -d namenode datanode1 spark-master spark-worker

- docker exec namenode hdfs dfs -mkdir -p /user/data
- docker cp data/sales_dataset.csv namenode:/tmp/
- docker exec namenode hdfs dfs -put -f /tmp/sales_dataset.csv /user/data/

- docker exec -it spark-master bash -c "apt-get update && apt-get install -y python3-pip && pip3 install pyspark==3.5.0"
- docker exec spark-master python3 /opt/spark-apps/spark_app.py exp1_1dn_spark false true


- docker exec spark-master python3 /opt/spark-apps/spark_app.py exp2_1dn_opt true true

- docker compose down


- docker compose --profile multi-dn up -d namenode datanode1 datanode2 datanode3 spark-master spark-worker



- docker exec namenode hdfs dfs -setrep -R 3 /user/data


- docker exec -it spark-master bash -c "apt-get update && apt-get install -y python3-pip && pip3 install pyspark==3.5.0"
- docker exec spark-master python3 /opt/spark-apps/spark_app.py exp3_3dn_spark false true



- docker exec spark-master python3 /opt/spark-apps/spark_app.py exp4_3dn_opt true true

- docker compose --profile multi-dn down


- python analyze_results.py
