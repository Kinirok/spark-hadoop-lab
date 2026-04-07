#!/usr/bin/env python3
"""Spark Application with performance monitoring"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import json
import os
import subprocess
from datetime import datetime

class SparkLabExperiment:
    def __init__(self, experiment_name, use_optimizations=False, use_hdfs=False):
        self.experiment_name = experiment_name
        self.use_optimizations = use_optimizations
        self.use_hdfs = use_hdfs
        self.metrics = {
            "experiment": experiment_name,
            "optimized": use_optimizations,
            "hdfs": use_hdfs,
            "start_time": None,
            "end_time": None,
            "duration_seconds": None,
            "memory_used_mb": None,
            "stages_count": 0,
            "tasks_count": 0,
            "shuffle_read_bytes": 0,
            "shuffle_write_bytes": 0,
            "input_bytes": 0
        }
    
    def get_container_memory(self):
        try:
            with open('/proc/meminfo', 'r') as f:
                for line in f:
                    if 'MemTotal' in line:
                        total_mem = int(line.split()[1]) / 1024  # в MB
                    if 'MemAvailable' in line:
                        available_mem = int(line.split()[1]) / 1024  # в MB
                
                used_mem = total_mem - available_mem
                if used_mem > 0:
                    return used_mem
        except Exception as e:
            print(f"Warning: Could not get memory stats: {e}")
            return 0
        
    def create_spark_session(self):
        if self.use_hdfs:
            master_url = "spark://spark-master:7077"
        else:
            master_url = "local[2]"
        
        builder = SparkSession.builder \
            .appName(self.experiment_name) \
            .config("spark.master", master_url) \
            .config("spark.executor.memory", "1g") \
            .config("spark.executor.cores", "2") \
            .config("spark.driver.memory", "1g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.ui.port", "4040")
        
        if self.use_optimizations:
            print("⚡ Applying optimizations...")
            builder = builder \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.autoBroadcastJoinThreshold", "10MB") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        spark = builder.getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        
        return spark
    
    def load_data(self, spark):

        if self.use_hdfs:
            hdfs_path = "hdfs://namenode:9000/user/data/sales_dataset.csv"
            df = spark.read.csv(hdfs_path, header=True, inferSchema=True)
        else:
            local_path = "/opt/spark-apps/data/sales_dataset.csv"
            df = spark.read.csv(local_path, header=True, inferSchema=True)
        
        
        if self.use_optimizations:
            df = df.repartition(8)
            df = df.cache()

            df.count()
        
        return df
    
    def analyze_data(self, spark, df):

        total_rows = df.count()
        self.metrics["input_bytes"] = total_rows * 200  
        

        category_revenue = df.groupBy("category") \
            .agg(
                sum("total_price").alias("total_revenue"),
                count("*").alias("transaction_count"),
                avg("total_price").alias("avg_transaction")
            ) \
            .orderBy(col("total_revenue").desc())
        category_revenue.show()
        

        top_products = df.groupBy("product") \
            .agg(
                sum("quantity").alias("total_quantity"),
                sum("total_price").alias("revenue")
            ) \
            .orderBy(col("revenue").desc()) \
            .limit(5)
        top_products.show()
        

        country_sales = df.groupBy("country") \
            .agg(
                sum("total_price").alias("revenue"),
                avg("rating").alias("avg_rating"),
                count("*").alias("orders")
            ) \
            .orderBy(col("revenue").desc())
        country_sales.show()
        

        return_analysis = df.groupBy("category") \
            .agg(
                (sum(when(col("is_returned") == "true", 1).otherwise(0)) / count("*") * 100).alias("return_rate_%"),
                sum("total_price").alias("revenue")
            ) \
            .orderBy(col("return_rate_%").desc())
        return_analysis.show()

        segment_analysis = df.groupBy("customer_segment") \
            .agg(
                avg("total_price").alias("avg_order_value"),
                count("*").alias("order_count"),
                avg("rating").alias("avg_rating")
            ) \
            .orderBy(col("avg_order_value").desc())
        segment_analysis.show()

        sc = spark.sparkContext
        try:
            status_tracker = sc.statusTracker()
            job_ids = status_tracker.getJobIdsForGroup(None)
            self.metrics["stages_count"] = len(job_ids)
        except:
            pass
        
        return {
            "total_rows": total_rows,
            "category_revenue": category_revenue.collect(),
            "top_products": top_products.collect(),
            "country_sales": country_sales.collect()
        }
    
    def save_results(self, results, spark):
        output_dir = f"/opt/spark-apps/results/{self.experiment_name}"
        os.makedirs(output_dir, exist_ok=True)
        
        with open(f"{output_dir}/metrics.json", 'w') as f:
            json.dump(self.metrics, f, indent=2, default=str)

        if self.use_optimizations and results:
            try:
                for key, value in results.items():
                    if isinstance(value, list) and len(value) > 0:

                        if value and hasattr(value[0], 'asDict'):
                            rows = [row.asDict() for row in value]
                            result_df = spark.createDataFrame(rows)
                            result_df.write.mode("overwrite").parquet(f"{output_dir}/{key}")
            except Exception as e:
                print(f"Warning: Could not save parquet files: {e}")

    def run(self):        
        self.metrics["start_time"] = datetime.now().isoformat()
        start_time = time.time()

        mem_before = self.get_container_memory()
        
        spark = self.create_spark_session()
        
        try:
            df = self.load_data(spark)
            results = self.analyze_data(spark, df)
            self.save_results(results, spark)
            
        finally:
            elapsed = time.time() - start_time
            self.metrics["duration_seconds"] = elapsed
            self.metrics["end_time"] = datetime.now().isoformat()
            
          
            mem_after = self.get_container_memory()
            memory_diff = mem_after - mem_before
            self.metrics["memory_used_mb"] = memory_diff if memory_diff > 0 else 0

            with open(f"/opt/spark-apps/results/{self.experiment_name}/metrics.json", 'w') as f:
                json.dump(self.metrics, f, indent=2, default=str)
            
            spark.stop()


if __name__ == "__main__":
    import sys
    
    experiment = sys.argv[1] if len(sys.argv) > 1 else "test_experiment"
    optimized = sys.argv[2].lower() == "true" if len(sys.argv) > 2 else False
    use_hdfs = sys.argv[3].lower() == "true" if len(sys.argv) > 3 else False
    
    lab = SparkLabExperiment(experiment, optimized, use_hdfs)
    lab.run()