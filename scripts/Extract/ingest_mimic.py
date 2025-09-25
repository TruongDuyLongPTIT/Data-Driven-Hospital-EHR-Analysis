from config import create_spark_session, DB_CONFIG, MINIO_CONFIG, MIMIC_TABLES, TABLE_CONFIG
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import time

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MimicIngestion:
    def __init__(self):
        self.spark = create_spark_session()
        self.table_config = TABLE_CONFIG

    def create_minio_bucket(self):
        """Create MinIO bucket if not exists"""
        try:
            # Tạm thời tạo bucket trên MinIO UI
            # Sau code đoạn này sau
            logger.info("MinIO bucket: mimic-lakehouse is ready")
        except Exception as e:
            logger.error(f"Error creating bucket: {e}")

    def ingest_table(self, table_name):
        self.spark.stop()
        self.spark = create_spark_session()
        """Ingest single table"""
        try:
            logger.info(f"🚀 Starting ingestion for table: {table_name}")
            start_time = time.time()
            
            # Get table config
            config = self.table_config.get(table_name, {"num_partition": 1, "fetch_size": 10000})
            
            # Build JDBC options
            jdbc_options = {
                "url": DB_CONFIG["url"],
                "dbtable": table_name,
                "user": DB_CONFIG["user"], 
                "password": DB_CONFIG["password"],
                "driver": DB_CONFIG["driver"],
                "fetchsize": str(config["fetch_size"])
            }
            
            # Add partitioning if configured
            if "partition_column" in config:
                jdbc_options.update({
                    "partitionColumn": config["partition_column"],
                    "lowerBound": str(config["partition_bounds"][0]),
                    "upperBound": str(config["partition_bounds"][1]),
                    "numPartitions": str(config["num_partition"])
                })
                logger.info(f"Using {config['num_partition']} partitions for {table_name}")
            
            # Read from PostgreSQL
            df = self.spark.read.format("jdbc").options(**jdbc_options).load()
            
            # Add metadata columns
            df = df.withColumn("ingestion_timestamp", current_timestamp()) \
                   .withColumn("source_system", lit("mimic4")) \
                   .withColumn("table_name", lit(table_name))
            
            # Count records
            record_count = df.count()
            logger.info(f"📊 Extracted {record_count:,} records from {table_name}")
            
            # Write to MinIO Bronze layer
            output_path = f"{MINIO_CONFIG['bronze_path']}/{table_name}"

            df.write.mode("overwrite") \
            .format("parquet") \
            .option("path", output_path) \
            .save()

            
            end_time = time.time()
            duration = end_time - start_time
            
            logger.info(f"✅ Successfully ingested {table_name} in {duration:.2f} seconds")
            
            # THÊM: Clear cache và unpersist DataFrame
            df.unpersist(blocking=True)
            self.spark.catalog.clearCache()
            
            # THÊM: Force garbage collection
            import gc
            gc.collect()

            self.spark.stop()

            return {
                "table": table_name,
                "success": True, 
                "records": record_count,
                "duration": duration
            }
            
        except Exception as e:
            logger.error(f"❌ Error ingesting {table_name}: {str(e)}")
            return {
                "table": table_name,
                "success": False,
                "error": str(e),
                "records": 0,
                "duration": 0
            }

    def ingest_tables_sequential(self, table_names):
        """Ingest tables one by one"""
        logger.info(f"🔄 Starting sequential ingestion for {len(table_names)} tables")
        
        results = []
        total_start = time.time()
        
        for table_name in table_names:
            result = self.ingest_table(table_name)
            results.append(result)
        
        total_end = time.time()
        
        # Print summary
        self.print_summary(results, total_end - total_start)
        return results


    def print_summary(self, results, total_duration, parallel=False):
        """Print ingestion summary"""
        mode = "PARALLEL" if parallel else "SEQUENTIAL"
        
        print("\n" + "="*70)
        print(f"📈 MIMIC-IV INGESTION SUMMARY ({mode})")
        print("="*70)
        
        # Convert to list và tính toán trước
        results_list = list(results) if not isinstance(results, list) else results
        successful = [r for r in results_list if r['success']]
        failed = [r for r in results_list if not r['success']]
        
        # Tính total_records bằng cách khác
        total_records = 0
        for r in successful:
            total_records += r['records']
        
        print(f"⏱️  Total Duration: {total_duration:.2f} seconds")
        print(f"📊 Tables Processed: {len(results_list)}")
        print(f"✅ Successful: {len(successful)}")
        print(f"❌ Failed: {len(failed)}")
        print(f"📄 Total Records: {total_records:,}")
        if total_duration > 0:
            print(f"🚀 Records/Second: {total_records/total_duration:,.0f}")

    def validate_connections(self):
        """Test connections to PostgreSQL and MinIO"""
        logger.info("🔍 Validating connections...")
        
        try:
            # Test PostgreSQL connection
            test_df = self.spark.read.format("jdbc") \
                .option("url", DB_CONFIG["url"]) \
                .option("dbtable", "(SELECT 1 as test) as test_query") \
                .option("user", DB_CONFIG["user"]) \
                .option("password", DB_CONFIG["password"]) \
                .option("driver", DB_CONFIG["driver"]) \
                .load()
            
            test_count = test_df.count()
            logger.info("✅ PostgreSQL connection successful")
            
        except Exception as e:
            logger.error(f"❌ PostgreSQL connection failed: {str(e)}")
            return False
        
        try:
            # Test MinIO connection by trying to list buckets
            # For now, assume it's working if no error
            logger.info("✅ MinIO connection assumed successful")
            
        except Exception as e:
            logger.error(f"❌ MinIO connection failed: {str(e)}")
            return False
        
        return True

    def stop(self):
        """Stop Spark session"""
        logger.info("🛑 Stopping Spark session...")
        self.spark.stop()
        logger.info("✅ Spark session stopped")


# Main execution function
def main():
    
    # Initialize ingestion
    ingestion = MimicIngestion()
    
    try:
        # Validate connections
        if not ingestion.validate_connections():
            logger.error("❌ Connection validation failed. Exiting...")
            return
        
        # Create bucket
        ingestion.create_minio_bucket()
        
        print("Ingesting...")
        
        # Run ingest process
        results = ingestion.ingest_tables_sequential(MIMIC_TABLES)

        
        # Check if any tables failed
        failed_count = len([r for r in results if not r['success']])
        if failed_count > 0:
            logger.warning(f"⚠️  {failed_count} tables failed. Check logs for details.")
        else:
            logger.info("🎉 All tables ingested successfully!")
            
    except KeyboardInterrupt:
        logger.info("🛑 Ingestion interrupted by user")
    except Exception as e:
        logger.error(f"💥 Unexpected error: {str(e)}")
    finally:
        ingestion.stop()

if __name__ == "__main__":
    main()