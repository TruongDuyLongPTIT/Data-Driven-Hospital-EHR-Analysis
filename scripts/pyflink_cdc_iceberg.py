import json
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction, ProcessFunction
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.typeinfo import Types
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from datetime import datetime
import uuid
import io

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CDCMessageParser(MapFunction):
    """Parse CDC message từ Debezium"""
    
    def map(self, value):
        try:
            message = json.loads(value)
            payload = message.get('payload', {})
            
            # Skip if payload không có 'after' (có thể là schema change event)
            if not payload.get('after'):
                return None
            
            after = payload['after']
            source = payload.get('source', {})
            op = payload.get('op', 'c')  # c=create, u=update, d=delete
            
            result = {
                'itemid': after.get('itemid'),
                'label': after.get('label'),
                'abbreviation': after.get('abbreviation'),
                'linksto': after.get('linksto'),
                'category': after.get('category'),
                'unitname': after.get('unitname'),
                'param_type': after.get('param_type'),
                'lownormalvalue': after.get('lownormalvalue'),
                'highnormalvalue': after.get('highnormalvalue'),
                'operation': op,
                'ts_ms': payload.get('ts_ms', 0),
                'lsn': source.get('lsn'),
                'table_name': source.get('table')
            }
            
            return json.dumps(result)
            
        except Exception as e:
            logger.error(f"Error parsing CDC message: {str(e)}")
            return None


class IcebergSinkFunction(ProcessFunction):
    """Process function để ghi dữ liệu vào MinIO Iceberg format"""
    
    def __init__(self, bucket_name='mimic-lakehouse', table_name='d_items'):
        self.bucket_name = bucket_name
        self.table_name = table_name
        self.minio_endpoint = 'http://minio1:9000'
        self.minio_access_key = 'minio'
        self.minio_secret_key = 'minio123'
        self.batch = []
        self.batch_size = 100
        self.s3_client = None
    
    def open(self, runtime_context):
        """Initialize S3 client"""
        try:
            self.s3_client = boto3.client(
                's3',
                endpoint_url=self.minio_endpoint,
                aws_access_key_id=self.minio_access_key,
                aws_secret_access_key=self.minio_secret_key,
                region_name='us-east-1'
            )
            
            # Tạo bucket nếu chưa tồn tại
            try:
                self.s3_client.head_bucket(Bucket=self.bucket_name)
            except:
                self.s3_client.create_bucket(Bucket=self.bucket_name)
                logger.info(f"Created bucket: {self.bucket_name}")
            
            logger.info("S3 client initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing S3 client: {str(e)}")
            raise
    
    def process_element(self, value, ctx):
        """Nhận message và thêm vào batch"""
        try:
            if value:
                record = json.loads(value)
                self.batch.append(record)
                
                # Khi batch đủ size, ghi xuống S3
                if len(self.batch) >= self.batch_size:
                    self.flush()
                    
        except Exception as e:
            logger.error(f"Error in process_element: {str(e)}")
    
    def close(self):
        """Flush remaining data khi close"""
        self.flush()
    
    def flush(self):
        """Ghi batch data vào S3 dưới dạng Parquet"""
        if not self.batch:
            return
        
        try:
            # Tách deleted vs active records
            active_records = [r for r in self.batch if r.get('operation') != 'd']
            deleted_records = [r for r in self.batch if r.get('operation') == 'd']
            
            # Ghi active records vào bronze layer
            if active_records:
                self._write_parquet(active_records, layer='bronze')
            
            # Log deleted records
            if deleted_records:
                logger.info(f"Processed {len(deleted_records)} delete operations")
            
            self.batch = []
            logger.info(f"Batch flushed successfully")
            
        except Exception as e:
            logger.error(f"Error flushing batch: {str(e)}")
    
    def _write_parquet(self, records, layer='bronze'):
        """Ghi records thành Parquet file vào MinIO"""
        
        # Tạo PyArrow table từ records
        table = self._create_arrow_table(records)
        
        # Tạo S3 path
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_id = str(uuid.uuid4())[:8]
        s3_key = f"{layer}/{self.table_name}/data/{timestamp}_{file_id}.parquet"
        
        # Ghi vào S3
        try:
            buffer = io.BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=buffer.getvalue()
            )
            
            logger.info(f"✅ Written {len(records)} records to s3://{self.bucket_name}/{s3_key}")
            
        except Exception as e:
            logger.error(f"Error writing to S3: {str(e)}")
            raise
    
    def _create_arrow_table(self, records):
        """Tạo PyArrow table từ records"""
        
        data = {
            'itemid': [],
            'label': [],
            'abbreviation': [],
            'linksto': [],
            'category': [],
            'unitname': [],
            'param_type': [],
            'lownormalvalue': [],
            'highnormalvalue': [],
            'operation': [],
            'ts_ms': [],
            'lsn': [],
            'table_name': []
        }
        
        for record in records:
            data['itemid'].append(record.get('itemid'))
            data['label'].append(record.get('label'))
            data['abbreviation'].append(record.get('abbreviation'))
            data['linksto'].append(record.get('linksto'))
            data['category'].append(record.get('category'))
            data['unitname'].append(record.get('unitname'))
            data['param_type'].append(record.get('param_type'))
            data['lownormalvalue'].append(record.get('lownormalvalue'))
            data['highnormalvalue'].append(record.get('highnormalvalue'))
            data['operation'].append(record.get('operation'))
            data['ts_ms'].append(record.get('ts_ms'))
            data['lsn'].append(record.get('lsn'))
            data['table_name'].append(record.get('table_name'))
        
        # Định nghĩa schema với nullable=True để xử lý None values
        schema = pa.schema([
            ('itemid', pa.int64()),
            ('label', pa.string()),
            ('abbreviation', pa.string()),
            ('linksto', pa.string()),
            ('category', pa.string()),
            ('unitname', pa.string()),
            ('param_type', pa.string()),
            ('lownormalvalue', pa.float64()),
            ('highnormalvalue', pa.float64()),
            ('operation', pa.string()),
            ('ts_ms', pa.int64()),
            ('lsn', pa.string()),
            ('table_name', pa.string())
        ])
        
        table = pa.table(data, schema=schema)
        return table


def main():
    logger.info("🚀 Starting PyFlink CDC to Iceberg Pipeline")
    
    # Tạo execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Start with 1 for testing
    
    # Enable checkpointing
    env.enable_checkpointing(60000)  # checkpoint every 60 seconds
    
    # Kafka consumer properties
    kafka_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'flink-d-items-consumer',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': 'true'
    }
    
    # Kafka consumer
    kafka_consumer = FlinkKafkaConsumer(
        topics='postgres_db.public.d_items',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    
    logger.info("📖 Reading from Kafka topic: postgres_db.public.d_items")
    
    # Read từ Kafka
    kafka_stream = env.add_source(kafka_consumer)
    
    # Parse CDC messages
    parsed_stream = kafka_stream.map(CDCMessageParser(), output_type=Types.STRING())
    
    # Filter out None values
    filtered_stream = parsed_stream.filter(lambda x: x is not None)
    
    # Process và ghi vào MinIO
    sink_function = IcebergSinkFunction(
        bucket_name='mimic-lakehouse',
        table_name='d_items'
    )
    
    filtered_stream.process(sink_function)
    
    logger.info("✅ Pipeline configured")
    logger.info("🔄 Starting execution...")
    
    # Execute
    env.execute("PyFlink CDC to Iceberg - d_items")


if __name__ == '__main__':
    main()