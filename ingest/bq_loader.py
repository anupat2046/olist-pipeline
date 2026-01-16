"""
=============================================================================
BigQuery Loader
=============================================================================

โหลดข้อมูล Parquet จาก GCS เข้า BigQuery
ใช้ LoadJobConfig เพื่อตั้งค่า schema และ partitioning
=============================================================================
"""

import os
import sys
from pathlib import Path
from typing import Optional, List

import yaml
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core import exceptions as gcp_exceptions

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.logger import get_logger


class BigQueryLoader:
    """
    ตัวโหลดข้อมูลเข้า BigQuery
    
    อ่าน Parquet จาก GCS แล้วสร้างตารางใน BigQuery
    รองรับ Hive partitioning (year/month)
    """
    
    def __init__(self, config_path: str = "config/settings.yaml"):
        self.logger = get_logger("bq_loader")
        self.config = self._load_config(config_path)
        self.client = self._get_bq_client()
        
        self.project_id = self.config["gcp"]["project_id"]
        self.dataset_name = self.config["gcp"]["dataset_name"]
        self.bucket_name = self.config["gcp"]["bucket_name"]
        self.location = self.config["gcp"]["location"]
        
    def _load_config(self, config_path: str) -> dict:
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            self.logger.info(f"โหลด config จาก {config_path}")
            return config
        except FileNotFoundError:
            self.logger.error(f"หาไฟล์ config ไม่เจอ: {config_path}")
            raise
        except yaml.YAMLError as e:
            self.logger.error(f"อ่าน YAML ไม่ได้: {e}")
            raise
    
    def _get_bq_client(self) -> bigquery.Client:
        """สร้าง BigQuery client โดยใช้ Service Account"""
        key_path = self.config["gcp"]["service_account_key"]
        project_id = self.config["gcp"]["project_id"]
        
        try:
            if not os.path.exists(key_path):
                self.logger.warning(
                    f"ไม่เจอ Service Account key ที่ {key_path} "
                    "ใช้ default credentials แทน"
                )
                return bigquery.Client(project=project_id)
            
            credentials = service_account.Credentials.from_service_account_file(
                key_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            client = bigquery.Client(project=project_id, credentials=credentials)
            self.logger.info("เชื่อมต่อ BigQuery สำเร็จ")
            return client
        except Exception as e:
            self.logger.error(f"เชื่อมต่อ BigQuery ไม่ได้: {e}")
            raise
    
    def ensure_dataset_exists(self):
        """สร้าง dataset ถ้ายังไม่มี"""
        dataset_ref = f"{self.project_id}.{self.dataset_name}"
        
        try:
            self.client.get_dataset(dataset_ref)
            self.logger.info(f"Dataset {self.dataset_name} มีอยู่แล้ว")
        except gcp_exceptions.NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self.location
            self.client.create_dataset(dataset)
            self.logger.info(f"สร้าง dataset {self.dataset_name} เรียบร้อย")
    
    def load_table(
        self,
        table_name: str,
        gcs_prefix: str,
        partition_field: Optional[str] = None,
        hive_partition: bool = False,
    ):
        """
        โหลดข้อมูลจาก GCS เข้า BigQuery
        
        ใช้ WRITE_TRUNCATE เพื่อให้รันซ้ำได้ (Idempotent)
        """
        table_ref = f"{self.project_id}.{self.dataset_name}.{table_name}"
        gcs_uri = f"gs://{self.bucket_name}/{gcs_prefix}/*.parquet"
        
        # ถ้ามี partition ต้องใช้ wildcard
        if hive_partition:
            gcs_uri = f"gs://{self.bucket_name}/{gcs_prefix}/*/*.parquet"
        
        self.logger.info(f"กำลังโหลด {gcs_uri} -> {table_ref}")
        
        # ตั้งค่า job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        
        # ตั้งค่า Hive partitioning (ถ้ามี)
        if hive_partition:
            job_config.hive_partitioning = bigquery.HivePartitioningOptions(
                mode="AUTO",
                source_uri_prefix=f"gs://{self.bucket_name}/{gcs_prefix}",
            )
        
        # ตั้งค่า time partitioning (ถ้ามี)
        if partition_field:
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
            )
        
        # รัน load job
        try:
            job = self.client.load_table_from_uri(
                gcs_uri,
                table_ref,
                job_config=job_config,
            )
            job.result()  # รอจนเสร็จ
            
            table = self.client.get_table(table_ref)
            self.logger.info(f"โหลดเสร็จ: {table.num_rows:,} rows")
            return table.num_rows
            
        except Exception as e:
            self.logger.error(f"โหลดไม่สำเร็จ: {e}")
            raise
    
    def load_all_tables(self):
        """โหลดทุกตารางที่กำหนดใน config"""
        print("\n" + "="*60)
        print("BIGQUERY LOADER")
        print("="*60)
        
        self.ensure_dataset_exists()
        
        tables = self.config.get("tables", [])
        successful = 0
        failed = 0
        
        for table_config in tables:
            table_name = table_config["table_name"]
            partition_by = table_config.get("partition_by")
            partition_field = table_config.get("partition_field")
            
            # เช็คว่ามี partition หรือไม่
            hive_partition = partition_by is not None and len(partition_by) > 0
            
            print(f"\nกำลังโหลด: {table_name}")
            
            try:
                self.load_table(
                    table_name=table_name,
                    gcs_prefix=f"silver/{table_name}",
                    partition_field=partition_field,
                    hive_partition=hive_partition,
                )
                successful += 1
            except Exception as e:
                self.logger.error(f"โหลด {table_name} ไม่สำเร็จ: {e}")
                failed += 1
        
        print("\n" + "="*60)
        print(f"สำเร็จ: {successful}, ล้มเหลว: {failed}")
        print("="*60)


def main():
    loader = BigQueryLoader()
    loader.load_all_tables()


if __name__ == "__main__":
    main()
