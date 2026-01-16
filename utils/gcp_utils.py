"""
=============================================================================
Olist Data Engineering Pipeline - GCP Utilities
=============================================================================

ไฟล์นี้รวม 2 Class หลัก:
   1. GCPUtils - จัดการ upload/download ไฟล์กับ Google Cloud Storage
   2. JobLogger - บันทึก metadata การรัน Pipeline ลง BigQuery

สิ่งที่เรียนรู้:
   - การใช้ Service Account Key เพื่อยืนยันตัวตนกับ GCP
   - การทำ Observability ด้วยการบันทึก log ลง database
   - ทำไมต้องมี metadata logging (เพื่อดูย้อนหลังว่าอะไรพังตรงไหน)
=============================================================================
"""

import os
from pathlib import Path
from typing import Optional, List

import yaml
from google.cloud import storage
from google.oauth2 import service_account

from utils.logger import get_logger


class GCPUtils:
    """
    ตัวช่วยจัดการ Google Cloud Storage
    
    ทำไมต้องใช้ Service Account?
       - เพื่อให้โปรแกรมเข้าถึง GCP ได้โดยไม่ต้อง login
       - ในงานจริงจะไม่มีใครมานั่งพิมพ์ password ให้ทุกครั้ง
    """
    
    def __init__(self, config_path: str = "config/settings.yaml"):
        """
        Initialize GCP utilities with configuration.
        
        Args:
            config_path: Path to YAML configuration file
        """
        self.logger = get_logger("gcp_utils")
        self.config = self._load_config(config_path)
        self.client = self._get_storage_client()
        self.bucket_name = self.config["gcp"]["bucket_name"]
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file."""
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            self.logger.info(f"Configuration loaded from {config_path}")
            return config
        except FileNotFoundError:
            self.logger.error(f"Configuration file not found: {config_path}")
            raise
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing YAML configuration: {e}")
            raise
    
    def _get_storage_client(self) -> storage.Client:
        """
        Create Google Cloud Storage client using Service Account.
        
        Returns:
            Authenticated storage.Client instance
        """
        key_path = self.config["gcp"]["service_account_key"]
        project_id = self.config["gcp"]["project_id"]
        
        try:
            # Check if key file exists
            if not os.path.exists(key_path):
                self.logger.warning(
                    f"Service Account key not found at {key_path}. "
                    "Falling back to default credentials."
                )
                return storage.Client(project=project_id)
            
            # Load credentials from Service Account key
            credentials = service_account.Credentials.from_service_account_file(
                key_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            
            client = storage.Client(
                project=project_id,
                credentials=credentials
            )
            self.logger.info("GCS client authenticated with Service Account")
            return client
            
        except Exception as e:
            self.logger.error(f"Failed to create GCS client: {e}")
            raise
    
    def get_bucket(self) -> storage.Bucket:
        """
        Get or create the configured GCS bucket.
        
        Returns:
            storage.Bucket instance
        """
        try:
            bucket = self.client.get_bucket(self.bucket_name)
            self.logger.info(f"Using existing bucket: {self.bucket_name}")
            return bucket
        except Exception:
            self.logger.warning(f"Bucket {self.bucket_name} not found")
            raise
    
    def upload_file(
        self,
        local_path: str,
        gcs_path: str,
        bucket_name: Optional[str] = None
    ) -> str:
        """
        Upload a single file to GCS.
        
        Args:
            local_path: Path to local file
            gcs_path: Destination path in GCS (blob name)
            bucket_name: Optional bucket name (uses config default)
            
        Returns:
            GCS URI (gs://bucket/path)
        """
        bucket_name = bucket_name or self.bucket_name
        
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(gcs_path)
            
            blob.upload_from_filename(local_path)
            
            gcs_uri = f"gs://{bucket_name}/{gcs_path}"
            self.logger.info(f"Uploaded: {local_path} -> {gcs_uri}")
            return gcs_uri
            
        except Exception as e:
            self.logger.error(f"Failed to upload {local_path}: {e}")
            raise
    
    def upload_folder(
        self,
        local_folder: str,
        gcs_prefix: str,
        bucket_name: Optional[str] = None,
        file_extension: Optional[str] = None
    ) -> List[str]:
        """
        Upload all files from a local folder to GCS.
        
        Args:
            local_folder: Path to local folder
            gcs_prefix: Prefix path in GCS (folder structure)
            bucket_name: Optional bucket name
            file_extension: Optional filter by extension (e.g., ".parquet")
            
        Returns:
            List of uploaded GCS URIs
        """
        bucket_name = bucket_name or self.bucket_name
        folder_path = Path(local_folder)
        
        if not folder_path.exists():
            self.logger.error(f"Local folder not found: {local_folder}")
            raise FileNotFoundError(f"Folder not found: {local_folder}")
        
        uploaded_files = []
        
        # Walk through folder recursively
        for file_path in folder_path.rglob("*"):
            if file_path.is_file():
                # Filter by extension if specified
                if file_extension and file_path.suffix != file_extension:
                    continue
                
                # Skip hidden files and Spark metadata
                if file_path.name.startswith(".") or file_path.name == "_SUCCESS":
                    continue
                
                # Calculate relative path for GCS
                relative_path = file_path.relative_to(folder_path)
                gcs_path = f"{gcs_prefix}/{relative_path}".replace("\\", "/")
                
                try:
                    uri = self.upload_file(str(file_path), gcs_path, bucket_name)
                    uploaded_files.append(uri)
                except Exception as e:
                    self.logger.warning(f"Skipping {file_path}: {e}")
        
        self.logger.info(
            f"Uploaded {len(uploaded_files)} files from {local_folder} to "
            f"gs://{bucket_name}/{gcs_prefix}"
        )
        return uploaded_files
    
    def download_file(
        self,
        gcs_path: str,
        local_path: str,
        bucket_name: Optional[str] = None
    ) -> str:
        """
        Download a file from GCS to local filesystem.
        
        Args:
            gcs_path: Source path in GCS (blob name)
            local_path: Destination local path
            bucket_name: Optional bucket name
            
        Returns:
            Local file path
        """
        bucket_name = bucket_name or self.bucket_name
        
        try:
            # Create local directory if needed
            local_dir = Path(local_path).parent
            local_dir.mkdir(parents=True, exist_ok=True)
            
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(gcs_path)
            
            blob.download_to_filename(local_path)
            
            self.logger.info(f"Downloaded: gs://{bucket_name}/{gcs_path} -> {local_path}")
            return local_path
            
        except Exception as e:
            self.logger.error(f"Failed to download {gcs_path}: {e}")
            raise
    
    def blob_exists(
        self,
        gcs_path: str,
        bucket_name: Optional[str] = None
    ) -> bool:
        """
        Check if a blob exists in GCS.
        
        Args:
            gcs_path: Path in GCS (blob name)
            bucket_name: Optional bucket name
            
        Returns:
            True if blob exists, False otherwise
        """
        bucket_name = bucket_name or self.bucket_name
        
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(gcs_path)
            exists = blob.exists()
            
            self.logger.debug(f"Blob exists check: {gcs_path} = {exists}")
            return exists
            
        except Exception as e:
            self.logger.error(f"Error checking blob existence: {e}")
            return False
    
    def list_blobs(
        self,
        prefix: str = "",
        bucket_name: Optional[str] = None
    ) -> List[str]:
        """
        List all blobs in bucket with optional prefix filter.
        
        Args:
            prefix: Optional prefix to filter blobs
            bucket_name: Optional bucket name
            
        Returns:
            List of blob names
        """
        bucket_name = bucket_name or self.bucket_name
        
        try:
            bucket = self.client.bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=prefix)
            
            blob_names = [blob.name for blob in blobs]
            self.logger.info(f"Found {len(blob_names)} blobs with prefix '{prefix}'")
            return blob_names
            
        except Exception as e:
            self.logger.error(f"Error listing blobs: {e}")
            raise


class JobLogger:
    """
    ตัวบันทึก metadata การรัน Pipeline ลง BigQuery (Observability)
    
    ทำไมต้องมี JobLogger?
       - เวลา Pipeline พัง เราจะได้ดูย้อนหลังได้ว่าพังตรงไหน
       - บันทึกเวลาเริ่ม/จบ และจำนวน rows ที่ประมวลผล
       - ใช้ดูว่า Pipeline ทำงานช้าลงหรือเปล่าเมื่อเวลาผ่านไป
    
    Table Schema (meta.job_runs):
       - job_id: รหัสการรัน (เช่น 20260116_0200)
       - step_name: ชื่อ step (เช่น silver_transformation)
       - status: SUCCESS หรือ FAILED
       - rows_processed: จำนวนแถวที่ประมวลผล
       - duration_seconds: ใช้เวลากี่วินาที
    
    หมายเหตุ: ตารางนี้ append-only (รันซ้ำได้ ข้อมูลไม่หาย)
    """
    
    def __init__(self, config_path: str = "config/settings.yaml"):
        """เริ่มต้น JobLogger พร้อม BigQuery client"""
        self.logger = get_logger("job_logger")
        self.config = self._load_config(config_path)
        self.client = self._get_bq_client()
        self.project_id = self.config["gcp"]["project_id"]
        self.dataset_id = "meta"  # Dataset สำหรับเก็บ metadata
        self.table_id = "job_runs"
        
        # สร้างตารางถ้ายังไม่มี
        self._ensure_table_exists()
    
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file."""
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    
    def _get_bq_client(self):
        """Create BigQuery client using Service Account."""
        from google.cloud import bigquery
        
        key_path = self.config["gcp"]["service_account_key"]
        project_id = self.config["gcp"]["project_id"]
        
        if os.path.exists(key_path):
            credentials = service_account.Credentials.from_service_account_file(
                key_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            return bigquery.Client(project=project_id, credentials=credentials)
        else:
            return bigquery.Client(project=project_id)
    
    def _ensure_table_exists(self):
        """Create meta.job_runs table if it doesn't exist."""
        from google.cloud import bigquery
        from google.api_core import exceptions as gcp_exceptions
        
        dataset_ref = f"{self.project_id}.{self.dataset_id}"
        table_ref = f"{dataset_ref}.{self.table_id}"
        
        # Create dataset if needed
        try:
            self.client.get_dataset(dataset_ref)
        except gcp_exceptions.NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self.config["gcp"]["location"]
            self.client.create_dataset(dataset)
            self.logger.info(f"Created dataset: {dataset_ref}")
        
        # Create table if needed
        try:
            self.client.get_table(table_ref)
        except gcp_exceptions.NotFound:
            schema = [
                bigquery.SchemaField("job_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("step_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("start_time", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("end_time", "TIMESTAMP"),
                bigquery.SchemaField("duration_seconds", "FLOAT64"),
                bigquery.SchemaField("rows_processed", "INT64"),
                bigquery.SchemaField("error_message", "STRING"),
                bigquery.SchemaField("metadata", "JSON"),
            ]
            table = bigquery.Table(table_ref, schema=schema)
            self.client.create_table(table)
            self.logger.info(f"Created table: {table_ref}")
    
    def log_start(self, job_id: str, step_name: str) -> dict:
        """
        Log the start of a pipeline step.
        
        Args:
            job_id: Unique identifier for the pipeline run
            step_name: Name of the step (e.g., 'spark', 'upload', 'ingest')
            
        Returns:
            Job record dict (for use in log_end)
        """
        from datetime import datetime, timezone
        
        start_time = datetime.now(timezone.utc)
        
        record = {
            "job_id": job_id,
            "step_name": step_name,
            "status": "running",
            "start_time": start_time.isoformat(),
        }
        
        self.logger.info(f"[{job_id}] Starting step: {step_name}")
        return record
    
    def log_end(
        self,
        record: dict,
        status: str = "success",
        rows_processed: int = None,
        error_message: str = None,
        metadata: dict = None
    ):
        """
        Log the end of a pipeline step.
        
        Args:
            record: Job record from log_start
            status: 'success' or 'failed'
            rows_processed: Optional row count
            error_message: Error message if failed
            metadata: Additional metadata dict
        """
        from datetime import datetime, timezone
        from google.cloud import bigquery
        
        end_time = datetime.now(timezone.utc)
        start_time = datetime.fromisoformat(record["start_time"])
        duration = (end_time - start_time).total_seconds()
        
        row = {
            "job_id": record["job_id"],
            "step_name": record["step_name"],
            "status": status,
            "start_time": record["start_time"],
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "rows_processed": rows_processed,
            "error_message": error_message,
            "metadata": metadata,
        }
        
        table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        
        try:
            errors = self.client.insert_rows_json(table_ref, [row])
            if errors:
                self.logger.error(f"Failed to log job: {errors}")
            else:
                self.logger.info(
                    f"[{record['job_id']}] Step {record['step_name']} "
                    f"completed: {status} ({duration:.2f}s)"
                )
        except Exception as e:
            self.logger.error(f"Error logging to BigQuery: {e}")
    
    def log_step(self, job_id: str, step_name: str):
        """
        Context manager for logging a pipeline step.
        
        Usage:
            with job_logger.log_step('run-123', 'spark') as step:
                # do work
                step['rows'] = 1000
        """
        class StepContext:
            def __init__(ctx, logger, job_id, step_name):
                ctx.logger = logger
                ctx.job_id = job_id
                ctx.step_name = step_name
                ctx.record = None
                ctx.rows = None
                ctx.metadata = None
            
            def __enter__(ctx):
                ctx.record = ctx.logger.log_start(ctx.job_id, ctx.step_name)
                return ctx
            
            def __exit__(ctx, exc_type, exc_val, exc_tb):
                if exc_type is None:
                    ctx.logger.log_end(
                        ctx.record,
                        status="success",
                        rows_processed=ctx.rows,
                        metadata=ctx.metadata
                    )
                else:
                    ctx.logger.log_end(
                        ctx.record,
                        status="failed",
                        error_message=str(exc_val)
                    )
                return False
        
        return StepContext(self, job_id, step_name)


# Convenience function for quick uploads
def upload_silver_to_gcs(config_path: str = "config/settings.yaml") -> List[str]:
    """
    Upload silver layer data to GCS.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        List of uploaded GCS URIs
    """
    gcp = GCPUtils(config_path)
    silver_path = gcp.config["paths"]["silver"]
    
    return gcp.upload_folder(
        local_folder=silver_path,
        gcs_prefix="silver",
        file_extension=".parquet"
    )


if __name__ == "__main__":
    # Test the GCP utilities
    print("Testing GCP Utils...")
    
    try:
        gcp = GCPUtils()
        print(f"✓ Configuration loaded")
        print(f"  Project: {gcp.config['gcp']['project_id']}")
        print(f"  Bucket: {gcp.bucket_name}")
        
        # Test bucket access (will fail if not configured)
        # bucket = gcp.get_bucket()
        # print(f"✓ Bucket accessible")
        
    except Exception as e:
        print(f"✗ Error: {e}")
