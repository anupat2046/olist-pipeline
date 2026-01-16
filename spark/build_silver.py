"""
=============================================================================
Olist Data Engineering Pipeline - Spark Silver Layer Builder
=============================================================================

สิ่งที่เรียนรู้จากไฟล์นี้:
- ทำไมต้องใช้ Explicit Schema แทน inferSchema (เพื่อป้องกัน Pipeline พัง)
- การทำ Partitioning เพื่อประหยัดค่า Query ใน BigQuery
- การเลือกใช้ Parquet แทน CSV เพราะอ่านเร็วกว่าและบีบอัดได้ดี
=============================================================================
"""

import os
import sys
from pathlib import Path
from datetime import datetime

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, year, month, 
    trim, when, regexp_replace
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType
)


# =============================================================================
# SCHEMA DEFINITIONS - กำหนดโครงสร้างข้อมูลเอง
# =============================================================================
# ทำไมต้องกำหนด Schema เอง?
#    - ถ้าใช้ inferSchema=True แล้ว Spark อาจเดา Type ผิด (เช่น ZIP Code เป็น Int แทน String)
#    - ถ้าไฟล์ CSV โครงสร้างเปลี่ยน Pipeline จะพังทันที (Fail Fast ดีกว่า Fail Silently)
#    - ในงานจริงเรียกว่าการทำ "Schema Contract" กับข้อมูลต้นทาง

# ตาราง Orders - ข้อมูลคำสั่งซื้อ
ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),  # ระวัง: ID นี้เปลี่ยนทุก order
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True),
    StructField("order_approved_at", StringType(), True),
    StructField("order_delivered_carrier_date", StringType(), True),
    StructField("order_delivered_customer_date", StringType(), True),
    StructField("order_estimated_delivery_date", StringType(), True),
])

# ตาราง Order Items - รายการสินค้าในแต่ละ order (1 order มีได้หลายสินค้า)
ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_item_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("shipping_limit_date", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True),
])

# ตาราง Customers - ข้อมูลลูกค้า
CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),        # ID ต่อ order (เปลี่ยนทุกครั้ง)
    StructField("customer_unique_id", StringType(), True), # ID ตัวบุคคลจริง - ใช้อันนี้คำนวณ LTV
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
])

# ตาราง Products - ข้อมูลสินค้า
PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_category_name", StringType(), True),
    StructField("product_name_lenght", IntegerType(), True),
    StructField("product_description_lenght", IntegerType(), True),
    StructField("product_photos_qty", IntegerType(), True),
    StructField("product_weight_g", IntegerType(), True),
    StructField("product_length_cm", IntegerType(), True),
    StructField("product_height_cm", IntegerType(), True),
    StructField("product_width_cm", IntegerType(), True),
])

# Mapping ชื่อไฟล์ CSV กับ Schema
SCHEMAS = {
    "olist_orders_dataset": ORDERS_SCHEMA,
    "olist_order_items_dataset": ORDER_ITEMS_SCHEMA,
    "olist_customers_dataset": CUSTOMERS_SCHEMA,
    "olist_products_dataset": PRODUCTS_SCHEMA,
}


class SparkSilverBuilder:
    """
    ตัวจัดการแปลง CSV เป็น Parquet (Silver Layer)
    
    ทำไมต้องมี Silver Layer?
       - Raw CSV มักมีปัญหา: encoding, null values, format วันที่ไม่ตรง
       - Silver Layer = ข้อมูลที่ทำความสะอาดแล้ว พร้อมใช้งาน
    """
    
    def __init__(self, config_path: str = "config/settings.yaml"):
        self.config = self._load_config(config_path)
        self.spark = self._create_spark_session()
        self.raw_path = Path(self.config["paths"]["raw"])
        self.silver_path = Path(self.config["paths"]["silver"])
        
    def _load_config(self, config_path: str) -> dict:
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    
    def _create_spark_session(self) -> SparkSession:
        """
        สร้าง Spark Session สำหรับรันในเครื่อง
        
        Config ที่เลือกใช้:
           - local[*] = ใช้ทุก CPU core ที่มี
           - snappy = compression ที่เร็วและขนาดพอดี
           - driver.memory = 2g เพราะ dataset ไม่ใหญ่มาก
        """
        spark = (
            SparkSession.builder
            .appName("OlistSilverBuilder")
            .master("local[*]")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.memory", "2g")
            .getOrCreate()
        )
        
        spark.sparkContext.setLogLevel("WARN")
        print(f"Spark session created: {spark.version}")
        return spark
    
    def read_csv(self, csv_name: str) -> "DataFrame":
        """
        อ่านไฟล์ CSV โดยใช้ Schema ที่กำหนดไว้
        
        ทำไมไม่ใช้ inferSchema=True?
           - Spark จะต้องอ่านไฟล์ 2 รอบ (รอบแรกเดา schema, รอบสองอ่านจริง)
           - ถ้าข้อมูล 1 ล้านแถว = เสียเวลา
        """
        csv_path = self.raw_path / f"{csv_name}.csv"
        
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        schema = SCHEMAS.get(csv_name)
        
        df = (
            self.spark.read
            .option("header", "true")
            .option("encoding", "utf-8")
            .schema(schema)
            .csv(str(csv_path))
        )
        
        row_count = df.count()
        print(f"  Read {row_count:,} rows from {csv_name}")
        return df
    
    def clean_orders(self, df: "DataFrame") -> "DataFrame":
        """
        ทำความสะอาดข้อมูล Orders
        
        สิ่งที่ทำ:
           1. แปลง string เป็น timestamp
           2. เพิ่มคอลัมน์ year/month สำหรับทำ Partitioning
        """
        timestamp_cols = [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ]
        
        for col_name in timestamp_cols:
            df = df.withColumn(
                col_name,
                to_timestamp(col(col_name), "yyyy-MM-dd HH:mm:ss")
            )
        
        # เพิ่มคอลัมน์ year และ month สำหรับ Partitioning
        # เวลา Query ใน BigQuery จะได้ไม่ต้องสแกนข้อมูลทั้งตาราง
        df = (
            df
            .withColumn("order_purchase_year", year(col("order_purchase_timestamp")))
            .withColumn("order_purchase_month", month(col("order_purchase_timestamp")))
        )
        
        df = df.withColumn("order_status", trim(col("order_status")))
        
        return df
    
    def clean_order_items(self, df: "DataFrame") -> "DataFrame":
        """
        ทำความสะอาดข้อมูล Order Items
        
        หมายเหตุ: 1 order_id อาจมีหลาย order_item_id
        """
        df = df.withColumn(
            "shipping_limit_date",
            to_timestamp(col("shipping_limit_date"), "yyyy-MM-dd HH:mm:ss")
        )
        
        df = (
            df
            .withColumn("order_purchase_year", year(col("shipping_limit_date")))
            .withColumn("order_purchase_month", month(col("shipping_limit_date")))
        )
        
        return df
    
    def clean_customers(self, df: "DataFrame") -> "DataFrame":
        """
        ทำความสะอาดข้อมูล Customers
        
        สำคัญ:
           - customer_id = ID ที่เปลี่ยนทุก order (ไม่ใช่ตัวบุคคล)
           - customer_unique_id = ID ตัวบุคคลจริง (ใช้อันนี้คำนวณ LTV)
        """
        df = (
            df
            .withColumn("customer_city", trim(col("customer_city")))
            .withColumn("customer_state", trim(col("customer_state")))
        )
        return df
    
    def clean_products(self, df: "DataFrame") -> "DataFrame":
        """
        ทำความสะอาดข้อมูล Products
        
        ถ้า category เป็น null ให้ใส่ 'unknown' แทน
        """
        df = (
            df
            .withColumn(
                "product_category_name",
                when(col("product_category_name").isNull(), "unknown")
                .otherwise(trim(col("product_category_name")))
            )
        )
        return df
    
    def write_parquet(
        self, 
        df: "DataFrame", 
        table_name: str, 
        partition_by: list = None
    ):
        """
        เขียนข้อมูลเป็น Parquet
        
        ทำไมใช้ Parquet แทน CSV?
           - อ่านเร็วกว่า 10-100 เท่า (columnar format)
           - บีบอัดได้ดี (file เล็กลงมาก)
           - เก็บ schema ไว้ในไฟล์เลย
        """
        output_path = self.silver_path / table_name
        
        self.silver_path.mkdir(parents=True, exist_ok=True)
        
        writer = (
            df.write
            .mode("overwrite")  # เขียนทับทุกครั้ง (Idempotent)
            .option("compression", "snappy")
        )
        
        # ทำ Partitioning ช่วยให้ BigQuery สแกนเฉพาะ partition ที่ต้องการ
        if partition_by:
            writer = writer.partitionBy(*partition_by)
            print(f"  Partitioning by: {partition_by}")
        
        writer.parquet(str(output_path))
        print(f"  Written to: {output_path}")
    
    def process_table(self, table_config: dict):
        csv_name = table_config["csv_name"]
        table_name = table_config["table_name"]
        partition_by = table_config.get("partition_by")
        
        print(f"\n{'='*60}")
        print(f"Processing: {csv_name} -> {table_name}")
        print(f"{'='*60}")
        
        df = self.read_csv(csv_name)
        
        if "orders_dataset" in csv_name and "items" not in csv_name:
            df = self.clean_orders(df)
        elif "order_items" in csv_name:
            df = self.clean_order_items(df)
        elif "customers" in csv_name:
            df = self.clean_customers(df)
        elif "products" in csv_name:
            df = self.clean_products(df)
        
        self.write_parquet(df, table_name, partition_by)
    
    def build_all(self):
        print("\n" + "="*60)
        print("OLIST SILVER LAYER BUILDER")
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        
        tables = self.config.get("tables", [])
        
        if not tables:
            print("No tables configured in settings.yaml")
            return
        
        successful = 0
        failed = 0
        
        for table_config in tables:
            try:
                self.process_table(table_config)
                successful += 1
            except FileNotFoundError as e:
                print(f"  Skipped (file not found): {e}")
                failed += 1
            except Exception as e:
                print(f"  Error: {e}")
                failed += 1
        
        print("\n" + "="*60)
        print(f"COMPLETED: {successful} successful, {failed} failed")
        print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
    
    def stop(self):
        if self.spark:
            self.spark.stop()
            print("\nSpark session stopped")


def main():
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config/settings.yaml"
    
    builder = None
    try:
        builder = SparkSilverBuilder(config_path)
        builder.build_all()
    except Exception as e:
        print(f"\nPipeline failed: {e}")
        sys.exit(1)
    finally:
        if builder:
            builder.stop()


if __name__ == "__main__":
    main()
