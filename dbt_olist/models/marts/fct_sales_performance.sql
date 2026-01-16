{{
  config(
    materialized='incremental',
    unique_key='order_item_key',
    cluster_by=['customer_unique_id'],
    partition_by={
      "field": "purchased_at",
      "data_type": "timestamp",
      "granularity": "day"
    }
  )
}}

/*
  =============================================================================
  Fact Table: ยอดขายรายรายการสินค้า (fct_sales_performance)
  =============================================================================
  
  Business Logic:
     - กรอง order_status != 'canceled' ออก (ไม่นับเป็นยอดขายจริง)
     - คำนวณ net_revenue = ราคาสินค้า + ค่าส่ง
     - ใช้ customer_unique_id (ตัวบุคคลจริง) ไม่ใช่ customer_id
  
  ทำไมใช้ Incremental?
     - โหลดเฉพาะข้อมูลใหม่กว่าครั้งก่อน (ประหยัดค่า BigQuery scan)
     - ถ้าใช้ FULL refresh ทุกครั้ง ข้อมูล 1 ล้านแถว = เสียเงินทุกวัน
  
  ทำไมต้อง Partition by purchased_at?
     - เวลา Query เช่น "ยอดขายเดือนนี้" BigQuery จะสแกนแค่ partition นั้น
     - ไม่ต้องอ่านข้อมูลทั้งตาราง = ประหยัดเงิน + เร็วขึ้น
  
  ทำไมต้อง Cluster by customer_unique_id?
     - เวลา JOIN กับตารางอื่นโดยใช้ customer จะเร็วขึ้น
     - BigQuery จะเก็บข้อมูลลูกค้าเดียวกันไว้ใกล้ๆ กัน
  =============================================================================
*/

WITH orders AS (
    -- ดึงเฉพาะ order ที่ไม่ได้ถูกยกเลิก
    SELECT * FROM {{ ref('stg_orders') }}
    WHERE order_status != 'canceled'
),

order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
)

SELECT
    -- Primary Keys
    oi.order_item_key,
    o.order_id,
    oi.product_id,
    c.customer_unique_id,
    oi.seller_id,
    
    -- Order Status
    o.order_status,
    
    -- Timestamps
    o.purchased_at,
    o.approved_at,
    o.shipped_at,
    o.delivered_at,
    
    -- Revenue Metrics
    oi.price,
    oi.freight_value,
    oi.item_total AS net_revenue,
    
    -- Location
    c.city AS customer_city,
    c.state AS customer_state

FROM orders o
INNER JOIN order_items oi ON o.order_id = oi.order_id
INNER JOIN customers c ON o.customer_id = c.customer_id

{% if is_incremental() %}
    -- Incremental: โหลดเฉพาะข้อมูลใหม่กว่าครั้งก่อน
    WHERE o.purchased_at > (SELECT MAX(purchased_at) FROM {{ this }})
{% endif %}
