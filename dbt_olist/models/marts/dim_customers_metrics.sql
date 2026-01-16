{{
  config(
    materialized='table'
  )
}}

/*
  =============================================================================
  Dimension Table: ตัวชี้วัดลูกค้า (dim_customers_metrics)
  =============================================================================
  
  สิ่งที่ต้องระวัง (The Olist Trap):
     - customer_id = เปลี่ยนทุกครั้งที่สั่งซื้อ (ใช้ไม่ได้)
     - customer_unique_id = ตัวบุคคลจริง (ใช้อันนี้)
  
     ตอนแรกถ้า GROUP BY customer_id ค่า LTV จะผิด
     เพราะคนเดียวกันถูกนับเป็นหลายคน
  
  Metrics ที่คำนวณ:
     1. lifetime_value = ยอดซื้อสะสมตลอดชีพ (LTV)
     2. total_orders = จำนวน order ทั้งหมด
     3. first/last_order_date = วันแรก/ล่าสุดที่สั่งซื้อ
     4. customer_tenure_days = อายุลูกค้า (วันแรก -> วันล่าสุด)
  
  Use Cases:
     - Customer Segmentation: แบ่งกลุ่มลูกค้าตามมูลค่า
     - RFM Analysis: Recency, Frequency, Monetary
     - Churn Prediction: ลูกค้าคนไหนจะหายไป
  =============================================================================
*/

WITH sales AS (
    SELECT * FROM {{ ref('fct_sales_performance') }}
)

SELECT
    -- Primary Key: ใช้ customer_unique_id (ตัวบุคคลจริง)
    customer_unique_id,
    
    -- LTV: Lifetime Value = ยอดซื้อสะสมทั้งหมด
    SUM(net_revenue) AS lifetime_value,
    
    -- Order Metrics
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(*) AS total_items_purchased,
    
    -- Timeline
    MIN(purchased_at) AS first_order_date,
    MAX(purchased_at) AS last_order_date,
    
    -- Average Metrics
    ROUND(AVG(net_revenue), 2) AS avg_order_value,
    
    -- Customer Tenure: อายุลูกค้า (นับวัน)
    DATE_DIFF(MAX(purchased_at), MIN(purchased_at), DAY) AS customer_tenure_days,
    
    -- Location: ดึงที่อยู่จากการซื้อครั้งล่าสุด
    ARRAY_AGG(customer_city ORDER BY purchased_at DESC LIMIT 1)[OFFSET(0)] AS city,
    ARRAY_AGG(customer_state ORDER BY purchased_at DESC LIMIT 1)[OFFSET(0)] AS state

FROM sales
GROUP BY customer_unique_id
