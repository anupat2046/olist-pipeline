{{
  config(
    materialized='view'
  )
}}

/*
  Staging: Orders
  
  เปลี่ยนชื่อคอลัมน์ให้อ่านง่ายขึ้น เช่น
  order_purchase_timestamp -> purchased_at
*/

SELECT
    order_id,
    customer_id,
    order_status,
    
    -- เปลี่ยนชื่อให้สั้นลง
    order_purchase_timestamp AS purchased_at,
    order_approved_at AS approved_at,
    order_delivered_carrier_date AS shipped_at,
    order_delivered_customer_date AS delivered_at,
    order_estimated_delivery_date AS estimated_delivery_at,
    
    -- คอลัมน์สำหรับ partition
    order_purchase_year,
    order_purchase_month

FROM {{ source('olist_warehouse', 'orders') }}
WHERE order_id IS NOT NULL
