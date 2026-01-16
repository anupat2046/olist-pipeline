{{
  config(
    materialized='view'
  )
}}

/*
  Staging: Order Items
  
  1 order มีได้หลายสินค้า ใช้ order_item_key เป็น primary key
  item_total = ราคาสินค้า + ค่าส่ง
*/

SELECT
    -- สร้าง key ที่ไม่ซ้ำกัน
    {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id', 'order_item_id']) }} AS order_item_key,
    
    order_id,
    order_item_id,
    product_id,
    seller_id,
    
    -- ราคา
    price,
    freight_value,
    price + freight_value AS item_total,
    
    shipping_limit_date AS shipping_deadline_at,
    
    order_purchase_year,
    order_purchase_month

FROM {{ source('olist_warehouse', 'order_items') }}
WHERE order_id IS NOT NULL
