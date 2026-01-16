{{
  config(
    materialized='view'
  )
}}

/*
  Staging: Customers
  
  ระวัง: ตาราง customers มี 2 ID
  - customer_id = เปลี่ยนทุก order (ใช้ join กับ orders)
  - customer_unique_id = ตัวคนจริง (ใช้คำนวณ LTV)
*/

SELECT
    customer_id,
    customer_unique_id,
    
    -- ที่อยู่
    customer_zip_code_prefix AS zip_code,
    customer_city AS city,
    customer_state AS state

FROM {{ source('olist_warehouse', 'customers') }}
WHERE customer_id IS NOT NULL
