{{
  config(
    materialized='view'
  )
}}

/*
  Staging: Products
  
  ถ้า category เป็น null ให้ใส่ 'unknown' แทน
  ชื่อคอลัมน์บางตัวมี typo จากต้นทาง (lenght แทน length)
*/

SELECT
    product_id,
    
    COALESCE(product_category_name, 'unknown') AS category,
    
    -- ข้อมูลสินค้า
    product_name_lenght AS name_length,
    product_description_lenght AS description_length,
    product_photos_qty AS photos_count,
    
    -- ขนาดสินค้า
    product_weight_g AS weight_grams,
    product_length_cm AS length_cm,
    product_height_cm AS height_cm,
    product_width_cm AS width_cm

FROM {{ source('olist_warehouse', 'products') }}
WHERE product_id IS NOT NULL
