WITH RECURSIVE SplitSellerId AS (
  SELECT 
    created_datetime,
    modified_datetime,
    marketplace,
    root_category_id,
    asin,
    brand,
    name,
    image_url, 
    breadcrumb_path_category_ids, 
    SUBSTRING_INDEX(seller_id_mode, '|', 1) AS seller_id,
    SUBSTRING(seller_id_mode, LENGTH(SUBSTRING_INDEX(seller_id_mode, '|', 1)) + 2) AS remaining_sellers,
    seller_types,
    (LENGTH(seller_id_mode) - LENGTH(REPLACE(seller_id_mode, '|', '')) + 1) AS seller_num
  FROM (
    SELECT
      created_datetime,
      modified_datetime,
      marketplace,
      root_category_id,
    YEAR,
    week,
      asin,
      brand,
      name,
      image_url, 
      breadcrumb_path_category_ids, 
      seller_id_mode,
      seller_types,
      ROW_NUMBER() OVER (PARTITION BY marketplace, root_category_id, asin ORDER BY breadcrumb_path_category_ids DESC, seller_id_mode DESC, image_url DESC) AS rn
    FROM
      db_junglescout_amazon.tb_sales_estimates_weekly_v2_old 
      PARTITION (p0)
    
      
  ) AS RandomSelection
  WHERE rn = 1
  
  UNION ALL

  SELECT 
    created_datetime,
    modified_datetime,
    marketplace,
    root_category_id,
    asin,
    brand,
    name,
    image_url,
    breadcrumb_path_category_ids,
    SUBSTRING_INDEX(remaining_sellers, '|', 1) AS seller_id,
    SUBSTRING(remaining_sellers, LENGTH(SUBSTRING_INDEX(remaining_sellers, '|', 1)) + 2) AS remaining_sellers,
    seller_types,
    seller_num
  FROM 
    SplitSellerId
  WHERE 
    remaining_sellers != ''
)

SELECT 
  marketplace,
  root_category_id,
  asin,
  seller_id,
  brand,
  name,
  image_url, 
  REPLACE(breadcrumb_path_category_ids, '|', ' > ') AS category_path,
  SUBSTRING_INDEX(breadcrumb_path_category_ids, '|', 1) AS category,
  SUBSTRING_INDEX(breadcrumb_path_category_ids, '|', 1) AS category_id_0,
  CASE WHEN LENGTH(breadcrumb_path_category_ids) - LENGTH(REPLACE(breadcrumb_path_category_ids, '|', '')) >= 1
       THEN SUBSTRING_INDEX(SUBSTRING_INDEX(breadcrumb_path_category_ids, '|', 2), '|', -1) END AS category_id_1,
  CASE WHEN LENGTH(breadcrumb_path_category_ids) - LENGTH(REPLACE(breadcrumb_path_category_ids, '|', '')) >= 2
       THEN SUBSTRING_INDEX(SUBSTRING_INDEX(breadcrumb_path_category_ids, '|', 3), '|', -1) END AS category_id_2,
  CASE WHEN LENGTH(breadcrumb_path_category_ids) - LENGTH(REPLACE(breadcrumb_path_category_ids, '|', '')) >= 3
       THEN SUBSTRING_INDEX(SUBSTRING_INDEX(breadcrumb_path_category_ids, '|', 4), '|', -1) END AS category_id_3,
  CASE WHEN LENGTH(breadcrumb_path_category_ids) - LENGTH(REPLACE(breadcrumb_path_category_ids, '|', '')) >= 4
       THEN SUBSTRING_INDEX(SUBSTRING_INDEX(breadcrumb_path_category_ids, '|', 5), '|', -1) END AS category_id_4,
  CASE WHEN LENGTH(breadcrumb_path_category_ids) - LENGTH(REPLACE(breadcrumb_path_category_ids, '|', '')) >= 5
       THEN SUBSTRING_INDEX(SUBSTRING_INDEX(breadcrumb_path_category_ids, '|', 6), '|', -1) END AS category_id_5,
  CASE WHEN LENGTH(breadcrumb_path_category_ids) - LENGTH(REPLACE(breadcrumb_path_category_ids, '|', '')) >= 6
       THEN SUBSTRING_INDEX(SUBSTRING_INDEX(breadcrumb_path_category_ids, '|', 7), '|', -1) END AS category_id_6,
  seller_num,
  seller_types,
  created_datetime,
  modified_datetime
FROM 
  SplitSellerId;