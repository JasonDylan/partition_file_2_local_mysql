CREATE TABLE db_junglescout_amazon.tb_data_product (
  marketplace varchar(5) NOT NULL,
  root_category_id bigint NOT NULL,
  --year int NOT NULL,
  --week varchar(3) NOT NULL,
  --start_date date NOT NULL,
  --end_date date NOT NULL,
  asin varchar(40) NOT NULL,
  --is_available int,
  brand varchar(1000),
  name varchar(1000)  COMMENT 'title',
  image_url varchar(1000),
  --price decimal(10,2),
  --review_count bigint  commet 'review',
  --ratings decimal(3,1),
 --breadcrumb_path_category_ids varchar(1000),
  category_name varchar(1000),
  category_path varchar(1000),
  cat_id0 bigint,
  cat_id1 bigint,
  cat_id2 bigint,
  cat_id3 bigint,
  cat_id4 bigint,
  cat_id5 bigint,
  cat_id6 bigint,
 --ranking_category_ids varchar(1000),
  first_date_available datetime  COMMENT 'publish_date_2',
  revenue decimal(20,2),
  revenue_1p decimal(20,2),
  revenue_3p decimal(20,2),
  sales decimal(20,2),
  sales_1p decimal(20,2),
  sales_3p decimal(20,2),
  --`seller_ids` varchar(1000),
  --seller_id_mode varchar(1000),
  seller_num int DEFAULT 0,
  seller_id varchar(100) DEFAULT "",
  seller_types varchar(100) COMMENT 'bbSeller'
  data_type tinyint unsigned NOT NULL DEFAULT 0 COMMENT '数据清理状态',
  created_datetime datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  modified_datetime datetime NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

