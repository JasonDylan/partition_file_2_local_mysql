"""
模块名称: 数据库加载工具
描述: 该模块提供功能以校验 CSV 文件的头部并将其加载到 MySQL 数据库中。
"""

import os
import time

import mysql.connector
import pandas as pd
from tqdm import tqdm  # 导入 tqdm

from config.config import DB_CONFIG  # 引入配置
from model import base_model, mapping
from model.server_108.db_junglescout_amazon import (
    TbDataProduct,
    TbDataWeek,
    TbLoadedRecords,
    TbSalesEstimatesWeeklyV2,
)
from util import sqlalchemy_orm_util
from util.file_util import get_a_table_all_file_by_format
from util.get_partition_info import (
    extract_ordered_partition_k_v_pairs_from_path,
    extract_partition_items,
)
from util.sqlalchemy_orm_util import create_table_if_not_exists


def validate_one_tb_partition_dir_csv_headers(
    table_path: str, class_obj: base_model.BaseModel
) -> tuple[bool, list[str]]:
    """
    校验单个表对应的 CSV 文件的头部.

    :param table_path: 表的绝对路径.
    :param class_obj: 具体的 ORM 类.
    :return: (是否所有 CSV 文件的头部格式正确, 所有文件列表).
    """
    # 获取 baseModel 的字段
    base_model_header = sqlalchemy_orm_util.get_abstract_class_fields(
        base_model.BaseModel
    )
    # 获取类的字段名称
    class_headers = sqlalchemy_orm_util.get_class_fields(class_obj)
    print(f"{class_obj.__tablename__=}")
    this_table_all_csv_header_is_formatted = True

    # 获取所有文件
    all_files_for_a_table = get_a_table_all_file_by_format(table_path)
    
    # 从db_junglescout_amazon.tb_loaded_records 根据表名筛选获取已经加载的文件
    # 使用mysql.connector连接数据库并查询已加载的记录
    with mysql.connector.connect(**DB_CONFIG) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT file_name FROM db_junglescout_amazon.tb_loaded_records WHERE table_name = '{class_obj.__tablename__}'")
            loaded_records = cursor.fetchall()
    
    loaded_file_names = [record[0] for record in loaded_records]
    # 从all_files_for_a_table中去除已经加载的文件
    files_to_process = [file for file in all_files_for_a_table if os.path.basename(file) not in loaded_file_names]
    
    # 打印去除前和去除后的文件数量
    print(f"总文件数量: {len(all_files_for_a_table)}")
    print(f"已加载文件数量: {len(loaded_file_names)}")
    print(f"待处理文件数量: {len(files_to_process)}")
    # 使用 tqdm 显示进度条
    for file_path in tqdm(
        files_to_process, desc=f"Processing files in {class_obj.__tablename__}"
    ):
        try:
            # 只读取标题行
            df = pd.read_csv(file_path, nrows=0)
            a_csv_headers = df.columns.tolist()

            # 提取分区字段
            partition_fields = extract_partition_items(
                partitioned_path=os.path.relpath(file_path, table_path), return_type=0
            )
            # 从类字段定义中去除无关字段后
            expected_csv_headers = [
                header
                for header in class_headers
                if header not in partition_fields and header not in base_model_header
            ]

            # 对比缺失和多余的字段
            missing_headers: set[str] = set(expected_csv_headers) - set(a_csv_headers)
            extra_headers: set[str] = set(a_csv_headers) - set(expected_csv_headers)

            if missing_headers:
                print(f"csv  Missing headers: {missing_headers} {file_path=}")
            if extra_headers:
                print(f"csv  Extra headers: {extra_headers} {file_path=}")

            # 如果有缺失或多余的字段，设置标记为 False
            if missing_headers or extra_headers:
                this_table_all_csv_header_is_formatted = False
                print(
                    f"header is error {file_path=} {extra_headers=} {missing_headers} {a_csv_headers=}"
                )

        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            return False, files_to_process  # 发生错误，退出程序

    return this_table_all_csv_header_is_formatted, files_to_process


from tqdm.contrib.concurrent import thread_map


def load_file_to_mysql(
    file_path: str, class_obj: base_model.BaseModel, table_path: str
) -> None:
    """
    加载单个 CSV 文件到数据库表中.

    :param file_path: 单个待加载的文件路径.
    :param class_obj: 具体的 ORM 类.
    :param table_path: 表的绝对路径.
    """
    try:
        print(f"start {file_path=}")
        table_name = class_obj.__tablename__
        base_model_header = sqlalchemy_orm_util.get_abstract_class_fields(
            base_model.BaseModel
        )
        class_headers = sqlalchemy_orm_util.get_class_fields(class_obj)

        # 提取分区字段
        partition_fields = extract_partition_items(
            partitioned_path=os.path.relpath(file_path, table_path), return_type=0
        )
        partition_kv = extract_ordered_partition_k_v_pairs_from_path(
            partitioned_path=os.path.relpath(file_path, table_path)
        )
        expected_csv_headers = [
            header
            for header in class_headers
            if header not in partition_fields and header not in base_model_header
        ]

        columns = ", ".join(expected_csv_headers)
        a_list = [
            f"{list(item.keys())[0]} = '{list(item.values())[0]}'"
            for item in partition_kv
        ]
        set_clause = ", ".join(a_list)

        # 初始化变量
        marketplace = 0
        root_category_id = -1
        year = 0
        week = 0

        for item in partition_kv:
            if "marketplace" in item:
                marketplace = item["marketplace"]
            elif "root_category_id" in item:
                root_category_id = item["root_category_id"]
            elif "year" in item:
                year = item["year"]
            elif "week" in item:
                week = item["week"]

        sql_load_data = f"""
            LOAD DATA LOCAL INFILE %s
            INTO TABLE {table_name}
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '\"' 
            LINES TERMINATED BY '\n'
            IGNORE 1 ROWS
            ({columns})
            SET {set_clause}
        """
        file_name = os.path.basename(file_path)

        with mysql.connector.connect(
            **DB_CONFIG, allow_local_infile=True
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_load_data, (file_path,))
                connection.commit()

                sql_insert_record = f"""
                    INSERT INTO db_junglescout_amazon.tb_loaded_records (table_name, marketplace, root_category_id, year, week, file_name)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        table_name =  VALUES(table_name),
                        marketplace =  VALUES(marketplace),
                        root_category_id = VALUES(root_category_id),
                        year = VALUES(year),
                        week = VALUES(week),
                        file_name = VALUES(file_name);
                """
                cursor.execute(
                    sql_insert_record,
                    (table_name, marketplace, root_category_id, year, week, file_name),
                )
                connection.commit()

        print(f"Done {file_path=}")  # 执行加载逻辑
        return True  # 表示成功
    except Exception as ex:
        print(f"{ex=}")
        return False  # 表示失败


def add_pk_to_js_org_table():
    with mysql.connector.connect(**DB_CONFIG) as connection:
        with connection.cursor() as cursor:
            sql_add_pk = """
                ALTER TABLE db_junglescout_amazon.tb_sales_estimates_weekly_v2 
                ADD CONSTRAINT tb_sales_estimates_weekly_v2_pk 
                PRIMARY KEY (marketplace, root_category_id, `year`, week, start_date, end_date, asin)
            """

            start_time = time.time()  # 记录开始时间
            try:
                cursor.execute(sql_add_pk)
                connection.commit()
            except mysql.connector.Error as err:
                print(f"Error: {err}")
            else:
                end_time = time.time()  # 记录结束时间
                execution_time = end_time - start_time
                print(
                    f"Primary key added successfully in {execution_time:.2f} seconds."
                )


# 调用函数


def load_partition_dir_2_mysql(
    files_to_process: list[str], class_obj: base_model.BaseModel, table_path: str
) -> None:
    """
    加载 CSV 文件到数据库表中.

    :param files_to_process: 所有待加载的文件列表.
    :param class_obj: 具体的 ORM 类.
    :param table_path: 表的绝对路径.
    """
    # 使用 tqdm 的并发加载
    thread_map(
        lambda file_path: load_file_to_mysql(file_path, class_obj, table_path),
        files_to_process,
        max_workers=1,  # 根据需要调整并发线程数 并发会锁表？
        desc=f"Loading files into {class_obj.__tablename__}",
    )


def validate_all_table_csv_headers(
    base_path: str = "/home/changliu/junglescout",
) -> bool:
    """
    校验所有表对应的 CSV 文件的头部.

    :param base_path: 基础路径.
    :return: 是否所有表的 CSV 文件头部格式正确.
    """
    all_table_is_ok = True

    create_table_if_not_exists(class_obj=TbLoadedRecords, db_config=DB_CONFIG)
    for relative_path, class_obj in mapping.TABLE_RELATIVE_PATH_CLASS_MAPPING.items():
        table_name = class_obj.__tablename__
        table_path = os.path.join(base_path, relative_path)  # 构建绝对路径
        this_table_all_csv_header_is_formatted, files_to_process = (
            validate_one_tb_partition_dir_csv_headers(
                table_path=table_path, class_obj=class_obj
            )
        )

        if this_table_all_csv_header_is_formatted:
            print(f"{table_name=} is ok to load")
            create_table_if_not_exists(class_obj=class_obj, db_config=DB_CONFIG)
            if files_to_process:  # 只有当有文件需要处理时才调用函数
                load_partition_dir_2_mysql(files_to_process, class_obj, table_path)
            else:
                print(f"No new files to process for {table_name}")
        else:
            print(f"{class_obj.__tablename__=} is not ok to load")
            all_table_is_ok = False

    return all_table_is_ok


def load_partition_data_to_data_product(partition_name: str):
    """
    加载特定分区的数据到 MySQL 数据库中。

    :param partition_name: 分区名称，例如 p0, p1 等。
    """
    sql_insert = f"""
        INSERT INTO db_junglescout_amazon.tb_data_product (
  marketplace,
  root_category_id,
  asin,
  seller_id,
  brand,
  name,
  image_url,
  category_path,
  category_name,
  category_id0,
  category_id1,
  category_id2,
  category_id3,
  category_id4,
  category_id5,
  category_id6,
  first_date_available,
  seller_num,
  seller_types,
  data_type,
  created_datetime,
  modified_datetime
)
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
    first_date_available,
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
    first_date_available,
      seller_id_mode,
      seller_types,
      ROW_NUMBER() OVER (PARTITION BY marketplace, root_category_id, asin ORDER BY breadcrumb_path_category_ids DESC, seller_id_mode DESC, image_url DESC) AS rn
    FROM
      db_junglescout_amazon.tb_sales_estimates_weekly_v2 
    PARTITION ({partition_name})
    
      
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
    first_date_available,
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
  SUBSTRING_INDEX(breadcrumb_path_category_ids, '|', 1) AS category_name,
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
  first_date_available,
  seller_num,
  seller_types,
  '0' as data_type,
  created_datetime,
  modified_datetime
FROM 
  SplitSellerId
ON DUPLICATE KEY UPDATE
  brand = VALUES(brand),
  name = VALUES(name),
  image_url = VALUES(image_url),
  category_path = VALUES(category_path),
  category_name = VALUES(category_name),
  category_id0 = VALUES(category_id0),
  category_id1 = VALUES(category_id1),
  category_id2 = VALUES(category_id2),
  category_id3 = VALUES(category_id3),
  category_id4 = VALUES(category_id4),
  category_id5 = VALUES(category_id5),
  category_id6 = VALUES(category_id6),
  first_date_available = VALUES(first_date_available),
  seller_num = VALUES(seller_num),
  seller_types = VALUES(seller_types),
  data_type = VALUES(data_type),
  modified_datetime = VALUES(modified_datetime);
    """
    try:
        with mysql.connector.connect(**DB_CONFIG) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET sql_mode = ''")  # 清除 SQL 模式
                cursor.execute(sql_insert)
                connection.commit()
                print(f"Data loaded successfully for partition {partition_name}.")
    except Exception as ex:
        print(f"Error loading data for partition {partition_name}: {ex}")


def load_partition_data_to_data_week(partition_name: str):
    """
    加载特定分区的数据到 MySQL 数据库中。

    :param partition_name: 分区名称，例如 p0, p1 等。
    """
    sql_insert = f"""
INSERT INTO db_junglescout_amazon.tb_data_week (
  created_datetime,
  modified_datetime,
  marketplace,
  root_category_id,
  year,
  week,
  start_date,
  end_date,
  asin,
  seller_id,
  is_available,
  category_rank,
  subcategory_rank,
  price,
  review_count,
  ratings,
  revenue,
  revenue_1p,
  revenue_3p,
  sales,
  sales_1p,
  sales_3p,
  revenue_org,
  revenue_1p_org,
  revenue_3p_org,
  sales_org,
  sales_1p_org,
  sales_3p_org
)
WITH RECURSIVE SplitSellerId AS (
  SELECT
    created_datetime,
    modified_datetime,
    marketplace,
    root_category_id,
    year,
    week,
    start_date,
    end_date,
    is_available,
    category_rank,
    subcategory_rank,
    price,
    review_count,
    ratings,
    revenue,
    revenue_1p,
    revenue_3p,
    sales,
    sales_1p,
    sales_3p,
    SUBSTRING_INDEX(seller_id_mode, '|', 1) AS seller_id,
    SUBSTRING(seller_id_mode, LENGTH(SUBSTRING_INDEX(seller_id_mode, '|', 1)) + 2) AS remaining_sellers,
    asin,
    (LENGTH(seller_id_mode) - LENGTH(REPLACE(seller_id_mode, '|', '')) + 1) AS seller_num
  FROM (
    SELECT
      created_datetime,
      modified_datetime,
      marketplace,
      root_category_id,
      year,
      week,
      start_date,
      end_date,
      asin,
      is_available,
      category_rank,
      subcategory_rank,
      price,
      review_count,
      ratings,
      revenue,
      revenue_1p,
      revenue_3p,
      sales,
      sales_1p,
      sales_3p,
      seller_id_mode
    FROM
      db_junglescout_amazon.tb_sales_estimates_weekly_v2
      PARTITION ({partition_name})
  ) AS RandomSelection
  UNION ALL
  SELECT
    created_datetime,
    modified_datetime,
    marketplace,
    root_category_id,
    year,
    week,
    start_date,
    end_date,
    is_available,
    category_rank,
    subcategory_rank,
    price,
    review_count,
    ratings,
    revenue,
    revenue_1p,
    revenue_3p,
    sales,
    sales_1p,
    sales_3p,
    SUBSTRING_INDEX(remaining_sellers, '|', 1) AS seller_id,
    SUBSTRING(remaining_sellers, LENGTH(SUBSTRING_INDEX(remaining_sellers, '|', 1)) + 2) AS remaining_sellers,
    asin,
    seller_num
  FROM
    SplitSellerId
  WHERE
    remaining_sellers != ''
)


SELECT
  created_datetime,
  modified_datetime,
  marketplace,
  root_category_id,
  year,
  week,
  start_date,
  end_date,
  asin,
  seller_id,
  is_available,
  category_rank,
  subcategory_rank,
  price,
  review_count,
  ratings,
  revenue / seller_num AS revenue,
  revenue_1p / seller_num AS revenue_1p,
  revenue_3p / seller_num AS revenue_3p,
  sales / seller_num AS sales,
  sales_1p / seller_num AS sales_1p,
  sales_3p / seller_num AS sales_3p,
  revenue AS revenue_org,
  revenue_1p AS revenue_1p_org,
  revenue_3p AS revenue_3p_org,
  sales AS sales_org,
  sales_1p AS sales_1p_org,
  sales_3p AS sales_3p_org
FROM
  SplitSellerId
ON DUPLICATE KEY UPDATE
  modified_datetime = VALUES(modified_datetime),
  is_available = VALUES(is_available),
  category_rank = VALUES(category_rank),
  subcategory_rank = VALUES(subcategory_rank),
  price = VALUES(price),
  review_count = VALUES(review_count),
  ratings = VALUES(ratings),
  revenue = VALUES(revenue),
  revenue_1p = VALUES(revenue_1p),
  revenue_3p = VALUES(revenue_3p),
  sales = VALUES(sales),
  sales_1p = VALUES(sales_1p),
  sales_3p = VALUES(sales_3p),
  revenue_org = VALUES(revenue_org),
  revenue_1p_org = VALUES(revenue_1p_org),
  revenue_3p_org = VALUES(revenue_3p_org),
  sales_org = VALUES(sales_org),
  sales_1p_org = VALUES(sales_1p_org),
  sales_3p_org = VALUES(sales_3p_org);
    """

    try:
        with mysql.connector.connect(**DB_CONFIG) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_insert)
                connection.commit()
                print(f"Data loaded successfully for partition {partition_name}.")
    except Exception as ex:
        print(f"Error loading data for partition {partition_name}: {ex}")


def execute_query(query):
    try:
        with mysql.connector.connect(**DB_CONFIG) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                connection.commit()
                print(f"done {query=}")
    except mysql.connector.Error as err:
        print(f"Error query {err}")


def set_global_setting():
    execute_query(query="SET GLOBAL sql_mode = ''")  # 清除全局 SQL 模式
    execute_query(query="SET GLOBAL local_infile = 1;")


def drop_primary_key_from_tb_sales_estimates():
    """
    删除 TbSalesEstimatesWeeklyV2 表的主键约束.
    """
    sql_drop_pk = """
    ALTER TABLE db_junglescout_amazon.tb_sales_estimates_weekly_v2 
    DROP PRIMARY KEY;
    """
    execute_query(query=sql_drop_pk)
    print("Primary key dropped successfully from tb_sales_estimates_weekly_v2.")


if __name__ == "__main__":
    base_path = "/home/changliu/junglescout"  # 替换为实际的绝对路径

    create_table_if_not_exists(class_obj=TbSalesEstimatesWeeklyV2, db_config=DB_CONFIG)
    create_table_if_not_exists(class_obj=TbDataProduct, db_config=DB_CONFIG)
    create_table_if_not_exists(class_obj=TbDataWeek, db_config=DB_CONFIG)
    set_global_setting()
    # drop_primary_key_from_tb_sales_estimates()
    all_table_is_ok = validate_all_table_csv_headers(
        base_path
    )  # 校验 CSV 文件的 Header
    # try:
    #     add_pk_to_js_org_table()
    # except Exception as ex:
    #     print(f"{ex=}")

    try:
        start_time = time.time()  # 记录整个过程开始时间

        # 调用函数处理每个分区
        partitions = [f"p{item}" for item in range(1024)]  # 根据您的分区名称调整

        # 使用 tqdm 显示进度条
        for partition in tqdm(partitions, desc="Loading data_product"):
            partition_start_time = time.time()  # 记录每个分区开始时间
            load_partition_data_to_data_product(partition)
            partition_end_time = time.time()  # 记录每个分区结束时间
            partition_execution_time = partition_end_time - partition_start_time
            print(
                f"Done loading data to data product for partition: {partition} in {partition_execution_time:.2f} seconds"
            )

        print("Done load_partition_data_to_data_product")

        for partition in tqdm(partitions, desc="Loading Data Week"):
            partition_start_time = time.time()  # 记录每个分区开始时间
            load_partition_data_to_data_week(partition)
            partition_end_time = time.time()  # 记录每个分区结束时间
            partition_execution_time = partition_end_time - partition_start_time
            print(
                f"Done loading data to data week for partition: {partition} in {partition_execution_time:.2f} seconds"
            )

        print("Done load_partition_data_to_data_week")

        end_time = time.time()  # 记录整个过程结束时间
        total_execution_time = end_time - start_time
        print(f"Total execution time: {total_execution_time:.2f} seconds")

    except Exception as ex:
        print(f"Error occurred: {ex}")
