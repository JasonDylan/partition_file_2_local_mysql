"""
模块名称: 数据库加载工具
描述: 该模块提供功能以校验 CSV 文件的头部并将其加载到 MySQL 数据库中。
"""

import os

import mysql.connector
import pandas as pd
from tqdm import tqdm  # 导入 tqdm

from config.config import DB_CONFIG  # 引入配置
from model import base_model, mapping
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
    all_files = get_a_table_all_file_by_format(table_path)

    # 使用 tqdm 显示进度条
    for file_path in tqdm(
        all_files, desc=f"Processing files in {class_obj.__tablename__}"
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
            return False, all_files  # 发生错误，退出程序

    return this_table_all_csv_header_is_formatted, all_files


def load_partition_dir_2_mysql(
    all_files: list[str], class_obj: base_model.BaseModel, table_path: str
) -> None:
    """
    加载 CSV 文件到数据库表中.

    :param all_files: 所有待加载的文件列表.
    :param class_obj: 具体的 ORM 类.
    :param table_path: 表的绝对路径.
    """
    table_name = class_obj.__tablename__
    base_model_header = sqlalchemy_orm_util.get_abstract_class_fields(
        base_model.BaseModel
    )
    # 获取类的字段名称
    class_headers = sqlalchemy_orm_util.get_class_fields(class_obj)

    for file_path in tqdm(all_files, desc=f"Load files in {table_name=}"):

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

        # 构建 SQL 语句
        columns = ", ".join(expected_csv_headers)

        # 构建 SET 子句
        a_list = [
            f"{list(item.keys())[0]} = '{list(item.values())[0]}'"
            for item in partition_kv
        ]
        print(f"{partition_kv=} {file_path=} {table_path=} {a_list=}")
        set_clause = ", ".join(a_list)

        # 初始化变量
        marketplace = None
        root_category_id = None  # 假设根类别 ID 在列表中不存在
        year = None
        week = None

        # 遍历 partition_kv 列表以获取相关信息
        for item in partition_kv:
            if "marketplace" in item:
                marketplace = item["marketplace"]
            elif "root_category_id" in item:
                root_category_id = item["root_category_id"]
            elif "year" in item:
                year = item["year"]
            elif "week" in item:
                week = item["week"]

        # 构建 LOAD DATA SQL 语句
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
        print(f"{sql_load_data=}")
        file_name = os.path.basename(file_path)

        # 连接数据库并执行插入操作
        with mysql.connector.connect(
            **DB_CONFIG, allow_local_infile=True
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_load_data, (file_path,))
                connection.commit()

                # 插入或更新已加载记录到 loaded_records 表
                sql_insert_record = f"""
                    INSERT INTO db_junglescout_amazon_v1.tb_loaded_records (table_name, marketplace, root_category_id, year, week, file_name)
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


def validate_all_table_csv_headers(
    base_path: str = "/home/changliu/junglescout",
) -> bool:
    """
    校验所有表对应的 CSV 文件的头部.

    :param base_path: 基础路径.
    :return: 是否所有表的 CSV 文件头部格式正确.
    """
    all_table_is_ok = True
    for relative_path, class_obj in mapping.TABLE_RELATIVE_PATH_CLASS_MAPPING.items():
        table_name = class_obj.__tablename__
        table_path = os.path.join(base_path, relative_path)  # 构建绝对路径
        this_table_all_csv_header_is_formatted, all_files = (
            validate_one_tb_partition_dir_csv_headers(
                table_path=table_path, class_obj=class_obj
            )
        )

        if this_table_all_csv_header_is_formatted:
            print(f"{table_name=} is ok to load")
            create_table_if_not_exists(class_obj=class_obj, db_config=DB_CONFIG)
            load_partition_dir_2_mysql(all_files, class_obj, table_path)
        else:
            print(f"{class_obj.__tablename__=} is not ok to load")
            all_table_is_ok = False

    return all_table_is_ok


if __name__ == "__main__":
    base_path = "/home/changliu/junglescout"  # 替换为实际的绝对路径

    all_table_is_ok = validate_all_table_csv_headers(
        base_path
    )  # 校验 CSV 文件的 Header
