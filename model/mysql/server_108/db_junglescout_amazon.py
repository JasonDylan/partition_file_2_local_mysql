from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
)

from ..base_model import BaseModel  # 导入基类


class TbDataWeek(BaseModel):
    __tablename__ = "tb_data_week"

    marketplace = Column(String(5), nullable=False)
    root_category_id = Column(BigInteger, nullable=False)
    year = Column(Integer, nullable=False)
    week = Column(String(3), nullable=False)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)
    asin = Column(String(40), nullable=False)
    seller_id = Column(String(100), server_default="", nullable=False)
    is_available = Column(Integer)
    category_rank = Column(BigInteger, comment="rank")
    subcategory_rank = Column(BigInteger)
    price = Column(Numeric(10, 2))
    review_count = Column(BigInteger, comment="review")
    ratings = Column(Numeric(3, 1))
    revenue = Column(Numeric(20, 2))
    revenue_1p = Column(Numeric(20, 2))
    revenue_3p = Column(Numeric(20, 2))
    sales = Column(Numeric(20, 2))
    sales_1p = Column(Numeric(20, 2))
    sales_3p = Column(Numeric(20, 2))
    revenue_org = Column(Numeric(20, 2))
    revenue_1p_org = Column(Numeric(20, 2))
    revenue_3p_org = Column(Numeric(20, 2))
    sales_org = Column(Numeric(20, 2))
    sales_1p_org = Column(Numeric(20, 2))
    sales_3p_org = Column(Numeric(20, 2))

    __table_args__ = (
        PrimaryKeyConstraint(
            "marketplace",
            "root_category_id",
            "year",
            "week",
            "start_date",
            "end_date",
            "asin",
            "seller_id",
            name="pk_tb_data_week",
        ),
        {
            "mysql_engine": "InnoDB",
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_0900_bin",
            "mysql_partition_by": "KEY(asin)",
            "mysql_partitions": "1024",
        },
    )


class TbDataProduct(BaseModel):
    __tablename__ = "tb_data_product"

    marketplace = Column(String(5), nullable=False)
    root_category_id = Column(BigInteger, nullable=False)
    asin = Column(String(40), nullable=False)
    seller_id = Column(String(100), server_default="", nullable=False)
    brand = Column(String(1000))
    name = Column(String(1000), comment="title")
    image_url = Column(String(1000))
    category_name = Column(String(1000))
    category_path = Column(String(1000))
    category_id0 = Column(BigInteger)
    category_id1 = Column(BigInteger)
    category_id2 = Column(BigInteger)
    category_id3 = Column(BigInteger)
    category_id4 = Column(BigInteger)
    category_id5 = Column(BigInteger)
    category_id6 = Column(BigInteger)
    first_date_available = Column(DateTime, comment="publish_date_2")
    seller_num = Column(Integer, server_default="0")
    seller_types = Column(String(100), comment="bbSeller")
    data_type = Column(
        Integer, nullable=False, server_default="0", comment="数据清理状态"
    )

    __table_args__ = (
        PrimaryKeyConstraint(
            "marketplace",
            "root_category_id",
            "asin",
            "seller_id",
            name="pk_tb_data_product",
        ),
        {
            "mysql_engine": "InnoDB",
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_0900_bin",
            "mysql_partition_by": "KEY(asin)",
            "mysql_partitions": "1024",
        },
    )


# 定义 tb_loaded_records 表的 ORM
class TbLoadedRecords(BaseModel):
    __tablename__ = "tb_loaded_records"

    table_name = Column(
        String(100), nullable=False, default="tb_sales_estimates_weekly_v2"
    )
    marketplace = Column(String(50), default="0")
    root_category_id = Column(BigInteger, default=-1)
    year = Column(Integer, default=0)
    week = Column(String(10), default="0")
    file_name = Column(String(255), default=0)

    __table_args__ = (
        PrimaryKeyConstraint(
            "table_name",
            "marketplace",
            "root_category_id",
            "year",
            "week",
            "file_name",
            name="tb_loaded_records_unique",
        ),
        {"mysql_charset": "utf8mb4", "mysql_collate": "utf8mb4_0900_bin"},
    )


# 定义 tb_category_tree_v2 表的 ORM
class TbCategoryTreeV2(BaseModel):
    __tablename__ = "tb_category_tree_v2"

    marketplace = Column(String(5))
    year = Column(Integer)
    week = Column(String(3))

    end_date = Column(Date)
    start_date = Column(Date)
    category_id = Column(BigInteger)
    active = Column(Boolean)
    category_name = Column(String(768))
    preceding_category_id = Column(BigInteger)  # 修正为 BigInteger
    root_category_id = Column(BigInteger)  # 修正为 BigInteger
    root_category_name = Column(String(768))
    path_by_id = Column(String(768))
    path_by_name = Column(String(768))
    path_by_id_array = Column(String(768))
    path_by_name_array = Column(String(768))
    category_tree_level = Column(Integer)
    subcategory_count = Column(Integer)
    updated_at = Column(Date)

    __table_args__ = (
        PrimaryKeyConstraint(
            "marketplace",
            "year",
            "week",
            "end_date",
            "category_id",
            name="pk_tb_category_tree_v2",
        ),
        {"mysql_charset": "utf8mb4", "mysql_collate": "utf8mb4_0900_bin"},
    )


# 定义 tb_category_tree_v2_latest 表的 ORM
class TbCategoryTreeV2Latest(BaseModel):
    __tablename__ = "tb_category_tree_v2_latest"

    marketplace = Column(String(5))

    end_date = Column(Date)
    start_date = Column(Date)
    category_id = Column(BigInteger)
    active = Column(Boolean)
    category_name = Column(String(768))
    preceding_category_id = Column(BigInteger)  # 修正为 BigInteger
    root_category_id = Column(BigInteger)  # 修正为 BigInteger
    root_category_name = Column(String(768))
    path_by_id = Column(String(768))
    path_by_name = Column(String(768))
    path_by_id_array = Column(String(768))
    path_by_name_array = Column(String(768))
    category_tree_level = Column(Integer)
    subcategory_count = Column(Integer)
    updated_at = Column(Date)

    __table_args__ = (
        PrimaryKeyConstraint(
            "marketplace",
            "end_date",
            "start_date",
            "category_id",
            name="pk_tb_category_tree_v2_latest",
        ),
        {"mysql_charset": "utf8mb4", "mysql_collate": "utf8mb4_0900_bin"},
    )


# 定义 tb_sales_estimates_weekly_v2 表的 ORM
class TbSalesEstimatesWeeklyV2(BaseModel):
    __tablename__ = "tb_sales_estimates_weekly_v2"

    marketplace = Column(String(5))
    root_category_id = Column(BigInteger)  # 修正为 BigInteger
    year = Column(Integer)
    week = Column(String(3))

    start_date = Column(Date)
    end_date = Column(Date)
    asin = Column(String(40))
    is_available = Column(Integer)
    category_rank = Column(BigInteger)  # 修正为 BigInteger
    subcategory_rank = Column(BigInteger)  # 修正为 BigInteger
    brand = Column(String(1000))
    name = Column(String(1000))
    image_url = Column(String(1000))
    price = Column(Numeric(10, 2))
    review_count = Column(BigInteger)  # 修正为 BigInteger
    ratings = Column(Numeric(3, 1))
    breadcrumb_path_category_ids = Column(String(1000))
    ranking_category_ids = Column(String(1000))
    first_date_available = Column(DateTime)
    revenue = Column(Numeric(20, 2))
    revenue_1p = Column(Numeric(20, 2))
    revenue_3p = Column(Numeric(20, 2))
    sales = Column(Numeric(20, 2))
    sales_1p = Column(Numeric(20, 2))
    sales_3p = Column(Numeric(20, 2))
    seller_ids = Column(String(1000))
    seller_id_mode = Column(String(1000))
    seller_types = Column(String(100))

    __table_args__ = (
        PrimaryKeyConstraint(
            "marketplace",
            "root_category_id",
            "year",
            "week",
            "start_date",
            "end_date",
            "asin",
            name="pk_tb_sales_estimates_weekly_v2",
        ),
        {
            "mysql_engine": "InnoDB",
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_0900_bin",
            "mysql_partition_by": "KEY(asin)",
            "mysql_partitions": "1024",
        },
    )


# 定义 tb_sales_estimates_weekly_v2_latest 表的 ORM
class TbSalesEstimatesWeeklyV2Latest(BaseModel):
    __tablename__ = "tb_sales_estimates_weekly_v2_latest"

    marketplace = Column(String(5))
    root_category_id = Column(BigInteger)  # 修正为 BigInteger

    start_date = Column(Date)
    end_date = Column(Date)
    asin = Column(String(40))
    is_available = Column(Integer)
    category_rank = Column(BigInteger)  # 修正为 BigInteger
    subcategory_rank = Column(BigInteger)  # 修正为 BigInteger
    brand = Column(String(1000))
    name = Column(String(1000))
    image_url = Column(String(1000))
    price = Column(Numeric(10, 2))
    review_count = Column(BigInteger)  # 修正为 BigInteger
    ratings = Column(Numeric(3, 1))
    breadcrumb_path_category_ids = Column(String(1000))
    ranking_category_ids = Column(String(1000))
    first_date_available = Column(DateTime)
    revenue = Column(Numeric(20, 2))
    revenue_1p = Column(Numeric(20, 2))
    revenue_3p = Column(Numeric(20, 2))
    sales = Column(Numeric(20, 2))
    sales_1p = Column(Numeric(20, 2))
    sales_3p = Column(Numeric(20, 2))
    seller_ids = Column(String(1000))
    seller_id_mode = Column(String(1000))
    seller_types = Column(String(100))

    __table_args__ = (
        PrimaryKeyConstraint(
            "marketplace",
            "root_category_id",
            "start_date",
            "end_date",
            "asin",
            name="pk_tb_sales_estimates_weekly_v2_latest",
        ),
        {"mysql_charset": "utf8mb4", "mysql_collate": "utf8mb4_0900_bin"},
    )
