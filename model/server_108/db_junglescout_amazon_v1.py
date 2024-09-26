from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)

from ..base_model import BaseModel  # 导入基类


# 定义 tb_loaded_records 表的 ORM
class TbLoadedRecords(BaseModel):
    __tablename__ = "tb_loaded_records"

    table_name = Column(
        String(100), nullable=False, default="tb_sales_estimates_weekly_v2"
    )
    marketplace = Column(String(5), default=None)
    root_category_id = Column(BigInteger, default=None)
    year = Column(Integer, default=None)
    week = Column(String(3), default=None)
    file_name = Column(String(255), default=None)

    __table_args__ = (
        UniqueConstraint(
            "table_name",
            "marketplace",
            "root_category_id",
            "year",
            "week",
            "file_name",
            name="tb_loaded_records_unique",
        ),
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
        UniqueConstraint(
            "marketplace",
            "year",
            "week",
            "end_date",
            "category_id",
            name="uk_tb_category_tree_v2",
        ),
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
        UniqueConstraint(
            "marketplace",
            "end_date",
            "category_id",
            name="uk_tb_category_tree_v2_latest",
        ),
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
        UniqueConstraint(
            "marketplace",
            "root_category_id",
            "year",
            "week",
            "end_date",
            "asin",
            name="uk_tb_sales_estimates_weekly_v2",
        ),
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
        UniqueConstraint(
            "marketplace",
            "root_category_id",
            "end_date",
            "asin",
            name="uk_tb_sales_estimates_weekly_v2_latest",
        ),
    )
