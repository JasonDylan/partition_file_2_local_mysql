from sqlalchemy import (BigInteger, Boolean, Column, Date, DateTime, Integer,
                        Numeric, PrimaryKeyConstraint, String)
from sqlalchemy.dialects.mysql import JSON

from ..base_model import BaseModel


# Base class for sales estimates tables
class BaseSalesEstimates(BaseModel):
    __abstract__ = True

    marketplace = Column(String(5), nullable=False)
    root_category_id = Column(BigInteger, nullable=False)
    
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)
    asin = Column(String(40), nullable=False)
    is_available = Column(Boolean)
    brand = Column(String(1000))
    title = Column(String(1000))
    category_ranks = Column(JSON)  # Array of objects with category_id and category_rank
    image_url_sample = Column(String(1000))
    average_price = Column(Numeric(10, 2))
    parent_rating_count = Column(BigInteger)
    average_rating = Column(Numeric(3, 1))
    breadcrumb_path_category_ids = Column(JSON)  # Array of category IDs
    first_date_available = Column(DateTime)
    revenue = Column(Numeric(20, 2))
    revenue_1p = Column(Numeric(20, 2))
    revenue_3p = Column(Numeric(20, 2))
    sales = Column(Numeric(20, 2))
    sales_1p = Column(Numeric(20, 2))
    sales_3p = Column(Numeric(20, 2))
    dominant_seller_id = Column(String(100))
    sellers = Column(JSON)  # Array of objects with seller_id and seller_type


class TbSalesEstimatesWeekly(BaseSalesEstimates):
    __tablename__ = "tb_sales_estimates_weekly"

    year = Column(Integer, nullable=False)
    week = Column(String(3), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint(
            "marketplace",
            "root_category_id",
            "year",
            "week",
            "start_date",
            "end_date",
            "asin",
            name="pk_tb_sales_estimates_weekly",
        ),
        {
            "mysql_engine": "InnoDB",
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_0900_bin",
            "mysql_partition_by": "KEY(asin)",
            "mysql_partitions": "1024",
        },
    )


class TbSalesEstimatesWeeklyLatest(BaseSalesEstimates):
    __tablename__ = "tb_sales_estimates_weekly_latest"

    __table_args__ = (
        PrimaryKeyConstraint(
            "marketplace",
            "root_category_id",
            "start_date",
            "end_date",
            "asin",
            name="pk_tb_sales_estimates_weekly_latest",
        ),
        {
            "mysql_engine": "InnoDB",
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_0900_bin",
            "mysql_partition_by": "KEY(asin)",
            "mysql_partitions": "1024",
        },
    )


class TbSalesEstimatesMonthly(BaseSalesEstimates):
    __tablename__ = "tb_sales_estimates_monthly"

    year = Column(Integer, nullable=False)
    month = Column(String(2), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint(
            "marketplace",
            "root_category_id",
            "year",
            "month",
            "start_date",
            "end_date",
            "asin",
            name="pk_tb_sales_estimates_monthly",
        ),
        {
            "mysql_engine": "InnoDB",
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_0900_bin",
            "mysql_partition_by": "KEY(asin)",
            "mysql_partitions": "1024",
        },
    )


class TbSalesEstimatesMonthlyLatest(BaseSalesEstimates):
    __tablename__ = "tb_sales_estimates_monthly_latest"

    __table_args__ = (
        PrimaryKeyConstraint(
            "marketplace",
            "root_category_id",
            "start_date",
            "end_date",
            "asin",
            name="pk_tb_sales_estimates_monthly_latest",
        ),
        {
            "mysql_engine": "InnoDB",
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_0900_bin",
            "mysql_partition_by": "KEY(asin)",
            "mysql_partitions": "1024",
        },
    )


# Base class for category tree tables
class BaseCategoryTree(BaseModel):
    __abstract__ = True

    marketplace = Column(String(5), nullable=False)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)
    category_id = Column(BigInteger, nullable=False)
    active = Column(Boolean)
    category_name = Column(String(768))
    preceding_category_id = Column(BigInteger)
    root_category_id = Column(BigInteger)
    root_category_name = Column(String(768))
    path_by_id = Column(String(768))
    path_by_name = Column(String(768))
    path_by_id_array = Column(String(768))
    path_by_name_array = Column(String(768))
    category_tree_level = Column(Integer)
    subcategory_count = Column(Integer)
    updated_at = Column(Date)


class TbCategoryTree(BaseCategoryTree):
    __tablename__ = "tb_category_tree"

    year = Column(Integer, nullable=False)
    week = Column(String(3), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint(
            "marketplace",
            "year",
            "week",
            "end_date",
            "category_id",
            name="pk_tb_category_tree",
        ),
        {"mysql_charset": "utf8mb4", "mysql_collate": "utf8mb4_0900_bin"},
    )


class TbCategoryTreeLatest(BaseCategoryTree):
    __tablename__ = "tb_category_tree_latest"

    __table_args__ = (
        PrimaryKeyConstraint(
            "marketplace",
            "end_date",
            "start_date",
            "category_id",
            name="pk_tb_category_tree_latest",
        ),
        {"mysql_charset": "utf8mb4", "mysql_collate": "utf8mb4_0900_bin"},
    )
