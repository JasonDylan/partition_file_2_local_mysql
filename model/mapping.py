from sqlalchemy.ext.declarative import DeclarativeMeta

from model import base_model
from model.server_108 import db_junglescout_amazon_v1

# 文件路径和类对象的映射
# TABLE_RELATIVE_PATH_CLASS_MAPPING: dict[str, DeclarativeMeta] = {
TABLE_RELATIVE_PATH_CLASS_MAPPING: dict[str, base_model.BaseModel] = {
    "version=2/format=csv/table=category_tree_v2/": db_junglescout_amazon_v1.TbCategoryTreeV2,
    "version=2/format=csv/table=category_tree_v2_latest/": db_junglescout_amazon_v1.TbCategoryTreeV2Latest,
    # "version=2/format=csv/table=sales_estimates_weekly_v2/": db_junglescout_amazon_v1.TbSalesEstimatesWeeklyV2,
    "version=2/format=csv/table=sales_estimates_weekly_v2_latest/": db_junglescout_amazon_v1.TbSalesEstimatesWeeklyV2Latest,
}
