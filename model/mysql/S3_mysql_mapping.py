from model.mysql import base_model
from model.mysql.server_108 import (db_junglescout_amazon,
                                    db_junglescout_amazon_v3)

# 文件路径和类对象的映射
# TABLE_RELATIVE_PATH_CLASS_MAPPING: dict[str, DeclarativeMeta] = {
TABLE_RELATIVE_PATH_CLASS_MAPPING: dict[str, base_model.BaseModel] = {
    "version=2/format=csv/table=category_tree_v2/": db_junglescout_amazon.TbCategoryTreeV2,
    "version=2/format=csv/table=category_tree_v2_latest/": db_junglescout_amazon.TbCategoryTreeV2Latest,
    "version=2/format=csv/table=sales_estimates_weekly_v2/": db_junglescout_amazon.TbSalesEstimatesWeeklyV2,
    "version=2/format=csv/table=sales_estimates_weekly_v2_latest/": db_junglescout_amazon.TbSalesEstimatesWeeklyV2Latest,
}


V3_TABLE_RELATIVE_PATH_CLASS_MAPPING: dict[str, base_model.BaseModel] = {
    "version=3/format=csv/table=category_tree/": db_junglescout_amazon_v3.TbCategoryTree,
    "version=3/format=csv/table=category_tree_latest/": db_junglescout_amazon_v3.TbCategoryTreeLatest,
    "version=3/format=csv/table=sales_estimates_weekly/": db_junglescout_amazon_v3.TbSalesEstimatesWeekly,
    "version=3/format=csv/table=sales_estimates_weekly_latest/": db_junglescout_amazon_v3.TbSalesEstimatesWeeklyLatest,
    "version=3/format=csv/table=sales_estimates_monthly/": db_junglescout_amazon_v3.TbSalesEstimatesMonthly,
    "version=3/format=csv/table=sales_estimates_monthly_latest/": db_junglescout_amazon_v3.TbSalesEstimatesMonthlyLatest,
}
