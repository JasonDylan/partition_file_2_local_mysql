"""

本基类定义了数据模型遵循的规范，确保数据的一致性和完整性。
所有继承该基类的表将自动包含以下字段，以便于数据库管理和数据追踪。

字段说明：
- id: 主键，自动递增，唯一标识每一条记录。
- created_datetime: 记录创建时间，默认为当前时间。
- modified_datetime: 记录最后修改时间，默认为当前时间，并在每次更新时自动修改。

此基类旨在为数据组提供统一的结构，确保在数据操作过程中遵循一致的规范，从而提升数据的可维护性和可追溯性。

设立依据：
根据文档
https://hkaift-my.sharepoint.com/:t:/p/xucanxiang/EXsj8dRAYphMnv1aY-lpJU0BrNYFNOeMLNzE415mtd52kg?e=67aDSO
《数据库规则怪谈十则（其一）》.md

必要性有待进一步实践或者理论
"""

from sqlalchemy import BigInteger, Column, DateTime
from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base
from sqlalchemy.sql import func

Base: DeclarativeMeta = declarative_base()


class BaseModel(Base):
    __abstract__ = True  # 标记为抽象类，不会创建表
    __tablename__ = None  # 显示声明用于代码提示

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    created_datetime = Column(DateTime, nullable=False, server_default=func.now())
    modified_datetime = Column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now()
    )
