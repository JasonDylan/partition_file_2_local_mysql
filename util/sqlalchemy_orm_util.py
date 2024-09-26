from typing import Type

from sqlalchemy import Column, create_engine, inspect
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import sessionmaker

from config.config import DB_CONFIG  # 引入配置


def create_table_if_not_exists(class_obj: DeclarativeMeta, db_config=DB_CONFIG) -> None:
    """
    创建表，如果表不存在的话.

    :param class_obj: 具体的 ORM 类.
    """
    # 使用 SQLAlchemy 创建数据库引擎
    engine = create_engine(
        f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
    )
    Session = sessionmaker(bind=engine)
    session = Session()

    # 创建表
    class_obj.metadata.create_all(engine)
    print(f"Table {class_obj.__tablename__} created or already exists.")
    session.close()


def get_abstract_class_fields(abstract_class: type) -> list[str]:
    """
    获取抽象类中的字段名.

    :param abstract_class: 抽象类，通常是 BaseModel.
    :return: 字段名列表.
    """
    return [
        attr
        for attr in dir(abstract_class)
        if isinstance(getattr(abstract_class, attr), Column)
    ]


def get_class_fields(cls: type) -> list[str]:
    """
    获取具体类的字段名.

    :param cls: 具体的 ORM 类.
    :return: 字段名列表.
    """
    return [column.name for column in inspect(cls).c]
