"""
最新类在192.168.2.122: AmazonEcommerceDataPipeline/util/base_handler.py
github: https://github.com/hkaift-com/keepa_20240305
BaseHandler 是 亚马逊 项目中的一个核心类，用于处理一些亚马逊的全局配置，以及mongodb和mysql数据库的连接。
比如货币与国家代码的映射，marketplace_id 与 country_code 的映射，以及一些通用的方法。
包括：
A.映射的变量
    1. currency_to_country:     货币与国家代码的映射
    2. marketplace_to_country:  marketplace_id 与 country_code 的映射
    3. country_to_domain:       country_code 与 domain_id 的映射
    4. domain_to_country:       domain_id 与 country_code 的映射
    5. domain_to_suffix:        domain_id 与 suffix 的映射
    6. domain_to_language:      domain_id 与 language 的映射 编码ISO 639-1 语言代码
B. 数据库连接方法
    1. __init__:                初始化BaseHandler以确保静态数据被加载
    2. get_engine_by_database:  获取engine
    3. get_collection_name:     根据collection_name和domain_id获取collection
    4. get_mongo_collection:    获取mongodb collection
    5. close_connections:       关闭连接
    6. handle_error:            处理错误
    7. build_timestamp_query:   构建mongodb timestamp查询
    8. build_id_query:          构建mongodb id查询
    9. build_query:             构建 mysql查询
"""

import contextlib
import json
import logging
import os
import re
import traceback
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Type, Union

import mysql.connector
import pymongo
import retrying
import sqlalchemy
from dateutil import parser
from project_config.project_config import MYSQL_DEFAULT_SUPPORT_DATA_CONFIG
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.sql import text


def with_db_retry(
    max_connection_attempts=None, max_deadlock_attempts=10, max_other_attempts=3
):
    """统一的数据库错误重试装饰器"""

    def should_retry(e):
        if isinstance(e, mysql.connector.Error):
            if e.errno == 2003:
                logging.error(f"数据库连接错误(将重试): {str(e)}", exc_info=True)
                return "connection"
            elif e.errno == 1205:
                logging.error(
                    f"数据库死锁错误(将在20分钟后重试): {str(e)}", exc_info=True
                )
                return "deadlock"
        logging.error(f"数据库操作错误(将重试): {str(e)}", exc_info=True)
        return "other"

    def wait_gen(attempt_number, e=None):  # 添加attempt_number参数
        error_type = should_retry(e)
        if error_type == "connection":
            return 500000  # 连接错误等待500秒
        elif error_type == "deadlock":
            return 1200000  # 死锁错误等待20分钟
        else:
            return 1000  # 其他错误等待1秒

    def stop_func(attempt, e):
        error_type = should_retry(e)
        if error_type == "connection":
            return max_connection_attempts and attempt >= max_connection_attempts
        elif error_type == "deadlock":
            return attempt >= max_deadlock_attempts
        else:
            return attempt >= max_other_attempts

    return retrying.retry(
        retry_on_exception=lambda e: bool(should_retry(e)),
        wait_func=wait_gen,
        stop_func=stop_func,
    )


@with_db_retry()  # 使用默认配置
def execute_with_retry(
    conn: sqlalchemy.engine.Connection,
    query: str,
    params: Optional[Dict[str, Any]] = None,
    description: Optional[str] = "",
    context: Optional[str] = None,
):
    """执行 SQL 查询并处理重试

    Args:
        conn: SQLAlchemy 数据库连接
        query: SQL查询语句
        params: 查询参数
        description: sql描述信息, 用于日志输出 可以用params填充
        context: 上下文信息, 用于日志输出
    Returns:
        查询结果

    Raises:
        Exception: 数据库执行错误
    """
    if params:
        try:
            description = description.format(**params)
        except KeyError:
            # 如果描述中的变量在params中找不到，保持原样
            pass

    if context:
        description = f"{context}:{description}"

    logging.info(f"{description} 开始执行 ")
    try:
        with conn.begin():  # 开始一个事务
            # 确保query是字符串类型
            if isinstance(query, sqlalchemy.sql.elements.TextClause):
                query = str(query)

            result = (
                conn.execute(text(query), params)
                if params
                else conn.execute(text(query))
            )
            logging.info(f"{description} 完成执行 , 影响行数: {result.rowcount}")
            return result
    except Exception as e:
        logging.error(f"{description} SQL执行错误: {str(e)}\n{traceback.format_exc()}")
        raise


def check_mysql_config(
    project_config: Dict[str, Any], config_name: str = "mysql_config"
) -> None:
    """检查MySQL配置是否包含所需的所有字段

    Args:
        project_config: MySQL配置字典
        config_name: 配置名称，用于错误信息

    Raises:
        ValueError: 当缺少必需的配置字段时
    """
    required_fields = ["database", "username", "password", "host", "port"]
    for field in required_fields:
        if field not in project_config:
            raise ValueError(f"{config_name} must contain a '{field}' key")


def create_mysql_engine(project_config: Dict[str, Any], **kwargs) -> sqlalchemy.engine.Engine:
    """根据配置创建MySQL engine

    Args:
        project_config: MySQL配置字典
        **kwargs: 传递给create_engine的额外参数

    Returns:
        SQLAlchemy engine实例
    """
    engine_url = URL.create(
        "mysql+pymysql",
        username=project_config["username"],
        password=project_config["password"],
        host=project_config["host"],
        port=project_config["port"],
        database=project_config["database"],
    )

    default_engine_args = {
        "pool_size": 5,
        "max_overflow": 10,
        "pool_timeout": 60,
        "pool_recycle": 1800,
        "pool_pre_ping": True,
        "max_identifier_length": 64,
    }

    # 更新默认参数
    default_engine_args.update(kwargs)

    return create_engine(engine_url, **default_engine_args)


class BaseHandler:
    currency_to_country: Dict[str, str] = {}
    marketplace_to_country: Dict[str, str] = {}
    country_to_domain: Dict[str, int] = {}
    domain_to_country: Dict[int, str] = {}
    domain_to_language: Dict[int, str] = {}
    domain_to_suffix: Dict[int, str] = {}

    def __init__(
        self,
        mongo_config: Dict[str, Any] = None,
        mysql_config: Dict[str, Any] = None,
        target_mysql_configs: Optional[Dict[str, Dict[str, Any]]] = None,
        batch_size: int = 1000,
    ):
        self.batch_size = batch_size
        self.mongo_config = mongo_config
        self.mysql_config = mysql_config
        self.target_mysql_configs = target_mysql_configs or {
            mysql_config["database"]: mysql_config
        }
        self.mysql_db_name = None
        self.mongo_db_name = None
        self.sp_config = None
        self.mongo_db = None
        self.mongo_client = None
        self.engine = None
        self.target_engines: Dict[str, Any] = {}

        # 加载支持数据库配置并创建engine
        self.config_mysql_support_data = MYSQL_DEFAULT_SUPPORT_DATA_CONFIG
        if self.config_mysql_support_data:
            check_mysql_config(self.config_mysql_support_data, "support_data_config")
            self.config_engine = create_mysql_engine(self.config_mysql_support_data)
            self.load_mapping_data(self.config_engine)

        # MongoDB setup
        if mongo_config:
            self.mongo_db_name = mongo_config["database"]
            self.mongo_client = pymongo.MongoClient(
                f"mongodb://{mongo_config['username']}:{mongo_config['password']}@{mongo_config['host']}:{mongo_config['port']}"
            )
            self.mongo_db = self.mongo_client[self.mongo_db_name]
        else:
            logging.warning(
                "mongo_config is None.(if you don't need mongo, just ignore this)"
            )

        # SQLAlchemy setup
        if mysql_config:
            check_mysql_config(mysql_config)
            self.mysql_db_name = mysql_config["database"]
            self.engine = create_mysql_engine(mysql_config)

            # 创建target engines
            for db_name, project_config in self.target_mysql_configs.items():
                logging.info(f"Creating engine for database: {db_name}")
                check_mysql_config(project_config, f"target_config_{db_name}")
                target_engine = create_mysql_engine(project_config)
                self.target_engines[project_config["database"]] = target_engine
            logging.info(f"Created {len(self.target_engines)} target engines")
        else:
            logging.warning(
                "mysql_config is None.(if you don't need mysql, just ignore this)"
            )

    @staticmethod
    def get_country_code_from_currency(currency_code: str) -> str:
        """根据货币代码获取对应的国家代码，与数据库生成规则保持一致"""
        return BaseHandler.currency_to_country.get(currency_code, "US")  # 默认返回US

    @staticmethod
    def get_country_code_from_marketplace_id(marketplace_id: str) -> str:
        """根据marketplace_id获取对应的country_code"""
        return BaseHandler.marketplace_to_country.get(marketplace_id, "US")

    @staticmethod
    def get_country_code_from_market(market: str) -> str:
        """从market获取country_code"""
        # 从market中提取后缀，例如从"Amazon.com"提取".com"
        suffix = "." + market.split(".", 1)[1] if "." in market else ".com"

        # 通过suffix查找domain_id
        for domain_id, domain_suffix in BaseHandler.domain_to_suffix.items():
            if domain_suffix == suffix:
                return BaseHandler.domain_to_country.get(domain_id, "US")
        return "US"

    def get_engine_by_database(self, mysql_config):
        return create_engine(
            f"mysql+pymysql://{mysql_config['username']}:{mysql_config['password']}@{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}",
        )

    @staticmethod
    def load_country_data() -> Dict[str, int]:
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "project_config",
            "region_country_marketplace_domain_suffix_keepa_id.json",
        )
        with open(config_path, "r") as f:
            data = json.load(f)

        country_to_domain = {}
        domain_to_suffix = {}
        for region_data in data["regions"].values():
            for country in region_data["countries"]:
                if country["number"] is not None:
                    country_to_domain[country["country_code"]] = country["number"]
                    domain_to_suffix[country["number"]] = country["domain_suffix"]
        return country_to_domain, domain_to_suffix

    @staticmethod
    def get_collections_with_domain_ids(mode: str) -> Dict[str, int]:
        country_to_domain, _ = BaseHandler.load_country_data()
        if mode == "seller":
            return {
                "seller": 1,  # US
                **{
                    f"seller_{country.lower()}": domain_id
                    for country, domain_id in country_to_domain.items()
                    if country != "US"
                },
            }
        elif mode == "product":
            return {
                "data": 1,  # US
                **{
                    f"data_{country.lower()}": domain_id
                    for country, domain_id in country_to_domain.items()
                    if country != "US"
                },
            }
        else:
            raise ValueError(f"Invalid mode: {mode}")

    def get_collection_name(
        self, collection_name: str = "data", domain_id: int = 1
    ) -> str:
        country_code = self.domain_to_country.get(domain_id, "unknown")
        if country_code == "US":
            return collection_name
        elif country_code == "unknown":
            return f"{collection_name}_unknown"
        else:
            return f"{collection_name}_{country_code.lower()}"

    def get_mongo_collection(self, collection_name: str):
        return self.mongo_db[collection_name]

    def close_connections(self):
        if self.mongo_client:
            try:
                self.mongo_client.close()
            except Exception as e:
                logging.error(f"Error closing MongoDB connection: {e}")

        if self.engine:
            try:
                self.engine.dispose(close=True)
            except Exception as e:
                logging.error(f"Error closing SQLAlchemy engine: {e}")

        # 关闭所有target engines
        for db_name, engine in self.target_engines.items():
            try:
                engine.dispose(close=True)
            except Exception as e:
                logging.error(
                    f"Error closing target SQLAlchemy engine for {db_name}: {e}"
                )

    def handle_error(self, error: Exception, item: Dict[str, Any]) -> None:
        logging.error(f"Error processing item: {error}")
        logging.error(traceback.format_exc())

    # 可以添加更多通用方法...

    def build_timestamp_query(self, start_date, end_date):
        return {
            "_timestamp": {
                "$gte": start_date.strftime("%Y-%m-%d"),
                "$lte": (end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
            }
        }

    def build_id_query(self, id_list: List[str]):
        # 不区分asin的country_code 因为collection name 决定了country_code 一个collection 只有一个country_code
        return {"_id": {"$in": id_list}}

    def build_query(self, start_date=None, end_date=None, days=None, id_list=None):
        if id_list:
            return self.build_id_query(id_list)

        if days is not None:
            today = datetime.now().date()
            start_date = today - timedelta(days=days)
            end_date = today - timedelta(days=1)

        if start_date and end_date:
            return self.build_timestamp_query(start_date, end_date)

        return {}

    @staticmethod
    def load_sp_seller_domain_list(
        engine, sp_seller_domain_list=None
    ) -> List[Tuple[str, int]]:
        if sp_seller_domain_list is None:
            # 如果没有提供sp_seller_domain_list，从数据库获取所有sellers
            query = text(
                """
                SELECT DISTINCT seller_id, domain_id
                FROM keepa_us.tb_seller_nasin_mapping
            """
            )
            with engine.connect() as conn:
                result = conn.execute(query)
                return [(row.seller_id, row.domain_id) for row in result]
        elif isinstance(sp_seller_domain_list, list):
            if all(isinstance(item, str) for item in sp_seller_domain_list):
                # 如果是简单的字符串列表，从数据库获取domain_id
                query = text(
                    """
                    SELECT DISTINCT sm.seller_id, sm.domain_id
                    FROM keepa_us.tb_seller_nasin_mapping sm
                    WHERE sm.seller_id IN :sellers
                """
                )
                with engine.connect() as conn:
                    result = conn.execute(
                        query, {"sellers": tuple(sp_seller_domain_list)}
                    )
                    return [(row.seller_id, row.domain_id) for row in result]
            elif all(
                isinstance(item, tuple) and len(item) == 2
                for item in sp_seller_domain_list
            ):
                # 如果是(seller_id, domain_id)的元组列表，直接返回
                return sp_seller_domain_list
            else:
                raise ValueError(
                    "Invalid format in sp_seller_domain_list. Expected list of strings or list of (seller_id, domain_id) tuples."
                )
        else:
            raise ValueError(
                "Invalid type for sp_seller_domain_list. Expected list or None."
            )

    @staticmethod
    def get_domain_suffix(domain_id: int) -> str:
        return BaseHandler.domain_to_suffix.get(
            domain_id, ".com"
        )  # Default to .com if not found

    def insert_data_into_mysql(self, table_name: str, data: Dict[str, Any]) -> None:
        with self.engine.connect() as connection:
            field_names = list(data.keys())
            placeholders = ", ".join([":" + field for field in field_names])
            columns = ", ".join(field_names)
            updates = ", ".join([f"{field} = VALUES({field})" for field in field_names])

            query = text(
                f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE
            {updates}
            """
            )

            try:
                connection.execute(query, data)
                logging.info(f"Data inserted successfully into {table_name}")
            except Exception as error:
                logging.error(f"Failed to insert data into {table_name}: {error}")

    @staticmethod
    def get_date_formats() -> Dict[int, Dict[str, str]]:
        # 定义不同语言的日期前缀和格式
        # 可能得到的 review_date 格式示例：
        # US: "on July 5, 2024"
        # DE: "vom 5. Juli 2024"
        # FR: "du 5 juillet 2024"
        # ES: "del 5 de julio de 2024"
        # IT: "del 5 luglio 2024"
        return {
            1: {"prefix": r"on\s", "format": "%B %d, %Y"},  # US
            3: {"prefix": r"vom\s", "format": "%d. %B %Y"},  # DE
            4: {"prefix": r"du\s", "format": "%d %B %Y"},  # FR
            5: {"prefix": r"del\s", "format": "%d de %B de %Y"},  # ES
            6: {"prefix": r"del\s", "format": "%d %B %Y"},  # IT
            2: {"prefix": r"le\s", "format": "%d %B %Y"},  # UK
            9: {"prefix": r"del\s", "format": "%d de %B de %Y"},  # ES
            10: {"prefix": r"के\s", "format": "%d %B %Y"},  # IN
            # 可以根据需要添加更多国家/语言
        }

    def format_date(self, review_date, domain_id):
        if not review_date:
            return None

        # 获取当前域名对应的日期格式信息
        date_formats = self.get_date_formats()
        date_info = date_formats.get(domain_id, date_formats[1])  # 默认使用美国格式

        try:
            # 尝试移除前缀
            if re.search(date_info["prefix"], review_date, re.IGNORECASE):
                date_part = re.split(
                    date_info["prefix"], review_date, flags=re.IGNORECASE
                )[-1].strip()
            else:
                date_part = review_date

            # 使用 dateutil 解析日期
            parsed_date = parser.parse(date_part, fuzzy=True, dayfirst=(domain_id != 1))
            return parsed_date.strftime("%Y-%m-%d")
        except Exception as e:
            logging.warning(
                f"Error parsing date '{review_date}' for domain {domain_id}: {str(e)}"
            )
            return None

    def preprocess_field_value(
        self, field_name: str, value: Any, field_type: Type
    ) -> Any:
        """
        根据字段类型预处理字段值

        Args:
            field_name: 字段名
            value: 字段值
            field_type: SQLAlchemy模型中定义的字段类型

        Returns:
            处理后的字段值
        """
        if value is None:
            return None

        try:
            # 获取字段的Python类型
            python_type = field_type.python_type

            # 处理列表类型 (根据字段类型是否为str来判断是否需要转换)
            if isinstance(value, list) and issubclass(python_type, str):
                return "||".join(str(item) for item in value if item is not None)

        except Exception as e:
            logging.warning(f"Error preprocessing field {field_name}: {str(e)}")
            return None

        return value

    @contextlib.contextmanager
    def get_target_connection(self, database: str):
        """
        获取指定数据库名称的target engine连接

        Args:
            database: 数据库名称

        Raises:
            ValueError: 如果指定的数据库名称不存在
        """
        if database not in self.target_engines:
            raise ValueError(f"Database {database} not found in target engines")

        connection = self.target_engines[database].connect()
        try:
            yield connection
        except Exception as e:
            logging.error(f"Error getting target connection for {database}: {e}")
            if connection.in_transaction():
                connection.rollback()
            raise
        finally:
            connection.close()

    @classmethod
    def load_mapping_data(cls, engine=None):
        """加载映射数据，优先从本地JSON文件加载，如果不存在则从数据库加载并保存到本地

        Args:
            engine: SQLAlchemy engine，当需要从数据库加载数据时使用
        """
        json_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "project_config",
            "region_country_marketplace_domain_mapping.json",
        )

        try:
            # 尝试从本地JSON文件加载
            if os.path.exists(json_path):
                with open(json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    cls.currency_to_country = data["currency_to_country"]
                    cls.marketplace_to_country = data["marketplace_to_country"]
                    cls.country_to_domain = data["country_to_domain"]
                    cls.domain_to_country = data["domain_to_country"]
                    cls.domain_to_suffix = data["domain_to_suffix"]
                    cls.domain_to_language = data["domain_to_language"]
                    logging.info(
                        "Successfully loaded mapping data from local JSON file"
                    )
                    return
        except Exception as e:
            logging.error(f"Error loading from JSON: {str(e)}")

        # 如果本地文件不存在或加载失败，从数据库加载
        if not engine:
            raise ValueError("Engine is required when loading from database")

        try:
            query = """
            SELECT 
                country_code,
                marketplace_id,
                keepa_id,
                domain_suffix,
                currency_code
            FROM analyze_support_data.tb_region_country_marketplace_domain_suffix_keepa_id
            WHERE keepa_id IS NOT NULL
            """

            with engine.connect() as conn:
                result = execute_with_retry(conn, query)
                rows = result.fetchall()

            # 构建映射字典
            currency_to_country = {}
            marketplace_to_country = {}
            country_to_domain = {}
            domain_to_country = {}
            domain_to_suffix = {}
            domain_to_language = {}  # 这个可能需要手动维护，因为数据库中没有语言信息

            for row in rows:
                country_code = row.country_code
                marketplace_id = row.marketplace_id
                domain_id = row.keepa_id
                suffix = row.domain_suffix
                currency = row.currency_code

                if currency and country_code:
                    currency_to_country[currency] = country_code
                if marketplace_id and country_code:
                    marketplace_to_country[marketplace_id] = country_code
                if country_code and domain_id:
                    country_to_domain[country_code] = domain_id
                if domain_id and country_code:
                    domain_to_country[domain_id] = country_code
                if domain_id and suffix:
                    domain_to_suffix[domain_id] = suffix

            # 手动设置语言映射（因为数据库中没有）
            domain_to_language = {
                1: "en",  # US
                6: "en",  # CA
                11: "es",  # MX
                2: "en",  # UK
                3: "de",  # DE
                4: "fr",  # FR
                8: "it",  # IT
                9: "es",  # ES
                10: "hi",  # IN
                5: "ja",  # JP
            }

            # 保存到类变量
            cls.currency_to_country = currency_to_country
            cls.marketplace_to_country = marketplace_to_country
            cls.country_to_domain = country_to_domain
            cls.domain_to_country = domain_to_country
            cls.domain_to_suffix = domain_to_suffix
            cls.domain_to_language = domain_to_language

            # 保存到本地JSON文件
            mapping_data = {
                "currency_to_country": currency_to_country,
                "marketplace_to_country": marketplace_to_country,
                "country_to_domain": country_to_domain,
                "domain_to_country": domain_to_country,
                "domain_to_suffix": domain_to_suffix,
                "domain_to_language": domain_to_language,
            }

            os.makedirs(os.path.dirname(json_path), exist_ok=True)
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(mapping_data, f, indent=4, ensure_ascii=False)

            logging.info(
                "Successfully loaded mapping data from database and saved to local JSON"
            )

        except Exception as e:
            logging.error(f"Error loading from database: {str(e)}")
            raise
