# partition_dir_2_mysql

s3 to mysql/s3_2_mysql
本项目用于将形如/xxx=xxx/xxx=xxx/的分区结构的csv数据结构通过mysql load file 导入mysql

.
├── config                    # 配置文件目录

│   ├── config.py            # 数据库连接配置和其他设置

│   └── config_template.py    # 配置模板，包含敏感信息的占位符

├── model                     # 数据模型文件目录

│   ├── base_model.py         # 基础模型类，定义通用的 ORM 行为

│   ├── mapping.py            # 定义分区目录与模型的映射定义

│   └── server_108           # 特定服务器的模型文件

│       └── db_junglescout_amazon_v1.py  # 处理 Amazon Jungle Scout 数据的模型

├── README.md                 # 项目说明文档，包含安装和使用指南

├── requirements.txt          # 项目依赖的 Python 包列表

├── util                      # 工具类文件目录

│   ├── file_util.py          # 文件处理工具函数

│   ├── format_converter.py    # 数据格式转换工具

│   ├── get_partition_info.py  # 获取数据分区信息的工具

│   └── sqlalchemy_orm_util.py # SQLAlchemy 相关的工具函数

└── validate_dir_2_mysql.py     # 数据验证并迁移到 MySQL
