import logging
import os
import re
import shutil


def extract_partition_items(partitioned_path: str, return_type: int = 0) -> list[str]:
    """
    从分区路径中提取分区字段或分区值。

    :param partitioned_path: 包含分区信息的路径字符串。
    :param return_type: 指定返回类型，0 返回字段名，1 返回字段值。
    :return: 根据返回类型返回分区字段名或字段值的列表。
    :raises ValueError: 当 return_type 不是 0 或 1 时抛出。
    """
    if return_type not in [0, 1]:
        raise ValueError("Invalid return_type. Use 0 for fields or 1 for values.")

    path_segments = partitioned_path.strip("/").split("/")

    return [item.split("=")[return_type] for item in path_segments if "=" in item]


def extract_ordered_partition_k_v_pairs_from_path(partitioned_path: str) -> list[dict]:
    """
    从分区路径字符串中按顺序提取分区字段，返回键值对列表。

    :param partitioned_path: 包含分区信息的 CSV 文件路径字符串。
    :return: 按路径顺序排列的字典列表，每个字典表示一个分区字段的键值对。
    """
    path_segments = partitioned_path.strip("/").split("/")
    return [
        {k: v}
        for segment in path_segments
        if "=" in segment
        for k, v in [segment.split("=")]
    ]


def get_a_table_all_file_by_format(table_path, required_format=".csv"):
    # 递归收集指定文件夹下，所有指定后缀的文件， 绝对路径
    all_files = []
    for root, dirs, files in os.walk(table_path):
        all_files.extend(
            os.path.join(root, file) for file in files if file.endswith(required_format)
        )

    # 对所有文件进行排序
    all_files.sort()
    return all_files


def create_config_template(config_file_path, template_file_path):
    """
    生成配置模板文件，移除所有密码值。

    :param config_file_path: 原始配置文件路径
    :param template_file_path: 生成的模板文件路径
    """
    with open(config_file_path, "r") as config_file:
        lines = config_file.readlines()

    with open(template_file_path, "w") as template_file:
        for line in lines:
            # 使用正则表达式匹配密码行
            if "password" in line:
                # 替换密码值为 'YOUR_PASSWORD_HERE'
                line = re.sub(
                    r'(\s*"password"\s*:\s*")([^"]*)(")',
                    r"\1YOUR_PASSWORD_HERE\3",
                    line,
                )
            # 写入模板文件
            template_file.write(line)


def copy_template_to_config(template_file_path, config_file_path):
    """
    如果目标配置文件不存在，则将模板文件复制为配置文件。

    :param template_file_path: 模板文件路径
    :param config_file_path: 目标配置文件路径
    """
    if not os.path.exists(config_file_path):
        shutil.copy(template_file_path, config_file_path)
        logging.info(f"Copied {template_file_path} to {config_file_path}")
    else:
        logging.info(f"{config_file_path} already exists. No action taken.")


if __name__ == "__main__":
    config_path = os.path.join("project_config", "project_config.py")  # 原始配置文件路径
    template_path = os.path.join("project_config", "config_template.py")  # 模板文件路径
    create_config_template(config_path, template_path)  # 上传git前先制作模板文件
    logging.info(f"Template created at: {template_path}")

    # template_path = os.path.join('project_config', 'config_template.py')  # 模板文件路径
    # config_path = os.path.join('project_config', 'project_config.py')  # 目标配置文件路径
    # copy_template_to_config(template_path, config_path)  # 还原
