import os
import re
import shutil


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
        print(f"Copied {template_file_path} to {config_file_path}")
    else:
        print(f"{config_file_path} already exists. No action taken.")


if __name__ == "__main__":
    config_path = os.path.join("config", "config.py")  # 原始配置文件路径
    template_path = os.path.join("config", "config_template.py")  # 模板文件路径
    create_config_template(config_path, template_path)  # 上传git前先制作模板文件
    print(f"Template created at: {template_path}")

    # template_path = os.path.join('config', 'config_template.py')  # 模板文件路径
    # config_path = os.path.join('config', 'config.py')  # 目标配置文件路径
    # copy_template_to_config(template_path, config_path)  # 还原
