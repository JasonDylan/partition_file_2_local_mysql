def snake_case_2_PascalCase(table_name):
    parts = table_name.split("_")
    return "".join(part.capitalize() for part in parts)


def convert_to_lowercase(input_string: str) -> str:
    """
    将输入字符串中的所有大写字母转换为小写字母。

    :param input_string: 输入字符串
    :return: 转换后的字符串
    """
    return input_string.lower()


if __name__ == "__main__":
    # 示例
    class_name = snake_case_2_PascalCase("tb_sales_estimates_weekly_v2")
    print(class_name)  # 输出: TbSalesEstimatesWeeklyV2

    # 示例使用
    to_convert_list = [
        "db_config",
    ]
    for example_string in to_convert_list:
        lowercase_string = example_string.lower()

        uppercase_string = example_string.upper()
        print(lowercase_string)  # 输出: hello, world!
        print(uppercase_string)  # 输出: hello, world!
