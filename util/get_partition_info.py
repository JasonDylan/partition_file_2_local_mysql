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
