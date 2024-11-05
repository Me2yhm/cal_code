import time
from pathlib import Path
from typing import Union
from loguru import logger

ROOT = Path(__file__).parent


def timeit(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        logger.info(f"{func.__name__} took {time.time() - start} seconds")
        return result

    return wrapper


def search_all_file(directory: str) -> list[Path]:
    """
    获取指定目录下的所有文件。directory: 是基于根目录的相对路径，且需要是文件夹
    """
    path = ROOT / directory
    if not path.exists() or not path.is_dir():
        print(f"目录 {directory} 不存在或不是一个目录")
        return []
    all_files = list(path.rglob("*"))  # rglob('*') 会递归查找所有文件
    return all_files


def get_date(logfile=Union[Path, str]):
    if isinstance(logfile, Path):
        name = logfile.name
    else:
        name = logfile
    return name.split("_")[-1].split(".")[0]


def others_price_better(order_price, current_price, direction):
    """判断是否被抢"""
    # 如何判断是否被抢？如果对方价格比我们劣1跳，是否还认为是被抢？有可能价格比我们劣，但比我们更快下单，并且在这之后盘口价格变化导致我们没有成交。
    if direction == "买入":
        return order_price <= current_price
    else:
        return order_price >= current_price


def get_accessed_volume(
    last_price,
    last_volume,
    order_price,
    bid1,
    ask1,
    bid1_volume,
    ask1_volume,
    direction,
):
    """计算可以成交的量"""
    match direction:
        case "买入":
            if ask1 >= order_price:
                if last_price >= order_price:
                    return 0
                return last_volume
            else:
                return last_volume + ask1_volume
        case "卖出":
            if bid1 <= order_price:
                if last_price <= order_price:
                    return 0
                return last_volume
            else:
                return last_volume + bid1_volume
        case _:
            raise ValueError(f"direction must be 买入 or 卖出, but got {direction}")
