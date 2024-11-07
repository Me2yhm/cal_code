import pickle
import time
from pathlib import Path
from typing import Union

from loguru import logger
from pymongo import MongoClient, UpdateOne

from const import COLLECTION, COLUMNS

ROOT = Path(__file__).parent


def timeit(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        logger.info(f"{func.__name__} took {time.time() - start} seconds")
        return result

    return wrapper


def search_all_file(directory: Union[str, Path]) -> list[Path]:
    """
    获取指定目录下的所有文件。directory: 是绝对路径，且需要是文件夹
    """
    if isinstance(directory, str):
        path = Path(directory)
    path = directory
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
    return int(name.split("_")[-1].split(".")[0])


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


def save_to_mongo(date, investor_id, results, collection: MongoClient, orient="row"):
    id = f"{date}_{investor_id}"
    match orient:
        case "row":
            results_dic = results.to_dict(orient="records")
            save_as_row(id, results_dic, collection)
        case "column":
            results_dic = results.to_dict(as_series=False)
            record = {"_id": id, **results_dic}
            collection.update_many(
                {"_id": id},
                {"$set": record},
                upsert=True,
            )
        case _:
            raise ValueError(f"orient must be row or column, but got {orient}")


def save_as_row(id: str, results_dic: list[dict], collection: MongoClient):
    operations = []
    for row in results_dic:
        filter_condition = {
            "date": row["date"],
            "investor_id": row["investor_id"],
            "symbol": row["symbol"],
            "snap_time": row["snap_time"],
        }
        operations.append(
            UpdateOne(
                filter_condition,  # 查找条件，确保唯一性
                {"$setOnInsert": row},  # 如果不存在则插入
                upsert=True,
            )
        )

    # 执行批量操作
    if operations:
        result = collection.bulk_write(operations)
        logger.info(
            f"Inserted: {result.upserted_count} records. Matched: {result.matched_count} records."
        )


def get_record_lists(success_path: Path, failed_path: Path):
    if success_path.exists():
        with open(success_path, "rb") as f:
            success_files = pickle.load(f)
    else:
        success_files = []
    if failed_path.exists():
        failed_path.parent.mkdir(parents=True, exist_ok=True)
        with open(failed_path, "rb") as f:
            failed_files = pickle.load(f)
    else:
        failed_path.parent.mkdir(parents=True, exist_ok=True)
        failed_files = []
    return success_files, failed_files


if __name__ == "__main__":
    COLLECTION.delete_many({})
