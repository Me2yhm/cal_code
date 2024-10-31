import json
import sys
from collections import defaultdict

import pandas as pd
import redis
from loguru import logger
from matplotlib import pyplot as plt

# 连接到 Redis
r = redis.Redis(host="127.0.0.1", port=6380, db=0)
time_orderid = defaultdict(dict)
symbol_time_orderid = defaultdict(lambda: defaultdict(dict))
total_result = {}
symbol_result = defaultdict(dict)
logger.remove()
logger.add("cal_code.log", level="DEBUG", mode="w")
logger.add(sys.stderr, level="INFO")


def get_members(zset: str, red: redis.Redis, start: int = 0, end: int = -1):
    zset_key = zset
    members = red.zrange(zset_key, start, end, withscores=False)
    return members


def parse_members(members):
    for member in members:
        json_member = json.loads(member)
        yield json_member["order_id"], json_member["order_time"], json_member["symbol"]


def cal_diff(
    diff: dict[str, int], name: str, type: str = "hist", if_save: bool = False
):
    fuc = getattr(plt, type)
    diff: pd.Series = pd.Series(diff)
    quantil = diff.abs().quantile([0.8])
    quantil_diff = diff[diff.abs() < 100]
    fuc(quantil_diff.values)
    plt.title(f"{name} order_id diff")
    plt.xlabel("order_id diff(account199 - account650)")
    plt.ylabel("count")
    if if_save:
        plt.savefig(f"./images/{name}_order_id_diff.png")
    plt.show()
    logger.info(
        f"On average order_id, The 88863199 is {quantil_diff.mean()} faster 80050650 than in {name} "
    )
    logger.info(f"Calculate {quantil_diff.count()} order id diff of {name}")


def cal_symbol_diff(symbol_diff: dict[str, dict[str, int]]):
    for symbol, diff in symbol_diff.items():
        cal_diff(diff, symbol, "boxplot")


times = []
members_650 = get_members("80050650_orders_zset", r)
memb_itr_650 = parse_members(members_650)
members_199 = get_members("88863199_orders_zset", r)
memb_itr_199 = parse_members(members_199)

for order_id, order_time, symbol in memb_itr_650:
    order_id = int(order_id)
    if time_orderid[order_time].get("80050650", 0) < order_id:
        time_orderid[order_time]["80050650"] = order_id
    if symbol_time_orderid[symbol][order_time].get("80050650", 0) < order_id:
        symbol_time_orderid[symbol][order_time]["80050650"] = order_id


for order_id, order_time, symbol in memb_itr_199:
    order_id = int(order_id)
    if time_orderid[order_time].get("88863199", 0) < order_id:
        time_orderid[order_time]["88863199"] = order_id
    if symbol_time_orderid[symbol][order_time].get("88863199", 0) < order_id:
        symbol_time_orderid[symbol][order_time]["88863199"] = order_id

for time in time_orderid:
    try:
        assert (
            len(time_orderid[time].keys()) == 2
        ), f"time {time} has {len(time_orderid[time].keys())} keys"
        total_result[time] = (
            time_orderid[time]["88863199"] - time_orderid[time]["80050650"]
        )
        times.append(time)
    except AssertionError as e:
        # logger.debug(e)
        pass
for symbol in symbol_time_orderid:
    for time in symbol_time_orderid[symbol]:
        try:
            assert (
                len(symbol_time_orderid[symbol][time].keys()) == 2
            ), f"symbol {symbol} time {time} has {len(symbol_time_orderid[symbol][time].keys())} keys"
            symbol_result[symbol][time] = (
                symbol_time_orderid[symbol][time]["88863199"]
                - symbol_time_orderid[symbol][time]["80050650"]
            )
        except AssertionError as e:
            # logger.debug(e)
            pass

logger.debug("time \t 88863199 \t 80050650")
for time in times:
    logger.debug(
        f"{time} \t {time_orderid[time]['88863199']} \t {time_orderid[time]['80050650']}"
    )

logger.info(
    pd.DataFrame(
        index=total_result.keys(), data=total_result.values(), columns=["total_diff"]
    )
)

cal_diff(total_result, "total", if_save=True)
