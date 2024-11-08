import os
import pickle
import sys
from collections import defaultdict, deque
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial
from pathlib import PurePath as Path
from typing import Callable, Literal, Union

import numpy as np
import pandas as pd
import polars as pl
from loguru import logger
from pymongo import MongoClient

from utils import (
    get_accessed_volume,
    get_date,
    get_record_lists,
    save_to_mongo,
    search_all_file,
)
from const import (
    COLUMNS,
    COLUMN_TYPES_PD,
    COLUMN_TYPES_PL,
    MAX_SIZE,
    TAIL_SIZE,
    ROOT,
    OrderStatus,
)

logger.remove()
logger.add(sys.stdout, level="SUCCESS")


class LogParser:
    def __init__(self):
        # 结果
        self.end_time = ""
        self.tick_time = ""
        self.start_time = ""  # signal time
        self.execute_time = ""
        self.order_start_time = ""  # 委托下单
        self.snap_time = 0
        self.execute_failed_time = ""
        self.tick_2_trade = 0
        self.tick_2_signal = 0
        self.signal_2_execute = 0
        self.execute_2_order = 0
        self.order_2_send = 0
        self.delay = 0  # sent to send ok
        self.order_sys_id = 0
        self.status = OrderStatus.UKNOWN

        # 判断
        self.symbol = ""
        self.local_order_id = ""
        self.direction = ""
        self.target_future = ""
        self.failed_reason = ""
        self.order_volume = 0
        self.order_price = 0.0
        self.current_price = 0.0
        self.last_volume = 0
        self.bid_price = 0.0
        self.ask_price = 0.0
        self.bid_volume = 0
        self.ask_volume = 0
        self.execute_2_fail = 0
        self.has_order = False

    def parse_signal(self, line: str):
        """
        解析信号行
        """
        logger.info(f"解析信号行: {line}")
        data = line.split()
        start_time = data[0]
        target_future = data[4]
        self.start_time = start_time
        self.target_future = target_future

    def parse_start_execute(self, line: str):
        """解析开始执行行"""
        logger.info(f"解析开始执行行: {line}")
        self.execute_time = line.split()[0]

    def parse_order_start(self, line: str):
        """解析委托下单行"""
        self.order_start_time = line.split()[0]

    def parse_order_send(self, line: str):
        """
        解析委托发送行
        """
        logger.info(f"解析委托行: {line}")
        data = line.split()
        end_time = data[0]
        symbol = data[5]
        local_order_id = data[-1].split(":")[-1]
        insturction = data[-2]
        direction = data[-6][:2]
        order_volume = data[-3]
        order_price = data[-5]

        self.end_time = end_time
        self.symbol = symbol
        self.direction = direction
        self.order_volume = int(order_volume)
        self.order_price = float(order_price)
        self.local_order_id = local_order_id
        self.instruction = insturction

    def parse_order_filled(self, line: str):
        """
        解析下单回报行
        """
        logger.info(f"解析下单回报行: {line}")
        data = line.split()
        delay = data[-1]
        local_order_id = data[-3].split(":")[-1]
        self.delay = int(delay.strip("ns"))
        self.local_order_id = local_order_id

    def parse_order_failed(self, line: str):
        """
        解析下单失败行
        """
        logger.info(f"解析下单失败行: {line}")
        data = line.split()
        self.failed_reason = data[-1]

    def parse_order_response(
        self, line: str, kind: Literal["fast_rsp", "normal"] = "normal"
    ):
        """
        解析委托回报行
        """
        logger.info(f"解析委托回报行: {line}")
        data = line.split()
        match kind:
            case "fast_rsp":
                order_sys_id = data[-8]
                self.order_sys_id = int(order_sys_id)
            case "normal":
                _, order_sys_id = data[-4].split(":")[-1].split("/")
                self.order_sys_id = int(order_sys_id)

    def parse_quotes(self, line: str, kind: Literal["head", "tail"] = "tail"):
        """
        解析行情行
        """
        logger.info(f"解析行情行: {line}")
        data = line.split()
        bid_price = data[-11]
        bid_volume = data[-9]
        ask_price = data[-7]
        ask_volume = data[-5]
        current_price = data[-3]
        last_volume = data[-1]
        snap_time = data[5].split("/")[-1]
        tick_time = data[0]
        match kind:
            case "tail":
                logger.info(f"解析tail行情行: {line}")
                self.current_price = float(current_price)
                self.last_volume = int(last_volume)
                self.bid_price = float(bid_price)
                self.ask_price = float(ask_price)
                self.bid_volume = int(bid_volume)
                self.ask_volume = int(ask_volume)
                if snap_time.isdigit():
                    self.snap_time = int(snap_time)
                else:
                    self.snap_time = 0
            case "head":
                logger.info(f"解析head行情行: {line}")
                self.tick_time = tick_time
            case _:
                logger.warning(f"未知的kind: {kind}")

    def parse_execute_failed(self, line: str):
        """
        解析执行失败行
        """
        data = line.split()
        self.execute_failed_time = data[0]
        self.execute_2_fail = cal_tick2trade(
            self.execute_failed_time, self.execute_time
        )
        self.failed_reason = data[5]

    def parse_success_return(self, line: str):
        """
        解析成交回报行
        """
        data = line.split()
        local_order_id = data[-6].split("/")[0].split(":")[-1]
        self.local_order_id = local_order_id

    def parse_tick2trade(self):
        self.tick_2_trade = cal_tick2trade(self.end_time, self.tick_time)
        self.tick_2_signal = cal_tick2trade(self.start_time, self.tick_time)
        self.signal_2_execute = cal_tick2trade(self.execute_time, self.start_time)
        self.execute_2_order = cal_tick2trade(
            self.order_start_time, self.execute_time
        )  # 开始执行到委托下单时间
        self.order_2_send = cal_tick2trade(self.end_time, self.order_start_time)
        # send to send ok is self.delay


class ConditionSet:
    current_conditon: Callable

    def __init__(self, parser: LogParser):
        self.current_conditon = self.is_signal
        self.quote = False
        self.parser = parser
        self.parse_func = self.parser.parse_signal

    def is_signal(self, line: str):
        if "反向" in line or "正向" in line:
            self.current_conditon = self.is_execute
            return True
        else:
            return False

    def is_execute(self, line: str):
        # future_target = self.parser.target_future
        # if not future_target or future_target not in line:
        #     logger.warning(f"未找到目标合约: {line}")
        #     return False
        if "开始执行" in line:
            self.current_conditon = self.is_order_start
            self.parse_func = self.parser.parse_start_execute
            return True
        elif (
            "不允许开仓" in line
            or "对手盘挂单量不足" in line
            or "可用资金比例不足" in line
            or "放弃这个机会" in line
            or "开仓价差 <=0" in line
        ):
            logger.info(f"委托被拒绝: {line}")
            self.current_conditon = self.stop
            self.parse_func = self.parser.parse_execute_failed
            self.parser.status = OrderStatus.DENIED
        else:
            return False

    def is_order_start(self, line: str):
        if "委托下单" in line:
            self.current_conditon = self.is_order_send
            self.parse_func = self.parser.parse_order_start
            return True
        else:
            return False

    def is_order_send(self, line: str):
        if "下单发送" in line:
            self.current_conditon = self.is_order_filled
            self.parse_func = self.parser.parse_order_send
            if self.is_FOK(line):
                self.fok = True
            elif self.is_limit_order(line):
                self.limit = True
            return True
        else:
            return False

    def is_order_filled(self, line: str):
        local_order_id = f"委托号:{self.parser.local_order_id}"
        if "下单成功" in line and local_order_id in line:
            self.current_conditon = self.is_order_response
            self.parse_func = self.parser.parse_order_filled
            self.quote = True
            return True
        elif "下单失败" in line:
            self.current_conditon = self.stop
            self.parser.status = OrderStatus.ORDER_FAILED
            self.parse_func = self.parser.parse_order_failed
            return True
        else:
            return False

    def is_quote(self, line: str):
        symbol = self.parser.symbol
        if "行情" in line and symbol in line:
            return True
        else:
            return False

    # 下单回报条件判断
    def is_order_response(self, line: str):
        self.parser.has_order = True
        symbol = self.parser.symbol
        if not symbol or symbol not in line:
            return False
        if self.is_fast_rsp(line):
            return self.fast_status(line)
        elif self.is_normal_rsp(line):
            return self.nomal_status(line)
        else:
            return False

    def fast_status(self, line: str):
        if self.is_traded(line):
            self.current_conditon = self.stop
            self.parser.status = OrderStatus.SUCCEEDED
            self.parse_func = partial(self.parser.parse_order_response, kind="fast_rsp")
            return True
        elif self.is_failed(line):
            self.current_conditon = self.stop
            self.parser.status = OrderStatus.UKNOWN
            self.parse_func = partial(self.parser.parse_order_response, kind="fast_rsp")
            return True
        else:
            return False

    def nomal_status(self, line: str):
        if self.is_execute_success(line):
            self.current_conditon = self.stop
            self.parser.status = OrderStatus.SUCCEEDED
            self.parse_func = partial(self.parser.parse_order_response, kind="normal")
            return True
        elif self.is_execute_failed(line):
            self.current_conditon = self.stop
            self.parse_func = partial(self.parser.parse_order_response, kind="normal")
            return True
        elif self.is_execute_refused(line):
            self.current_conditon = self.check_refused_reason
            self.parse_func = self.skip
            return True
        else:
            return False

    def stop(self, line: str):
        pass

    def skip(self, line: str):
        pass

    # FOK指令
    def is_FOK(self, line: str):
        return "FOK" in line

    ## fast_rsp
    def is_fast_rsp(self, line: str):
        return "fast_rsp" in line

    ### 全部成交
    def is_traded(self, line: str):
        return "状态48" in line

    def is_success(self, line: str):
        local_order_id = (
            f"委托号:{self.parser.local_order_id}/{self.parser.order_sys_id}"
        )
        if "成交回报" in line and local_order_id in line:
            self.current_conditon = self.stop
            self.parse_func = self.parser.parse_success_return
            return True
        else:
            return False

    ### 委托取消
    def is_failed(self, line: str):
        return "状态53" in line

    ## 委托回报
    def is_normal_rsp(self, line: str):
        return "委托回报" in line

    ### 全部成交
    def is_execute_success(self, line: str):
        local_order_id = f"委托号:{self.parser.local_order_id}/"
        return "已成交" in line and local_order_id in line

    ### 委托拒绝
    def is_execute_refused(self, line: str):
        local_order_id = f"委托号:{self.parser.local_order_id}/"
        if "已拒绝" in line and local_order_id in line:
            return True

    ### 委托取消
    def is_execute_failed(self, line: str):
        return "已取消" in line

    ### 委托取消回报
    def check_refused_reason(self, line: str):
        if "委托已拒绝" in line:
            if "错误号:1" in line:
                self.current_conditon = self.is_order_response
                self.parse_func = self.skip
            else:
                self.current_conditon = self.stop
                self.parser.status = OrderStatus.DENIED
                self.parse_func = self.skip
            return True
        else:
            return False

    # 限价指令
    def is_limit_order(self, line: str):
        return "限价" in line


def parse_tail(lines: list[str], parser: LogParser, conditions):
    for line in lines:
        if conditions.current_conditon(line):
            conditions.parse_func(line)
        if conditions.quote:
            if conditions.is_quote(line):
                parser.parse_quotes(line)
                conditions.quote = False
        if conditions.current_conditon == conditions.stop and not conditions.quote:
            break
    return parser


def parse_head(lines: list[str], parser: LogParser, conditions: ConditionSet):
    conditions.current_conditon = conditions.is_signal
    parse_snap = True
    parse_signal = True
    for i in range(len(lines) - 1, -1, -1):
        line = lines[i]
        if "行情" in line and parse_snap:
            parser.parse_quotes(line, kind="head")
            parse_snap = False
        elif conditions.is_signal(line) and parse_signal:
            parser.parse_signal(line)
            parse_signal = False
    parser.parse_tick2trade()
    return parser


def parse_log(lines: list[str]):
    parser = LogParser()
    logger.info(f"开始解析{len(lines)}行日志")
    conditions = ConditionSet(parser)
    conditions.current_conditon = conditions.is_execute
    tail_lines = lines[-TAIL_SIZE:]
    logger.info(tail_lines[0])
    parser = parse_tail(tail_lines, parser, conditions)
    if (
        not parser.snap_time
        and parser.status != OrderStatus.DENIED
        and parser.status != OrderStatus.ORDER_FAILED
    ):
        raise ValueError(
            f"未解析行情，可能解析失败：result: \
{parser.snap_time,parser.order_sys_id,parser.tick_2_trade,parser.signal_2_execute,parser.order_2_send,parser.delay,parser.status}, \
local_order_id: {parser.local_order_id} "
        )
    if parser.status == OrderStatus.UKNOWN and parser.has_order:
        parser.status = check_failed_status(parser)
    head_lines = lines[:-TAIL_SIZE]
    parser = parse_head(head_lines, parser, conditions)
    return parser


def check_failed_status(parser: LogParser):
    """
    判断失败原因，针对FOK：如果当前盘口能买的加上已成交量大于等于下单量，说明被抢；否则，说明是快照问题。
    """
    last_volume = parser.last_volume
    order_volume = parser.order_volume
    order_price = parser.order_price
    current_price = parser.current_price
    direction = parser.direction
    bid1 = parser.bid_price
    ask1 = parser.ask_price
    bid1_volume = parser.bid_volume
    ask1_volume = parser.ask_volume
    accessed_volume = get_accessed_volume(
        current_price,
        last_volume,
        order_price,
        bid1,
        ask1,
        bid1_volume,
        ask1_volume,
        direction,
    )
    if (accessed_volume) >= order_volume:
        # 用当前成交量和下单量作比较来判断是否被抢，而不是和0比较
        return OrderStatus.FAILED_NOT_FAST_ENOUGH
    return OrderStatus.FAILED_FALSE_SIGNAL


def cal_tick2trade(end_time: str, start_time: str):
    """
    计算下单到成交的时间
    """
    end = end_time.split(".")
    start = start_time.split(".")
    logger.info(f"end: {end}, start: {start}")
    end_ns = int(end[0][-2:] + end[-1])
    start_ns = int(start[0][-2:] + start[-1])
    diff = end_ns - start_ns
    if diff < 0:
        diff += 1000000000
    return diff


def single_parse(
    log_lines: dict[int, list[str]],
    date: str,
    investor_id: str,
    orient: Literal["row", "column"],
):
    results = np.zeros((2000, len(COLUMNS)), dtype="<U25")
    results[:, 0] = date
    results[:, 1] = investor_id
    row_ind = 0
    all_success = True
    for index, lines in log_lines.items():
        parser = None
        result = None
        try:
            parser = parse_log(lines)
            result = (
                parser.snap_time,
                parser.symbol,
                parser.order_sys_id,
                parser.tick_2_trade,
                parser.tick_2_signal,
                parser.signal_2_execute,
                parser.execute_2_order,
                parser.order_2_send,
                parser.delay,
                parser.status,
            )
            assert parser.order_sys_id > 0, "未解析出order_sys_id,可能是缺少委托回报"
            if (
                result[-1] != OrderStatus.DENIED
                and result[-1] != OrderStatus.ORDER_FAILED
            ):
                results[row_ind, 2:] = result
                row_ind += 1
            else:
                logger.info(
                    f"{date}_{investor_id} 执行失败,原因为: {parser.failed_reason}"
                )
        except Exception as e:
            logger.error(f"{date}_{investor_id}解析失败: [{e.__class__}] {e}: {result}")
            all_success = False
            continue
    match orient:
        case "row":
            df = pd.DataFrame(results[:row_ind], columns=COLUMNS).astype(
                dict(COLUMN_TYPES_PD)
            )
        case "column":
            df = pl.DataFrame(results[:row_ind], schema=COLUMN_TYPES_PL)
    return df, all_success


def get_parse_data(logfile: str, pickle_file: str = "", if_pickle: bool = False):
    original_lines = deque(maxlen=MAX_SIZE)
    matched_lines: dict[int, list[str]] = defaultdict(list)  # {匹配号：[匹配行]}
    with open(logfile, "r", encoding="utf-8") as f:
        lines = f.readlines()
        index = 0
        matched = defaultdict(int)
        for line in lines:
            original_lines.append(line)
            if original_lines.__len__() == MAX_SIZE:
                original_lines.popleft()
            if "开始执行" in line:
                matched[index] = 1
                matched_lines[index] = original_lines.copy()
                index += 1
            for ind in matched:
                if matched[ind] > 0:
                    if matched[ind] == TAIL_SIZE:
                        matched[ind] = 0
                        matched_lines[ind] = list(matched_lines[ind])
                        continue
                    matched[ind] += 1
                    matched_lines[ind].append(line)

    if if_pickle:
        with open(pickle_file, "wb") as f:
            pickle.dump(matched_lines, f)
    return matched_lines


def get_matched_lines(
    logfile: str,
    date: str,
    investor_id: str,
    if_pickle: bool = False,
):
    pickle_file = ROOT / f"./cache/{investor_id}/{date}_{investor_id}.pkl"
    if pickle_file.exists():
        with open(pickle_file, "rb") as f:
            matched_lines = pickle.load(f)
    else:
        if not pickle_file.parent.exists():
            pickle_file.parent.mkdir(parents=True, exist_ok=True)
        matched_lines = get_parse_data(logfile, pickle_file, if_pickle)

    return matched_lines


def parse_one_logfile(
    logfile: Union[Path, str],
    collection: MongoClient,
    investor_id: str = "123456",
    if_pickle: bool = False,
    to_mongo: bool = False,
    orient: Literal["row", "column"] = "row",
):
    date = get_date(logfile)
    logger.remove()
    logger.add(sys.stdout, level="SUCCESS")
    logger.add(ROOT / f"logs/{investor_id}/{date}_{investor_id}.log", level="INFO")
    matched_lines = get_matched_lines(logfile, date, investor_id, if_pickle)
    results, all_success = single_parse(matched_lines, date, investor_id, orient)
    if to_mongo:
        save_to_mongo(date, investor_id, results, collection, orient)
    return results, all_success


def run(
    directory: Union[str, Path],
    if_pickle: bool = False,
    to_mongo: bool = False,
    orient: Literal["row", "column"] = "row",
):
    if isinstance(directory, str):
        directory = Path(directory)
    investor_id = directory.name
    success_path = ROOT / f"cache/{investor_id}/{investor_id}_success_files.pkl"
    failed_path = ROOT / f"cache/{investor_id}/{investor_id}failed_files.pkl"
    success_files, failed_files = get_record_lists(success_path, failed_path)
    collection = MongoClient(os.getenv("MONGODB_URL")).Quote.Tick2Trade
    for file in search_all_file(directory):
        if file.is_file():
            try:
                logger.info(f"开始解析{file.name}")
                if file in success_files:
                    logger.warning(f"{file.name}已解析过")
                    continue
                _, all_success = parse_one_logfile(
                    file,
                    investor_id=investor_id,
                    if_pickle=if_pickle,
                    to_mongo=to_mongo,
                    orient=orient,
                    collection=collection,
                )
                assert all_success, f"{investor_id}/{file.name}未全部解析成功"
                success_files.append(file)
                logger.success(f"{investor_id}_{file.name}全部解析成功")
            except Exception as e:
                logger.error(f"解析失败:[{e.__class__}] {e} {investor_id}/{file.name}")
                failed_files.append(file)
                continue
    logger.success(
        f"成功解析{len(success_files)}个日志文件，解析过的文件被保存在{success_path}中"
    )
    logger.error(
        f"解析失败{len(failed_files)}个日志文件，解析失败的文件被保存在{failed_path}中"
    )
    logger.error(f"解析失败的文件: {failed_files}")
    with open(success_path, "wb") as f:
        pickle.dump(success_files, f)
    with open(failed_path, "wb") as f:
        pickle.dump(failed_files, f)


def main(
    logs_dir: str,
    if_pickle=True,
    to_mongo=True,
    orient: Literal["row", "column"] = "row",
):
    logs_path = ROOT / logs_dir
    for investor_id in logs_path.iterdir():
        if investor_id.is_dir():
            logger.info(f"开始解析{investor_id.name}")
            run(investor_id, if_pickle, to_mongo, orient)
            logger.success(f"{investor_id.name}解析完成")


def multi_main(
    logs_dir: str,
    if_pickle=True,
    to_mongo=True,
    orient: Literal["row", "column"] = "row",
):
    logs_path = ROOT / logs_dir
    futures = {}
    with ProcessPoolExecutor() as executor:
        for investor_id in logs_path.iterdir():
            if investor_id.is_dir():
                logger.info(f"开始解析{investor_id.name}")
                future = executor.submit(run, investor_id, if_pickle, to_mongo, orient)
                futures[future] = investor_id.name
        for future in as_completed(futures):
            try:
                future.result()
                logger.success(f"{investor_id}解析完成")
            except Exception as e:
                logger.error(f"{investor_id}解析失败: {e}")


if __name__ == "__main__":
    multi_main("vola", if_pickle=True, to_mongo=True, orient="row")
