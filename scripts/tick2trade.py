import os
import pickle
import sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial
from pathlib import Path
from typing import Literal, Union

import numpy as np
import pandas as pd
import polars as pl
from loguru import logger

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
    ROOT,
    OrderStatus,
)

logger.remove()
logger.add(sys.stdout, level="SUCCESS")


class LogParser:
    def __init__(self):
        # 结果
        self.start_time = ""
        self.end_time = ""
        self.execute_time = ""
        self.execute_failed_time = ""
        self.tick_2_trade = 0
        self.tick_2_execute = 0
        self.execute_2_send = 0
        self.order_sys_id = ""
        self.delay = ""
        self.status = OrderStatus.UKNOWN

        # 判断
        self.symbol = ""
        self.local_order_id = ""
        self.direction = ""
        self.target_future = ""
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
        self.execute_time = line.split()[0]
        self.tick_2_execute = cal_tick2trade(self.execute_time, self.start_time)

    def parse_order_send(self, line: str):
        """
        解析委托行
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
        self.tick_2_trade = cal_tick2trade(end_time, self.start_time)
        self.execute_2_send = cal_tick2trade(end_time, self.execute_time)

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
                self.order_sys_id = order_sys_id
            case "normal":
                _, order_sys_id = data[-4].split(":")[-1].split("/")
                self.order_sys_id = order_sys_id

    def parse_quotas(self, line: str):
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

        self.current_price = float(current_price)
        self.last_volume = int(last_volume)
        self.bid_price = float(bid_price)
        self.ask_price = float(ask_price)
        self.bid_volume = int(bid_volume)
        self.ask_volume = int(ask_volume)

    def parse_execute_failed(self, line: str):
        """
        解析执行失败行
        """
        data = line.split()
        self.execute_failed_time = data[0]
        self.execute_2_fail = cal_tick2trade(
            self.execute_failed_time, self.execute_time
        )

    def parse_success_return(self, line: str):
        """
        解析成交回报行
        """
        data = line.split()
        local_order_id = data[-6].split("/")[0].split(":")[-1]
        self.local_order_id = local_order_id


class ConditionSet:
    current_conditon: str

    def __init__(self, parser: LogParser):
        self.current_conditon = self.is_signal
        self.quoto = False
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
            self.current_conditon = self.is_order_send
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
            self.quoto = True
            return True
        else:
            return False

    def is_quota(self, line: str):
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
            self.parser.status = OrderStatus.SUCCESS
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
            self.parser.status = OrderStatus.SUCCESS
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


def parse_log(lines: list[str]):
    parser = LogParser()
    logger.info(f"开始解析{len(lines)}行日志")
    conditons = ConditionSet(parser)
    for line in lines:
        if conditons.current_conditon(line):
            conditons.parse_func(line)
        if conditons.quoto:
            if conditons.is_quota(line):
                parser.parse_quotas(line)
                conditons.quoto = False
        if conditons.current_conditon == conditons.stop:
            break
    if parser.status == OrderStatus.UKNOWN and parser.has_order:
        parser.status = check_failed_status(parser)
    return (
        parser.execute_time[:-6],
        parser.order_sys_id,
        parser.tick_2_trade,
        parser.tick_2_execute,
        parser.execute_2_send,
        parser.delay,
        parser.status,
    )


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
        return OrderStatus.FAILED_FOR_COMPLETION
    return OrderStatus.FAILED_FOR_SNAP


def cal_tick2trade(end_time: str, start_time: str):
    """
    计算下单到成交的时间
    """
    end = end_time.split(".")
    start = start_time.split(".")
    logger.info(f"end: {end}, start: {start}")
    end_ns = int(end[0][-2:] + end[-1])
    start_ns = int(start[0][-2:] + start[-1])
    return end_ns - start_ns


def multi_parse(log_lines: dict[int, list[str]], date: str, investor_id: str):
    results = np.zeros((500, len(COLUMNS)), dtype="<U10")
    results[:, 0] = date
    results[:, 1] = investor_id
    futures = []
    row_ind = 0
    with ProcessPoolExecutor() as executor:
        for index, lines in log_lines.items():
            futures.append(executor.submit(parse_log, lines))
        for future in as_completed(futures):
            try:
                if result := future.result():
                    if result[-1] != OrderStatus.DENIED:
                        results[row_ind, 2:] = result
                        row_ind += 1
            except Exception as e:
                logger.error(f"解析失败: {e}: {result}")
                continue
    df = pd.DataFrame(results[:row_ind], columns=COLUMNS)
    return df


def single_parse(
    log_lines: dict[int, list[str]],
    date: str,
    investor_id: str,
    orient: Literal["row", "column"],
):
    results = np.zeros((500, len(COLUMNS)), dtype="<U20")
    results[:, 0] = date
    results[:, 1] = investor_id
    row_ind = 0
    for index, lines in log_lines.items():
        result = None
        try:
            result = parse_log(lines)
            if result[-1] != OrderStatus.DENIED:
                results[row_ind, 2:] = result
                row_ind += 1
        except Exception as e:
            logger.error(f"{date}_{investor_id}解析失败: {e}: {result}")
            continue
    match orient:
        case "row":
            df = pd.DataFrame(results[:row_ind], columns=COLUMNS).astype(
                dict(COLUMN_TYPES_PD)
            )
        case "column":
            df = pl.DataFrame(results[:row_ind], schema=COLUMN_TYPES_PL)
    return df


def get_parse_data(logfile: str, pickle_file: str = "", if_pickle: bool = False):
    matched_lines: dict[int, list[str]] = defaultdict(list)  # {匹配号：[匹配行]}
    with open(logfile, "r", encoding="utf-8") as f:
        lines = f.readlines()
        index = 0
        matched = defaultdict(int)
        for line in lines:
            if "反向" in line or "正向" in line:
                matched_lines[index].append(line)
                matched[index] = 1
                index += 1
            for ind in matched:
                if matched[ind] > 0:
                    matched_lines[ind].append(line)
                    matched[ind] += 1
                    if matched[ind] == MAX_SIZE:
                        matched[ind] = 0
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
    pickle_file = ROOT / f"./cache/{date}_{investor_id}.pkl"
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
    investor_id: str = "123456",
    if_pickle: bool = False,
    to_mongo: bool = False,
    orient: Literal["row", "column"] = "column",
):
    date = get_date(logfile)
    logger.remove()
    logger.add(sys.stdout, level="SUCCESS")
    logger.add(ROOT / f"logs/{date}_{investor_id}.log", level="INFO")
    matched_lines = get_matched_lines(logfile, date, investor_id, if_pickle)
    results = single_parse(matched_lines, date, investor_id, orient)
    if to_mongo:
        save_to_mongo(date, investor_id, results, orient)
    return results


def run(
    directory: Union[str, Path],
    if_pickle: bool = False,
    to_mongo: bool = False,
    orient: Literal["row", "column"] = "row",
):
    if isinstance(directory, str):
        directory = Path(directory)
    investor_id = directory.name
    success_path = ROOT / "success_files.pickle"
    failed_path = ROOT / "failed_files.pickle"
    success_files, failed_files = get_record_lists()
    for file in search_all_file(directory):
        if file.is_file():
            try:
                logger.info(f"开始解析{file.name}")
                if file in success_files:
                    continue
                parse_one_logfile(
                    file,
                    investor_id=investor_id,
                    if_pickle=if_pickle,
                    to_mongo=to_mongo,
                    orient=orient,
                )
                success_files.append(file)
                logger.success(f"{file.name}解析成功")
            except Exception as e:
                logger.error(f"解析失败: {e} {file.name}")
                failed_files.append(file)
                continue
    logger.success(
        f"成功解析{len(success_files)}个日志文件，解析过的文件被保存在success_files.pkl中"
    )
    logger.error(
        f"解析失败{len(failed_files)}个日志文件，解析失败的文件被保存在failed_files.pkl中"
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
    logs_path = Path(logs_dir)
    for investor_id in logs_path.iterdir():
        if investor_id.is_dir():
            logger.info(f"开始解析{investor_id.name}")
            run(investor_id, if_pickle, to_mongo, orient)
            logger.success(f"{investor_id.name}解析完成")


if __name__ == "__main__":
    run("vola", if_pickle=True, to_mongo=True, orient="row")