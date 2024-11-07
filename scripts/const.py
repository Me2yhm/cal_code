import os
from pathlib import Path

import numpy as np
import polars as pl
from pymongo import MongoClient

COLLECTION = MongoClient(os.getenv("MONGODB_URL")).Quote.Tick2Trade
ROOT = Path(__file__).parent.parent
MAX_SIZE = 600
TAIL_SIZE = 400
COLUMNS = [
    "date",
    "investor_id",
    "snap_time",
    "symbol",
    "order_sys_id",
    "tick2trade",
    "tick2signal",
    "signal2execute",
    "execute2order",
    "order2send",
    "send2sendok",
    "status",
]
COLUMN_TYPES_PD = list(
    zip(
        COLUMNS,
        [
            np.int64,
            "<U25",
            np.int64,
            "<U25",
            np.int64,
            np.int64,
            np.int64,
            np.int64,
            np.int64,
            np.int64,
            np.int64,
            "<U25",
        ],
    )
)
COLUMN_TYPES_PL = list(
    zip(
        COLUMNS,
        [
            pl.Int64,
            pl.String,
            pl.Int64,
            pl.String,
            pl.Int64,
            pl.String,
            pl.Int64,
            pl.Int64,
            pl.Int64,
            pl.Int64,
            pl.Int64,
            pl.String,
        ],
    )
)


class OrderStatus:
    SUCCEEDED = "SUCCEEDED"
    FAILED_FALSE_SIGNAL = "FAILED_FALSE_SIGNAL"
    FAILED_NOT_FAST_ENOUGH = "FAILED_NOT_FAST_ENOUGH"
    DENIED = "DENIED"
    UKNOWN = "UNKNOWN"
    ORDER_FAILED = "ORDER_FAILED"
