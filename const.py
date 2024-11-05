import os
from pathlib import Path

import numpy as np
import polars as pl
from pymongo import MongoClient

COLLECTION = MongoClient(os.getenv("MONGODB_URL")).Quote.Tick2Trade
ROOT = Path(__file__).parent
MAX_SIZE = 500
COLUMNS = [
    "date",
    "investor_id",
    "order_sys_id",
    "tick2trade",
    "tick2execute",
    "execute2send",
    "send2success",
    "status",
]
COLUMN_TYPES_PD = list(
    zip(
        COLUMNS,
        ["<U10", "<U10", "<U10", np.int64, np.int64, np.int64, np.int64, "<U20"],
    )
)
COLUMN_TYPES_PL = list(
    zip(
        COLUMNS,
        [
            pl.String,
            pl.String,
            pl.String,
            pl.Int64,
            pl.Int64,
            pl.Int64,
            pl.Int64,
            pl.String,
        ],
    )
)


class OrderStatus:
    SUCCESS = "SUCCESS"
    FAILED_FOR_SNAP = "FAILED_FOR_SNAP"
    FAILED_FOR_COMPLETION = "FAILED_FOR_COMPLETION"
    DENIED = "DENIED"
    UKNOWN = "UNKNOWN"
