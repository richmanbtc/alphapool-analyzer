import os
from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class Settings:
    mode: str
    frequency: str
    retention: pd.Timedelta
    advance: pd.Timedelta
    overlap: pd.Timedelta


CRYPTO = Settings('crypto', '5min', pd.Timedelta(days=7), pd.Timedelta(days=1), pd.Timedelta(days=1))
STOCK = Settings('stock', '1D', pd.Timedelta(days=56), pd.Timedelta(days=7), pd.Timedelta(days=7))
POSITION_TTL = pd.Timedelta(days=1)
CRYPTO_INTERVAL = pd.Timedelta(CRYPTO.frequency)


def get_settings(stock):
    return STOCK if stock else CRYPTO


ENV_NAMES = {
    'project_id': 'GC_PROJECT_ID',
    'market_dataset': 'ALPHAPOOL_DATASET',
    'output_dataset': 'ALPHAPOOL_ANALYZER_DATASET',
    'database_url': 'ALPHAPOOL_DATABASE_URL',
    'log_level': 'ALPHAPOOL_LOG_LEVEL',
}


def get_config_value(name):
    return os.getenv(ENV_NAMES[name])
