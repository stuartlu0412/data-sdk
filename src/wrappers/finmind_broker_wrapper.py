import os
import sys

import pandas as pd
from FinMind.data import DataLoader


class FinMindWrapper:
    _api = None
    _ref_count = 0

    def __init__(self):
        if FinMindWrapper._api is None:
            token = os.environ.get("FINMIND_API_TOKEN")
            if not token:
                print("Warning: FINMIND_API_TOKEN not set. Download may fail.", file=sys.stderr)

            FinMindWrapper._api = DataLoader()
            if token:
                FinMindWrapper._api.login_by_token(api_token=token)
        FinMindWrapper._ref_count += 1

    def __del__(self):
        FinMindWrapper._ref_count -= 1
        if FinMindWrapper._ref_count == 0:
            FinMindWrapper._api = None

    # ------------------------------------------------------------------
    # Paths
    # ------------------------------------------------------------------

    @staticmethod
    def broker_dir():
        output_dir = os.environ.get("DATA_SDK_FINMIND_BROKER_PATH")
        if not output_dir:
            print(
                "Warning: DATA_SDK_FINMIND_BROKER_PATH not set. Using current directory.",
                file=sys.stderr,
            )
            output_dir = "."
        return output_dir

    @staticmethod
    def broker_day_path(day, output_dir=None):
        return os.path.join(output_dir or FinMindWrapper.broker_dir(), f"{day}.parquet")

    # ------------------------------------------------------------------
    # Read surface -- pure. Never downloads, never writes.
    # ------------------------------------------------------------------

    def get_broker(self, day, sid):
        """Broker rows for ``(day, sid)`` from the archive.

        Reads never download: the archive is filled by the scheduled writer,
        and a missing day raises instead of costing a whole-day fetch here.
        """
        path = self.broker_day_path(day)
        self._require_day_file(day, path)
        out = pd.read_parquet(path, filters=[("stock_id", "==", str(sid))])
        return out.reset_index(drop=True)

    def get_broker_day(self, day, sids=None, columns=None):
        """Whole-day broker rows, optionally projected and filtered."""
        path = self.broker_day_path(day)
        self._require_day_file(day, path)
        filters = None
        if sids is not None:
            filters = [("stock_id", "in", [str(s) for s in sids])]
        out = pd.read_parquet(
            path, columns=list(columns) if columns else None, filters=filters
        )
        return out.reset_index(drop=True)

    def archived_stock_ids(self, day, output_dir=None):
        """Distinct stock ids already stored for ``day``; empty set if absent."""
        path = self.broker_day_path(day, output_dir)
        if not os.path.isfile(path) or os.path.getsize(path) == 0:
            return set()
        frame = pd.read_parquet(path, columns=["stock_id"])
        return set(frame["stock_id"].astype(str))

    @staticmethod
    def _require_day_file(day, path):
        if not os.path.isfile(path):
            raise FileNotFoundError(f"FinMind broker file not found: {path} (day {day})")
        if os.path.getsize(path) == 0:
            raise FileNotFoundError(f"FinMind broker file is empty: {path} (day {day})")

    def get_trading_dates(self, start_date, end_date):
        """Taiwan trading calendar between the two dates, inclusive.

        Thin accessor over FinMind's ``taiwan_stock_trading_date`` dataset so
        consumers never reach the underlying DataLoader directly. Returns the
        DataLoader's DataFrame (a ``date`` column of ``YYYY-MM-DD`` strings).
        """
        return FinMindWrapper._api.taiwan_stock_trading_date(
            start_date=start_date, end_date=end_date
        )
