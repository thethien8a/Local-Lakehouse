import argparse
import datetime as dt
from typing import List, Tuple

from src.bronze.utils import parse_date


def parse_bronze_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=False, help="Ngày cần xử lý (Format: YYYY-MM-DD)")
    parser.add_argument("--date_from", required=False, help="Ngày bắt đầu (Format: YYYY-MM-DD)")
    parser.add_argument("--date_to", required=False, help="Ngày kết thúc (Format: YYYY-MM-DD)")
    return parser.parse_args()


def validate_and_build_target_dates(args: argparse.Namespace) -> Tuple[List[str], str, str]:
    if args.date and (args.date_from or args.date_to):
        raise ValueError("Chỉ được dùng --date hoặc (--date_from và --date_to), không dùng đồng thời")
    if not args.date and (not args.date_from and not args.date_to):
        raise ValueError("Thiếu tham số: dùng --date hoặc dùng cả --date_from và --date_to")
    if (args.date_from and not args.date_to) or (args.date_to and not args.date_from):
        raise ValueError("Phải truyền đủ cả --date_from và --date_to")

    if args.date:
        start_date = parse_date(args.date)
        end_date = start_date
    else:
        start_date = parse_date(args.date_from)
        end_date = parse_date(args.date_to)
        if end_date < start_date:
            raise ValueError("--date_to phải lớn hơn hoặc bằng --date_from")

    target_dates: List[str] = []
    cur = start_date
    while cur <= end_date:
        target_dates.append(cur.strftime("%Y-%m-%d"))
        cur += dt.timedelta(days=1)

    app_name = f"Bronze Ingestion - {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
    return target_dates, app_name, start_date.strftime("%Y-%m-%d")
