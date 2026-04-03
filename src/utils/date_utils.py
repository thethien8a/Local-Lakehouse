import argparse
import datetime as dt
from typing import List, Tuple
from dataclasses import dataclass


@dataclass
class DateRange:
    """Kết quả parse date arguments"""
    start_date: dt.date
    end_date: dt.date
    target_dates: List[str]  # List các ngày dạng "YYYY-MM-DD"
    
    @property
    def is_single_day(self) -> bool:
        return self.start_date == self.end_date
    
    def as_filter_tuple(self) -> Tuple[str, str]:
        """Trả về (start_str, end_str) để dùng cho filter SQL/DataFrame"""
        return (
            self.start_date.strftime("%Y-%m-%d"),
            self.end_date.strftime("%Y-%m-%d")
        )


def parse_date(date_str: str) -> dt.date:
    """Parse string YYYY-MM-DD thành date object"""
    try:
        return dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError(f"Sai format ngày: {date_str}. Yêu cầu YYYY-MM-DD") from e


def create_date_parser(layer_name: str) -> argparse.ArgumentParser:
    """Tạo parser chuẩn cho mỗi layer"""
    parser = argparse.ArgumentParser(description=f"{layer_name} Ingestion")
    parser.add_argument(
        "--start", 
        required=True, 
        help="Ngày bắt đầu (YYYY-MM-DD). Bắt buộc."
    )
    parser.add_argument(
        "--end", 
        required=False, 
        help="Ngày kết thúc (YYYY-MM-DD). Mặc định = --start (single day)."
    )
    return parser


def parse_date_args(layer_name: str) -> DateRange:
    """
    Parse và validate date arguments cho bất kỳ layer nào.
    
    Usage:
        date_range = parse_date_args("Silver")
        # date_range.start_date, date_range.end_date, date_range.target_dates
    """
    parser = create_date_parser(layer_name)
    args = parser.parse_args()
    
    start_date = parse_date(args.start)
    end_date = parse_date(args.end) if args.end else start_date
    
    if end_date < start_date:
        raise ValueError("--end phải lớn hơn hoặc bằng --start")
    
    # Build list các ngày
    target_dates: List[str] = []
    cur = start_date
    while cur <= end_date:
        target_dates.append(cur.strftime("%Y-%m-%d"))
        cur += dt.timedelta(days=1)
    
    return DateRange(
        start_date=start_date,
        end_date=end_date,
        target_dates=target_dates
    )


def build_app_name(layer_name: str, date_range: DateRange) -> str:
    """Tạo app name cho Spark Session"""
    start_str = date_range.start_date.strftime("%Y-%m-%d")
    end_str = date_range.end_date.strftime("%Y-%m-%d")
    
    if date_range.is_single_day:
        return f"{layer_name} Ingestion - {start_str}"
    return f"{layer_name} Ingestion - {start_str} to {end_str}"
