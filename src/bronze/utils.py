import datetime as dt

def parse_date(date_str: str) -> dt.date:
    try:
        return dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError(f"Sai format ngày: {date_str}. Yêu cầu YYYY-MM-DD") from e