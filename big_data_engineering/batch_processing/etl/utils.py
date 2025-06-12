# ----------------------------------------------------------------------------------
# FILE: utils.py
# Contains utility functions for the ETL CLI.
# ----------------------------------------------------------------------------------

def expand_ranges(values, range_str, *, min_val, max_val):
    """
    Combine explicit integer values and/or a 'start-end' range string
    into a sorted list of unique integers, validating bounds.
"""
    result = set()

    if values:
        for v in values:
            if not (min_val <= v <= max_val):
                raise ValueError(f"Value {v} out of bounds [{min_val}-{max_val}]")
            result.add(v)  # Collect explicit values :contentReference[oaicite:4]{index=4}

    if range_str:
        try:
            start, end = map(int, range_str.split("-", 1))
        except Exception:
            raise ValueError(f"Invalid range format '{range_str}'. Use 'start-end'")  # Range parsing :contentReference[oaicite:5]{index=5}
        if start > end or start < min_val or end > max_val:
            raise ValueError(f"Range '{range_str}' out of bounds [{min_val}-{max_val}]")
        result.update(range(start, end + 1))  # Expand range inclusively :contentReference[oaicite:6]{index=6}

    if not result:
        raise ValueError("No values provided. Use --year/--year-range and --month/--month-range.")  # Input requirement :contentReference[oaicite:7]{index=7}

    return sorted(result)  # Return sorted list for deterministic looping :contentReference[oaicite:8]{index=8}
