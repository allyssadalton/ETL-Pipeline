from datetime import datetime


def required_field(value, field_name):
    if value is None or value == "":
        return False, f"Missing required field: {field_name}"
    return True, None


def is_number(value, field_name):
    try:
        float(value)
        return True, None
    except (TypeError, ValueError):
        return False, f"Invalid number for field: {field_name}"


def non_negative(value, field_name):
    try:
        if float(value) < 0:
            return False, f"Negative value not allowed for field: {field_name}"
        return True, None
    except (TypeError, ValueError):
        return False, f"Invalid number for field: {field_name}"


def allowed_values(value, field_name, allowed):
    if value not in allowed:
        return False, f"Invalid value '{value}' for field: {field_name}"
    return True, None


def valid_date(value, field_name, date_formats):
    for fmt in date_formats:
        try:
            datetime.strptime(value, fmt)
            return True, None
        except ValueError:
            continue
    return False, f"Invalid date format for field: {field_name}"
