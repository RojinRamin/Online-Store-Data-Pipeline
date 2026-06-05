from datetime import datetime

class TransformError(Exception):
    """Raised when a raw event cannot be transformed safely."""
    pass

def is_null(value):
    return (
        value is None
        or value == ""
        or str(value).strip().lower() in ["null", "none", "nan"]
    )

def cast_str(value, field_name, required=False, default=None):
    if is_null(value):
        if required:
            raise TransformError(f"Required string field is null: {field_name}")
        return default
    return str(value).strip()

def cast_int(value, field_name, required=False, default=None):
    if is_null(value):
        if required:
            raise TransformError(f"Required int field is null: {field_name}")
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        raise TransformError(f"Cannot cast field '{field_name}' to int: {value}")

def cast_float(value, field_name, required=False, default=None):
    if is_null(value):
        if required:
            raise TransformError(f"Required float field is null: {field_name}")
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        raise TransformError(f"Cannot cast field '{field_name}' to float: {value}")

def cast_bool(value, field_name, required=False, default=None):
    if is_null(value):
        if required:
            raise TransformError(f"Required bool field is null: {field_name}")
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in ["true", "1", "yes", "y"]:
        return True
    if normalized in ["false", "0", "no", "n"]:
        return False
    raise TransformError(f"Cannot cast field '{field_name}' to bool: {value}")

def cast_datetime(value, field_name="timestamp", required=True, default=None):
    if is_null(value):
        if required:
            raise TransformError(f"Required datetime field is null: {field_name}")
        return default
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        raise TransformError(f"Cannot cast field '{field_name}' to datetime: {value}")
