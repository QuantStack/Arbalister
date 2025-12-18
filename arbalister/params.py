import dataclasses
from types import NoneType, UnionType
from typing import Any, Callable, Union, cast, get_args, get_origin


def _parse_bool(value: Any) -> bool:
    match value:
        case bool():
            return value
        case str():
            lower = value.strip().lower()
            if lower in {"true", "1", "yes", "y", "on"}:
                return True
            if lower in {"false", "0", "no", "n", "off"}:
                return False
        case int() | float():
            return bool(value)
    raise ValueError(f"Cannot parse bool from {value!r}")


def _convert(value: Any, target_type: type[Any]) -> Any:
    match target_type:
        case _ if target_type is bool:
            return _parse_bool(value)
        case _ if target_type is int:
            return int(value)
        case _ if target_type is float:
            return float(value)
        case _ if target_type is str:
            return str(value)
        case _:
            raise TypeError(f"Unsupported type {target_type!r}")


def _ordered_union_args(value: Any, args: tuple[type[Any], ...]) -> list[type[Any]]:
    non_none = [arg for arg in args if arg is not NoneType]
    if {int, float}.issuperset(set(non_none)) and len(non_none) == 2:
        if isinstance(value, float) and not value.is_integer():
            return [float, int]
        if isinstance(value, str) and any(ch in value.lower() for ch in (".", "e")):
            return [float, int]
    return non_none


def _parse_dataclass(value: Any, dataclass_type: type[Any]) -> Any:
    """Parse a value into a dataclass instance."""
    # If already an instance of the dataclass, return as-is
    if isinstance(value, dataclass_type):
        return value

    # If it's a dict, build the dataclass from it
    if isinstance(value, dict):
        return build_dataclass(dataclass_type, lambda name, default: value.get(name, default))

    # If it's a string, try to parse as JSON first
    if isinstance(value, str):
        import json

        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return build_dataclass(dataclass_type, lambda name, default: parsed.get(name, default))
        except (json.JSONDecodeError, ValueError):
            pass

    raise TypeError(f"Cannot convert {type(value).__name__} to {dataclass_type.__name__}")


def _parse_union(value: Any, args: tuple[type[Any], ...]) -> Any:
    has_none = any(arg is NoneType for arg in args)
    if value is None or (isinstance(value, str) and value.strip().lower() == "none"):
        if has_none:
            return None
    last_error: Exception | None = None
    for arg in _ordered_union_args(value, args):
        try:
            return _parse_value(value, arg)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
    if last_error is not None:
        raise last_error
    raise TypeError("No matching union type")


def _parse_value(value: Any, annotation: Any) -> Any:
    origin = get_origin(annotation)
    match origin:
        case _ if origin is Union or origin is UnionType:
            return _parse_union(value, get_args(annotation))
        case None:
            # Check if annotation is a dataclass type (not an instance)
            if dataclasses.is_dataclass(annotation) and isinstance(annotation, type):
                return _parse_dataclass(value, annotation)
            return _convert(value, annotation)
        case _:
            return _convert(value, origin)


def build_dataclass[T](dataclass_type: type[T], callback: Callable[[str, Any], Any]) -> T:
    """Build a dtaclass from its definition and a value-p[rovising callback."""
    values: dict[str, Any] = {}
    for field in dataclasses.fields(cast(type[Any], dataclass_type)):
        default = (
            field.default
            if field.default is not dataclasses.MISSING
            else (
                field.default_factory()
                if field.default_factory is not dataclasses.MISSING
                else dataclasses.MISSING
            )
        )
        raw = callback(field.name, default)
        values[field.name] = _parse_value(raw, field.type)
    return dataclass_type(**values)
