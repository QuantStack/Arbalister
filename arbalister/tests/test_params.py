import dataclasses
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import arbalister.params as params


@dataclasses.dataclass
class SimpleExample:
    """A simple test dataclass."""

    count: int
    label: str
    flag: bool


@dataclasses.dataclass
class OptionalExample:
    """A test dataclass with optional."""

    value: int | None
    text: Optional[str]
    flag: Optional[bool]


@dataclasses.dataclass(frozen=True, slots=True)
class UnionExample:
    """A test dataclass with union."""

    data: Union[int, str]
    number_or_float: int | float


@dataclasses.dataclass
class DefaultExample:
    """A test dataclass with default."""

    count: int = 3
    name: str = "default"
    enabled: bool = True
    generated: str = dataclasses.field(default_factory=lambda: "factory")


def _callback_from_mapping(responses: Dict[str, Any]) -> Callable[[str, Any], Any]:
    def _cb(name: str, default: Any) -> Any:
        return responses.get(name, default)

    return _cb


def test_simple_fields() -> None:
    """Simple builder."""
    responses: Dict[str, Any] = {"count": "42", "label": 99, "flag": "yes"}
    result = params.build_dataclass(SimpleExample, _callback_from_mapping(responses))
    assert result == SimpleExample(count=42, label="99", flag=True)


def test_optional_fields_with_none() -> None:
    """None parsed as optional."""
    responses: Dict[str, Any] = {"value": "None", "text": None, "flag": "off"}
    result = params.build_dataclass(OptionalExample, _callback_from_mapping(responses))
    assert result == OptionalExample(value=None, text=None, flag=False)


def test_optional_fields_with_values() -> None:
    """Values parsed as optional."""
    responses: Dict[str, Any] = {"value": "7", "text": "hello", "flag": True}
    result = params.build_dataclass(OptionalExample, _callback_from_mapping(responses))
    assert result == OptionalExample(value=7, text="hello", flag=True)


def test_union_fields_int_branch() -> None:
    """Union are parsed."""
    responses: Dict[str, Any] = {"data": "21", "number_or_float": "8"}
    result = params.build_dataclass(UnionExample, _callback_from_mapping(responses))
    assert result == UnionExample(data=21, number_or_float=8)


def test_union_fields_str_and_float_branch() -> None:
    """Union are parsed."""
    responses: Dict[str, Any] = {"data": "abc", "number_or_float": "3.14"}
    result = params.build_dataclass(UnionExample, _callback_from_mapping(responses))
    assert result == UnionExample(data="abc", number_or_float=3.14)


def test_defaults_passed_to_callback() -> None:
    """Default are used in absence of value."""
    seen: List[Tuple[str, Any]] = []

    def _cb(name: str, default: Any) -> Any:
        seen.append((name, default))
        return default

    result = params.build_dataclass(DefaultExample, _cb)
    assert result == DefaultExample()
    assert seen == [
        ("count", 3),
        ("name", "default"),
        ("enabled", True),
        ("generated", "factory"),
    ]


def test_overriding_defaults() -> None:
    """Default are ignored in when a value is provided."""
    responses: Dict[str, Any] = {"count": "10", "name": "custom", "generated": "supplied"}
    result = params.build_dataclass(DefaultExample, _callback_from_mapping(responses))
    assert result == DefaultExample(count=10, name="custom", enabled=True, generated="supplied")


def test_bool_parsing_variants() -> None:
    """Truthy values are parsed as bool."""
    responses: Dict[str, Any] = {"count": "1", "label": "x", "flag": "FALSE"}
    result = params.build_dataclass(SimpleExample, _callback_from_mapping(responses))
    assert result == SimpleExample(count=1, label="x", flag=False)


def test_float_parsing_in_union() -> None:
    """Float is a better match than int in union."""
    responses: Dict[str, Any] = {"data": "5.0", "number_or_float": 2.5}
    result = params.build_dataclass(UnionExample, _callback_from_mapping(responses))
    assert result == UnionExample(data="5.0", number_or_float=2.5)


@dataclasses.dataclass
class Address:
    """A nested dataclass."""

    street: str
    city: str
    zip_code: int


@dataclasses.dataclass
class Person:
    """A dataclass with nested dataclass field."""

    name: str
    age: int
    address: Address


def test_nested_dataclass_from_dict() -> None:
    """Nested dataclass parsed from dict."""
    responses: Dict[str, Any] = {
        "name": "Alice",
        "age": "30",
        "address": {"street": "123 Main St", "city": "Springfield", "zip_code": "12345"},
    }
    result = params.build_dataclass(Person, _callback_from_mapping(responses))
    assert result == Person(
        name="Alice", age=30, address=Address(street="123 Main St", city="Springfield", zip_code=12345)
    )


def test_nested_dataclass_from_json_string() -> None:
    """Nested dataclass parsed from JSON string."""
    responses: Dict[str, Any] = {
        "name": "Bob",
        "age": "25",
        "address": '{"street": "456 Oak Ave", "city": "Shelbyville", "zip_code": "67890"}',
    }
    result = params.build_dataclass(Person, _callback_from_mapping(responses))
    assert result == Person(
        name="Bob", age=25, address=Address(street="456 Oak Ave", city="Shelbyville", zip_code=67890)
    )


def test_nested_dataclass_already_instance() -> None:
    """Nested dataclass already an instance."""
    addr = Address(street="789 Pine Rd", city="Capital City", zip_code=11111)
    responses: Dict[str, Any] = {"name": "Charlie", "age": "35", "address": addr}
    result = params.build_dataclass(Person, _callback_from_mapping(responses))
    assert result == Person(name="Charlie", age=35, address=addr)
