import pytest
from utils import camel_to_snake

@pytest.mark.parametrize(
    "input_str, expected",
    [
        ("CamelCase", "camel_case"),
        ("camelCase", "camel_case"),
        ("HTTPServerError", "http_server_error"),
        ("URLValidator", "url_validator"),
        ("myVariableName", "my_variable_name"),
        ("Camel", "camel"),
        ("A", "a"),  # single letter
        ("ABC", "abc"),  # all caps
        ("abc", "abc"),  # all lowercase should remain unchanged
        ("JSONData", "json_data"),  # leading uppercase acronym followed by word
    ],
)
def test_camel_to_snake(input_str, expected):
    result = camel_to_snake(input_str)
    assert result == expected, f"Expected {expected}, but got {result}"