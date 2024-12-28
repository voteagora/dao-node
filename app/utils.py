import re

pattern = re.compile(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
def camel_to_snake(a_str):
    return pattern.sub('_', a_str).lower()