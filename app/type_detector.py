"""
Ethereum-aware type detection for event field analysis.

This module inspects Python values and returns detailed type strings
that capture Ethereum-specific nuances like address casing, hex data, etc.
"""

import re
from typing import Any, List, Tuple, Union


# Ethereum address pattern - 0x followed by 40 hex characters
ADDRESS_PATTERN = re.compile(r'^0x[a-fA-F0-9]{40}$')

# Hex bytes pattern - 0x followed by even number of hex characters
HEX_BYTES_PATTERN = re.compile(r'^0x[a-fA-F0-9]*$')

# Numeric string pattern
NUMERIC_PATTERN = re.compile(r'^-?\d+$')


def is_ethereum_address(value: str) -> bool:
    """Check if a string is a valid Ethereum address."""
    return bool(ADDRESS_PATTERN.match(value))


def is_lowercase_address(value: str) -> bool:
    """Check if an Ethereum address is all lowercase (after 0x)."""
    if not is_ethereum_address(value):
        return False
    return value[2:] == value[2:].lower()


def is_checksummed_address(value: str) -> bool:
    """Check if an Ethereum address has mixed case (checksummed)."""
    if not is_ethereum_address(value):
        return False
    hex_part = value[2:]
    # Checksummed addresses have mixed case
    return hex_part != hex_part.lower() and hex_part != hex_part.upper()


def is_hex_bytes(value: str) -> bool:
    """Check if a string is hex-encoded bytes (0x prefixed)."""
    if not isinstance(value, str):
        return False
    if not value.startswith('0x'):
        return False
    # Must have even number of hex chars after 0x (or be just 0x for empty)
    hex_part = value[2:]
    if len(hex_part) % 2 != 0:
        return False
    return bool(HEX_BYTES_PATTERN.match(value))


def is_numeric_string(value: str) -> bool:
    """Check if a string represents a numeric value."""
    return bool(NUMERIC_PATTERN.match(value))


def detect_type(value: Any) -> str:
    """
    Detect the Ethereum-aware type of a value.
    
    Returns a type string like:
    - "int" for integers
    - "str:address:lowercase" for lowercase Ethereum addresses
    - "str:address:checksummed" for checksummed addresses
    - "str:bytes:hex" for hex-encoded bytes
    - "str:numeric" for numeric strings
    - "str:literal" for plain strings
    - "bytes" for raw bytes
    - "bool" for booleans
    - "none" for None
    - "list[<element_type>]" for lists
    - "tuple[<type1>,<type2>,...]" for tuples
    - "dict" for dictionaries
    """
    if value is None:
        return "none"
    
    if isinstance(value, bool):
        return "bool"
    
    if isinstance(value, int):
        return "int"
    
    if isinstance(value, bytes):
        return "bytes"
    
    if isinstance(value, str):
        return detect_string_type(value)
    
    if isinstance(value, (list, tuple)):
        return detect_sequence_type(value)
    
    if isinstance(value, dict):
        return "dict"
    
    return f"unknown:{type(value).__name__}"


def detect_string_type(value: str) -> str:
    """Detect the specific type of a string value."""
    if not value:
        return "str:empty"
    
    # Check for Ethereum address first (most specific)
    if is_ethereum_address(value):
        if is_lowercase_address(value):
            return "str:address:lowercase"
        elif is_checksummed_address(value):
            return "str:address:checksummed"
        else:
            # All uppercase (rare but possible)
            return "str:address:uppercase"
    
    # Check for hex bytes (after address check since addresses are also hex)
    if is_hex_bytes(value):
        # Distinguish between short hex (like topic hashes) and longer data
        hex_len = len(value) - 2  # exclude 0x
        if hex_len == 64:
            return "str:bytes:hex:bytes32"
        elif hex_len == 0:
            return "str:bytes:hex:empty"
        else:
            return "str:bytes:hex"
    
    # Check for numeric string
    if is_numeric_string(value):
        return "str:numeric"
    
    # Plain string literal
    return "str:literal"


def detect_sequence_type(value: Union[List, Tuple]) -> str:
    """Detect the type of a list or tuple, including element types."""
    seq_type = "list" if isinstance(value, list) else "tuple"
    
    if len(value) == 0:
        return f"{seq_type}:empty"
    
    # Detect element types
    element_types = [detect_type(elem) for elem in value]
    
    # Check if all elements have the same type
    unique_types = list(dict.fromkeys(element_types))  # preserve order, remove dupes
    
    if len(unique_types) == 1:
        return f"{seq_type}[{unique_types[0]}]"
    
    # For tuples with different types, show all types
    if seq_type == "tuple" or len(unique_types) <= 3:
        return f"{seq_type}[{','.join(unique_types)}]"
    
    # For lists with mixed types, show "mixed" with the types found
    return f"{seq_type}:mixed[{','.join(unique_types[:3])}...]"


def detect_event_schema(event: dict) -> dict:
    """
    Analyze an entire event dictionary and return a schema.
    
    Args:
        event: A dictionary representing an Ethereum event
        
    Returns:
        A dictionary mapping field names to their detected types
    """
    schema = {}
    for key, value in event.items():
        schema[key] = detect_type(value)
    return schema


def schema_to_sorted_tuple(schema: dict) -> Tuple[Tuple[str, str], ...]:
    """Convert a schema dict to a sorted tuple for hashing/comparison."""
    return tuple(sorted(schema.items()))


def schemas_match(schema1: dict, schema2: dict) -> bool:
    """Check if two schemas are identical."""
    return schema_to_sorted_tuple(schema1) == schema_to_sorted_tuple(schema2)


def schema_diff(schema1: dict, schema2: dict) -> dict:
    """
    Compare two schemas and return the differences.
    
    Returns a dict with:
    - 'only_in_first': fields only in schema1
    - 'only_in_second': fields only in schema2
    - 'type_mismatches': fields present in both but with different types
    """
    keys1 = set(schema1.keys())
    keys2 = set(schema2.keys())
    
    diff = {
        'only_in_first': {k: schema1[k] for k in keys1 - keys2},
        'only_in_second': {k: schema2[k] for k in keys2 - keys1},
        'type_mismatches': {}
    }
    
    for key in keys1 & keys2:
        if schema1[key] != schema2[key]:
            diff['type_mismatches'][key] = {
                'first': schema1[key],
                'second': schema2[key]
            }
    
    return diff


if __name__ == '__main__':
    # Quick test
    test_event = {
        'block_number': '12345678',
        'log_index': 42,
        'from': '0xabcdef1234567890abcdef1234567890abcdef12',
        'to': '0xAbCdEf1234567890AbCdEf1234567890AbCdEf12',
        'value': 1000000000000000000,
        'data': '0x1234abcd',
        'topics': ['0x' + 'a' * 64, '0x' + 'b' * 64],
        'delegatees': [('0xabcdef1234567890abcdef1234567890abcdef12', 5000)],
        'description': 'A proposal description',
        'empty_list': [],
        'raw_bytes': b'\x00\x01\x02',
    }
    
    schema = detect_event_schema(test_event)
    for field, type_str in sorted(schema.items()):
        print(f"  {field}: {type_str}")

