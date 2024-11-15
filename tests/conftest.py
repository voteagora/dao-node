import os
import pytest
from abifsm import ABI, ABISet

@pytest.fixture(scope="session")
def compound_governor_abis():
    """
    Session-scoped fixture that loads the compound governor ABIs.
    This will be instantiated only once for the entire test session.
    """
    abi_path = os.path.join('tests', 'abis', 'uni-gov.json')
    abi = ABI.from_file('gov', abi_path)
    abis = ABISet('test-abis', [abi])
    return abis
