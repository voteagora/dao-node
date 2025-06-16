import os
import pytest
from abifsm import ABI, ABISet

@pytest.fixture(scope="session")
def compound_governor_abis():
    abi_path = os.path.join('tests', 'abis', 'uni-gov.json')
    abi = ABI.from_file('gov', abi_path)
    abis = ABISet('test-abis', [abi])
    return abis

@pytest.fixture(scope="session")
def op_governor_abis():
    abi_path = os.path.join('tests', 'abis', 'op-gov.json')
    abi = ABI.from_file('gov', abi_path)
    abis = ABISet('test-abis', [abi])
    return abis

@pytest.fixture(scope="session")
def pguild_ptc_abi():
    abi_path = os.path.join('tests', 'abis', 'pguild-ptc.json')
    abi = ABI.from_file('ptc', abi_path)
    abis = ABISet('test-abis', [abi])
    return abis

@pytest.fixture(scope="session")
def pguild_token_abi():
    abi_path = os.path.join('tests', 'abis', 'pguild-token.json')
    abi = ABI.from_file('token', abi_path)
    abis = ABISet('test-abis', [abi])
    return abis

@pytest.fixture(scope="session")
def cyber_token_abi():
    abi_path = os.path.join('tests', 'abis', 'cyber-token.json')
    abi = ABI.from_file('token', abi_path)
    abis = ABISet('test-abis', [abi])
    return abis

@pytest.fixture(scope="session")
def scroll_token_abi():
    abi_path = os.path.join('tests', 'abis', 'scroll-token.json')
    abi = ABI.from_file('token', abi_path)
    abis = ABISet('test-abis', [abi])
    return abis

@pytest.fixture(scope="session")
def v2_scope_abi():
    abi_path = os.path.join('tests', 'abis', 'world-ptc.json')
    abi = ABI.from_file('ptc', abi_path)
    abis = ABISet('test-abis', [abi])
    return abis

@pytest.fixture(scope="session") 
def v2_proposal_abi():
    gov_abi_path = os.path.join('tests', 'abis', 'world-gov.json')
    gov_abi = ABI.from_file('gov', gov_abi_path)
    
    voting_module_abi_path = os.path.join('tests', 'abis', 'world-voting_module.json')
    voting_module_abi = ABI.from_file('voting_module', voting_module_abi_path)
    
    abis = ABISet('test-abis', [gov_abi, voting_module_abi])
    return abis