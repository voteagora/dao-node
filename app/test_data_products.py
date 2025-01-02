import pytest
from data_products import Balances

def test_balances():

    balances = Balances()

    data = [
             {'block_number': 2456013, 'transaction_index': 1, 'log_index': 0,  'from': '0x0000000000000000000000000000000000000000', 'to': '0xcfbcda93bee60e1f4865783e141b1dd913d219df', 'value': 1000000000000000,   'signature': 'Transfer(address,address,uint256)', 'sighash': 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'},
             {'block_number': 2456013, 'transaction_index': 1, 'log_index': 2,  'from': '0x0000000000000000000000000000000000000000', 'to': '0xe368d397eae44f1f7f6b922877564d4e592d63b8', 'value': 1000000000000000,   'signature': 'Transfer(address,address,uint256)', 'sighash': 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'},
             {'block_number': 2456013, 'transaction_index': 1, 'log_index': 4,  'from': '0x0000000000000000000000000000000000000000', 'to': '0x465c63680f2a0b4277d9b4cecc3f3310e531a77f', 'value': 1968807210960000,   'signature': 'Transfer(address,address,uint256)', 'sighash': 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'},
             {'block_number': 2456013, 'transaction_index': 1, 'log_index': 6,  'from': '0x0000000000000000000000000000000000000000', 'to': '0xb054902bc6260f3e733b78adf736b17783158953', 'value': 3000000000000000,   'signature': 'Transfer(address,address,uint256)', 'sighash': 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'},
             {'block_number': 2456013, 'transaction_index': 1, 'log_index': 8,  'from': '0x0000000000000000000000000000000000000000', 'to': '0xb86faa020274ae3fc3a883293f041f23793f698e', 'value': 36722878065972185,  'signature': 'Transfer(address,address,uint256)', 'sighash': 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'},
             {'block_number': 2456013, 'transaction_index': 1, 'log_index': 10, 'from': '0x0000000000000000000000000000000000000000', 'to': '0xfdbf50bfc69a2d6d400ae6e4d18624a534a6980f', 'value': 82871585988544595,  'signature': 'Transfer(address,address,uint256)', 'sighash': 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'},
             {'block_number': 2456013, 'transaction_index': 1, 'log_index': 12, 'from': '0x0000000000000000000000000000000000000000', 'to': '0x0b0df332d1126851f5fb9394e4d8aaae714833cf', 'value': 100000000000000000, 'signature': 'Transfer(address,address,uint256)', 'sighash': 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'},
             {'block_number': 2456013, 'transaction_index': 1, 'log_index': 14, 'from': '0x0000000000000000000000000000000000000000', 'to': '0x16a3c50f1ec275335cf2feaf96738de54c6ae9a2', 'value': 100000000000000000, 'signature': 'Transfer(address,address,uint256)', 'sighash': 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'},
             {'block_number': 2456013, 'transaction_index': 1, 'log_index': 16, 'from': '0x0000000000000000000000000000000000000000', 'to': '0x2643c742ce701a8ab2394c0debfdac0d6cbb3010', 'value': 100000000000000000, 'signature': 'Transfer(address,address,uint256)', 'sighash': 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'},
             {'block_number': 2456014, 'transaction_index': 2, 'log_index': 1,  'from': '0x0000000000000000000000000000000000000000', 'to': '0x0b0df332d1126851f5fb9394e4d8aaae714833cf', 'value': 100000000000000000, 'signature': 'Transfer(address,address,uint256)', 'sighash': 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'}, # Fudged record for testing purposes.
             {'block_number': 2456015, 'transaction_index': 2, 'log_index': 1,  'from': '0x0b0df332d1126851f5fb9394e4d8aaae714833cf', 'to': '0x2643c742ce701a8ab2394c0debfdac0d6cbb3010', 'value': 50000000000000000,  'signature': 'Transfer(address,address,uint256)', 'sighash': 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'}   # Fudged record for testing purposes.
           ] 

    for record in data:
        balances.handle(record)

    assert balances.balance_of('0xfdbf50bfc69a2d6d400ae6e4d18624a534a6980f') == 82871585988544595
    assert balances.balance_of('0x0b0df332d1126851f5fb9394e4d8aaae714833cf') == 150000000000000000
