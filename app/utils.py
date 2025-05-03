import re

pattern = re.compile(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
def camel_to_snake(a_str):
    return pattern.sub('_', a_str).lower()

def get_block_time_by_chain_id(chain_id):
    block_times = {
        # Mainnets
        1: 12.0,      # Ethereum Mainnet
        10: 2.0,      # Optimism
        42161: 0.25,  # Arbitrum One
        534352: 3.0,  # Scroll
        901: 2.0,     # Derive
        7560: 2.0,    # Cyber Mainnet
        8453: 2.0,    # Base Mainnet
        59144: 2.0,   # Linea Mainnet
        
        # Testnets
        11155111: 12.0, # Sepolia Testnet
        421614: 0.25,   # Arbitrum Sepolia
        957: 2.0,       # Derive Testnet
        111557560: 2.0, # Cyber Testnet
        59141: 2.0,     # Linea Testnet
    }
    
    if chain_id not in block_times:
        raise ValueError(f"Block time for chain:{chain_id} not specified")
    
    return block_times[chain_id]