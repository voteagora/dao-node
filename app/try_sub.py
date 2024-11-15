import asyncio
from web3 import AsyncWeb3, WebSocketProvider
from eth_abi.abi import decode

OP_ADDRESS = "0x4200000000000000000000000000000000000042"


async def subscribe_to_transfer_events():
    async with AsyncWeb3(WebSocketProvider('ws://localhost:8545')) as w3:
        transfer_event_topic = w3.keccak(text="Transfer(address,address,uint256)")
        filter_params = {
            "address": OP_ADDRESS,
            "topics": ['0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'],
        }
        print(filter_params)
        subscription_id = await w3.eth.subscribe("logs", filter_params)
        print(f"Subscribing to transfer events for WETH at {subscription_id}")

        async for payload in w3.socket.process_subscriptions():
            result = payload["result"]

            from_addr = decode(["address"], result["topics"][1])[0]
            to_addr = decode(["address"], result["topics"][2])[0]
            amount = decode(["uint256"], result["data"])[0]
            print(f"{w3.from_wei(amount, 'ether')} WETH from {from_addr} to {to_addr}")

asyncio.run(subscribe_to_transfer_events())