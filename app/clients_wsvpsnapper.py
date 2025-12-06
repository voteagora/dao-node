import json, asyncio, websockets, random
from pprint import pprint

from sanic.log import logger as logr

class VPSnappercWsClient():

    def __init__(self, url):
        self.url = url + "/subscribe"
 
    async def read(self):
        base_delay = 1
        max_delay = 60
        attempt = 0

        while True:
            try:
                async with websockets.connect(self.url) as ws:
                    attempt = 0  # Reset backoff on successful connection

                    async for message in ws:
                        payload = json.loads(message)
                        yield payload

            except Exception as e:
                attempt += 1
                delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                logr.error(f"WebSocket error: {e}. Reconnecting in {delay:.1f}s (attempt {attempt})")
                await asyncio.sleep(delay)
                

if __name__ == "__main__":

    # client = VPSnappercWsClient('ws://localhost:8001', 'VPSnapper')
    
    async def main():
        i = 0
        async for payload in client.read():
            print(payload)

            if False:
                with open(f"./tests/data/nonivotes-syndicate/{i}-{payload['timestamp']}.json", "w") as f:
                    f.write(json.dumps(payload))

                i += 1
    asyncio.run(main())
