from warnings import warn
from pathlib import Path
import csv
import os
import sys
import json
from datetime import datetime, timedelta
import websocket, websockets
import asyncio
from eth_abi.abi import decode as decode_abi

from web3 import Web3, AsyncWeb3, WebSocketProvider
from web3.middleware import ExtraDataToPOAMiddleware
from sanic.log import logger as logr, error_logger as errlogr

from .utils import camel_to_snake

DAO_NODE_USE_POA_MIDDLEWARE = os.getenv('DAO_NODE_USE_POA_MIDDLEWARE', "false").lower() in ('true', '1')

class JsonRpcRTWsClient:
    timeliness = 'realtime'

    def __init__(self, url):
        self.url = url
        self.ws = None
        self.ws_lock = asyncio.Lock()
        self.ws_recv_lock = asyncio.Lock()  # New lock specifically for recv operations
        self.subscription_lock = asyncio.Lock()
        self.subscriptions = {}
        self.next_sub_request_id = 1
        self.message_task = None
        self._closing = False  # Flag to indicate client is shutting down
        self.response_queues = {}  # Maps request_id -> Queue for responses
        self.block_headers_queue = asyncio.Queue()  # Queue for block headers
        self.block_header_sub_id = None  # Subscription ID for block headers

    def is_valid(self):
        if self.url in ('', 'ignored', None):
            ans = False
        else:
            try:
                ws = websocket.create_connection(self.url)
                ws.close()
                ans = True
            except Exception:
                ans = False

        if ans:
            print(f"The server '{self.url}' is valid.")
        else:
            print(f"The server '{self.url}' is not valid.")
        
        return ans

    async def ensure_connection(self):
        async with self.ws_lock:
            # First check if we need to clean up old connection
            if self.ws is not None:
                
                try:
                
                    pong = await self.ws.ping()
                    await asyncio.wait_for(pong, timeout=1)
                    return  # Connection is good
                
                except Exception:
                    
                    # Connection is bad, clean it up
                    try:
                        await self.ws.close()
                    except Exception:
                        pass
                   
                    self.ws = None

                    if self.message_task and not self.message_task.done():
                        self.message_task.cancel()
                        try:
                            await self.message_task
                        except asyncio.CancelledError:
                            pass
                    self.message_task = None

            # Create new connection
            self.ws = await websockets.connect(self.url, ping_interval=5, ping_timeout=3)
            self.message_task = asyncio.create_task(self._handle_messages())
            
            # Subscribe to block headers
            await self._subscribe_to_block_headers()
            
            logr.info("New WebSocket connection established and message handler started")

    async def _handle_messages(self):
        try:
            logr.info("Message handler started")
            while not self._closing:
                try:
                    # Get the message without holding the lock
                    async with self.ws_recv_lock:
                        raw_message = await self.ws.recv()
                    message = json.loads(raw_message)
                    # Handle subscription responses
                    if "id" in message:
                        req_id = message["id"]
                        if req_id in self.response_queues:
                            await self.response_queues[req_id].put(message)
                            continue

                    # Handle subscription events
                    if message.get("method") == "eth_subscription":
                        sub_id = message["params"]["subscription"]
                        result = message["params"]["result"]

                        # Check if this is a block header subscription
                        if sub_id == self.block_header_sub_id:
                            await self.block_headers_queue.put(result)
                            continue

                        # Get subscription info under lock
                        queue = None
                        event_info = None
                        async with self.subscription_lock:
                            if sub_id in self.subscriptions:
                                queue, event_info = self.subscriptions[sub_id]
                                logr.info(f"Processing message for subscription {sub_id} (address: {event_info['address']}, topic: {event_info['topic']})")

                        if queue and event_info:
                            # Process event outside the lock
                            event = self.decode_payload(result, event_info["inputs"], event_info["signature"], event_info["topic"])
                            # logr.info(f"Decoded event: {event}")
                            await queue.put(event)
                        else:
                            logr.warning(f"Received message for unknown subscription {sub_id}")

                except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosedOK) as e:
                    if isinstance(e, websockets.exceptions.ConnectionClosedOK) and e.code == 1000:
                        logr.info("WebSocket connection closed normally")
                        # Notify all queues that connection is closed
                        async with self.subscription_lock:
                            for sub_id, (queue, _) in self.subscriptions.items():
                                await queue.put(None)
                            self.subscriptions.clear()
                        return
                    
                    logr.error(f"WebSocket connection closed ({type(e).__name__}), code={getattr(e, 'code', 'unknown')}, reason={getattr(e, 'reason', 'unknown')}")
                    # Store existing subscriptions before clearing
                    old_subscriptions = None
                    async with self.subscription_lock:
                        old_subscriptions = self.subscriptions.copy()
                        self.subscriptions.clear()
                    
                    # Try to reconnect
                    for attempt in range(3):
                        try:
                            await asyncio.sleep(1 * (attempt + 1))  # Exponential backoff
                            await self.ensure_connection()
                            # If connection successful, try to resubscribe
                            if old_subscriptions:
                                async with self.subscription_lock:
                                    self.subscriptions = old_subscriptions.copy()
                                await self._resubscribe_all()
                            break
                        except Exception as reconnect_err:
                            logr.error(f"Reconnection attempt {attempt + 1} failed: {reconnect_err}")
                    else:  # All reconnection attempts failed
                        logr.error("Failed to reconnect after 3 attempts")
                        # Notify all queues of failure
                        if old_subscriptions:
                            for sub_id, (queue, _) in old_subscriptions.items():
                                await queue.put(None)
                        # Also notify block header subscribers
                        await self.block_headers_queue.put(None)
                        return
                    
                except Exception as e:
                    logr.exception(f"Error in message handler: {e}")
                    await asyncio.sleep(1)
        finally:
            logr.info("Message handler stopping")
            # Clean up subscriptions when handler stops
            async with self.subscription_lock:
                for sub_id, (queue, _) in self.subscriptions.items():
                    await queue.put(None)
                self.subscriptions.clear()
                # Also notify block header subscribers
                await self.block_headers_queue.put(None)
            self.message_task = None

    async def _resubscribe_all(self):
        # Get copy of subscriptions under lock
        async with self.subscription_lock:
            old_subscriptions = self.subscriptions.copy()
            self.subscriptions.clear()
    
        if not old_subscriptions:
            logr.info("No subscriptions to restore")
            return

        logr.info(f"Attempting to restore {len(old_subscriptions)} subscriptions")
        
        # Resubscribe outside the lock
        for old_sub_id, (queue, event_info) in old_subscriptions.items():
            for attempt in range(3):  # Try up to 3 times per subscription
                try:
                    subscribe_params = {
                        "jsonrpc": "2.0",
                        "id": self.next_sub_request_id,
                        "method": "eth_subscribe",
                        "params": [
                            "logs",
                            {
                                "address": event_info["address"],
                                "topics": ["0x" + event_info["topic"]]
                            }
                        ]
                    }
                    self.next_sub_request_id += 1
                    
                    async with self.ws_lock:
                        logr.info(f"Resubscribing to {event_info['address']} with topic {event_info['topic']} (attempt {attempt + 1}/3)")
                        await self.ws.send(json.dumps(subscribe_params))
                        response = json.loads(await self.ws.recv())
                        
                    if "result" in response:
                        new_sub_id = response["result"]
                        async with self.subscription_lock:
                            self.subscriptions[new_sub_id] = (queue, event_info)
                        logr.info(f"Restored subscription {old_sub_id} -> {new_sub_id} for address: {event_info['address']}")
                        break  # Success, move to next subscription
                    else:
                        error = response.get('error', {})
                        logr.error(f"Failed to resubscribe (attempt {attempt + 1}/3): {error.get('message', 'Unknown error')}")
                        if attempt < 2:  # Only sleep if we're going to retry
                            await asyncio.sleep(1 * (attempt + 1))  # Exponential backoff
                except Exception as e:
                    logr.exception(f"Error during resubscription (attempt {attempt + 1}/3): {e}")
                    if attempt < 2:  # Only sleep if we're going to retry
                        await asyncio.sleep(1 * (attempt + 1))  # Exponential backoff
            else:  # All attempts failed for this subscription
                logr.error(f"Failed to restore subscription for {event_info['address']} after 3 attempts")
                await queue.put(None)  # Notify reader that subscription failed
        
        logr.info(f"Resubscription complete. Restored {len(self.subscriptions)} of {len(old_subscriptions)} subscriptions")

    async def _subscribe_to_block_headers(self):
        """Subscribe to new block headers"""
        subscribe_params = {
            "jsonrpc": "2.0",
            "id": self.next_sub_request_id,
            "method": "eth_subscribe",
            "params": ["newHeads"]
        }
        self.next_sub_request_id += 1

        # Create response queue for this request
        response_queue = asyncio.Queue()
        self.response_queues[subscribe_params["id"]] = response_queue

        try:
            logr.info("Subscribing to block headers")
            await self.ws.send(json.dumps(subscribe_params))

            # Wait for response with timeout
            try:
                response = await asyncio.wait_for(response_queue.get(), timeout=5)
            except asyncio.TimeoutError:
                logr.error("Timeout waiting for block header subscription response")
                raise
        finally:
            # Clean up response queue
            self.response_queues.pop(subscribe_params["id"], None)

        if "result" in response:
            self.block_header_sub_id = response["result"]
            logr.info(f"Successfully subscribed to block headers with ID: {self.block_header_sub_id}")
        else:
            logr.error(f"Failed to subscribe to block headers: {response}")
            raise Exception(f"Failed to subscribe to block headers: {response.get('error', 'Unknown error')}")

    async def read_blocks(self, chain_id, after):
        """Get an async iterator over block headers"""
        while not self._closing:
            try:
                header = await self.block_headers_queue.get()
                if header is None:
                    return
                block_num = int(header['number'], 16)
                if block_num > after:
                    event = {'block_number': block_num,
                            'timestamp': int(header['timestamp'], 16)}        
                    yield event
    
            except Exception as e:
                logr.exception(f"Error getting block header: {e}")
                await asyncio.sleep(1)

    async def read(self, chain_id, address, signature, abis, after):
        event = abis.get_by_signature(signature)
        abi = event.literal
        inputs = abi['inputs']
        
        event_info = {
            "inputs": inputs,
            "signature": event.signature,
            "topic": event.topic,
            "address": address
        }

        queue = asyncio.Queue()
        
        while True:
            logr.info(f"Starting read loop for {address} with topic {event.topic}")
            try:
                # Check connection status and establish if needed
                async with self.ws_lock:
                    need_new_connection = False
                    if self.ws is None:
                        need_new_connection = True
                    else:
                        try:
                            pong = await self.ws.ping()
                            await asyncio.wait_for(pong, timeout=1)
                        except Exception:
                            need_new_connection = True

                if need_new_connection:
                    await self.ensure_connection()

                # Set up subscription
                subscribe_params = {
                    "jsonrpc": "2.0",
                    "id": self.next_sub_request_id,
                    "method": "eth_subscribe",
                    "params": [
                        "logs",
                        {
                            "address": address,
                            "topics": ["0x" + event.topic]
                        }
                    ]
                }
                self.next_sub_request_id += 1
                
                # Create response queue for this request
                response_queue = asyncio.Queue()
                self.response_queues[subscribe_params["id"]] = response_queue

                try:
                    # Send subscription request
                    logr.info(f"Subscribing to {address} with topic {event.topic}")
                    await self.ws.send(json.dumps(subscribe_params))
                    
                    # Wait for response with timeout
                    try:
                        response = await asyncio.wait_for(response_queue.get(), timeout=5)
                    except asyncio.TimeoutError:
                        logr.error(f"Timeout waiting for subscription response for {address}")
                        raise
                finally:
                    # Clean up response queue
                    self.response_queues.pop(subscribe_params["id"], None)

                if "result" in response:
                    sub_id = response["result"]
                    async with self.subscription_lock:
                        self.subscriptions[sub_id] = (queue, event_info)
                    logr.info(f"Setup subscription ID: {sub_id} for address: {address}")
                else:
                    logr.error(f"Failed to subscribe: {response}")
                    await asyncio.sleep(1)
                    continue

                while True:
                    try:
                        event = await queue.get()
                        if event is None:
                            # Subscription was cancelled
                            return
                        yield event

                    except websockets.exceptions.ConnectionClosedError as err:
                        logr.exception(f"ConnectionClosedError: Problem getting real time data for {address} {signature}: {err}")
                        break  # Break to outer loop to reconnect
                    except Exception as err:
                        logr.exception(f"Other Exception: Problem getting real time data for {address} {signature}: {err}")
                        await asyncio.sleep(1)
                        break  # Break to outer loop to reconnect

            except Exception as e:
                logr.exception(f"Error in read loop: {e}")
                await asyncio.sleep(1)  # Brief delay before retry

    @staticmethod
    def decode_payload(ws_payload, inputs, signature, topic):
        def bytes_to_str(x):
            if isinstance(x, bytes):
                return x.hex()
            return x

        def array_of_bytes_to_str(x):
            if isinstance(x, list):
                return [bytes_to_str(i) for i in x]
            elif isinstance(x, bytes):
                return bytes_to_str(x)
            return x
            
        log_data = ws_payload["data"]
        log_topics = ws_payload["topics"]

        # Extract indexed vs non-indexed inputs
        indexed_inputs = [i for i in inputs if i['indexed']]
        non_indexed_inputs = [i for i in inputs if not i['indexed']]

        # Decode indexed topics (skip topic[0] which is event sig hash)
        indexed_values = [
            decode_abi([i["type"]], bytes.fromhex(t[2:]))[0]
            for i, t in zip(indexed_inputs, log_topics[1:])
        ]
        non_indexed_values = list(decode_abi(
            [i["type"] for i in non_indexed_inputs],
            bytes.fromhex(log_data[2:])
        ))

        decoded = {}
        for i, arg in enumerate(indexed_inputs + non_indexed_inputs):
            decoded[arg["name"]] = (indexed_values + non_indexed_values)[i]

        out = {
            "block_number": str(int(ws_payload["blockNumber"], 16)),
            "log_index": int(ws_payload["logIndex"], 16),
            "transaction_index": int(ws_payload["transactionIndex"], 16),
            "signature": signature,
            "sighash": topic,
        }
        out.update(decoded)

        out = {
            camel_to_snake(k): array_of_bytes_to_str(v)
            for k, v in out.items()
        }

        return out
