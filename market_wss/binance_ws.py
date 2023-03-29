import json
import os
import time
import datetime
import websocket
import requests
import asyncio

UPDATES_PER_FILE = 256
PRINT_FREQUENCY = 128

class binance_ws:
    def __init__(self, symbol, depth):
        self.symbol = symbol
        self.depth = depth
        self.order_book_updates = []
        self.lastUpdateId = None
        self.last_u = None
        self.synced = False

        now = datetime.datetime.now()
        self.date = now.strftime('%Y%m%d')
        self.path = f'data/orderbook/binance/{self.symbol}/{self.date}/'
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self.take = f"take_{len(os.listdir(self.path)) + 1}"
        self.path = f"{self.path}/{self.take}"
        os.makedirs(os.path.dirname(self.path), exist_ok=True)

    def on_message(self, ws, message):
        data = json.loads(message)
        
        if not self.synced:            
            """
            U <= lastUpdateId+1 AND u >= lastUpdateId+1.
            """
            now = datetime.datetime.now()
            timestamp = now.strftime('%H%M%S%f')
            if (data["U"] <= self.lastUpdateId) and (self.lastUpdateId <= data["u"]):
                self.synced=True
                self.last_u = data["u"]
                print(f"First update event time: {timestamp}")
                return True
            else:
                print(f"Dropping event time: {timestamp}")

        if self.synced:
            """
            each new event's U should be equal to the previous event's u+1.
            """

            if data['U'] != (self.last_u + 1):
                print("Out of sync. Stopping process.")
                raise ValueError(f"Value does not match expected value: U: {data['U']} != last_u+1: {self.last_u + 1}")
                
            self.last_u = data['u']
            self.order_book_updates.append(data)
            
            if len(self.order_book_updates)%PRINT_FREQUENCY == 0 :
                print(f'Current update queue: {len(self.order_book_updates)}')
            
            
            if len(self.order_book_updates) == UPDATES_PER_FILE:
                now = datetime.datetime.now()
                timestamp = now.strftime('%H%M%S%f')
                path = f'{self.path}/{timestamp}.json'
                print(f"Updating time: {timestamp}")
                with open(path, 'w') as file:
                    json.dump(self.order_book_updates, file)
                self.order_book_updates = []

    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws):
        print("Connection closed")
        
    def on_open(self, ws):
        print("Connection opened")
        now = datetime.datetime.now()
        timestamp = now.strftime('%H%M%S%f')
        path = f'{self.path}/{timestamp}_ob.json'
        print(f"Getting order book: {timestamp}")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as file:
            response = requests.get(f'https://api.binance.com/api/v3/depth?symbol={self.symbol.upper()}&limit={self.depth}')
            self.lastUpdateId = response.json()["lastUpdateId"] + 1
            json.dump(response.json(), file)

    def run(self):
        websocket.enableTrace(True)        
        ws = websocket.WebSocketApp(f"wss://stream.binance.com:9443/ws/{self.symbol}@depth@100ms",
                    on_message = lambda ws,msg: self.on_message(ws, msg),
                    on_error   = lambda ws,msg: self.on_error(ws, msg),
                    on_close   = lambda ws:     self.on_close(ws),
                    on_open    = lambda ws:     self.on_open(ws))
        
        #ws.on_open = self.on_open
        ws.run_forever()
