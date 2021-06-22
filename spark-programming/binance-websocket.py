import asyncio
import json

import websockets
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Kafka server
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # json serializer
)


async def consumer_handler(frames):
    async for frame in frames:
        trade = json.loads(frame)
        print(trade)
        producer.send(topic='binance-trades', value=trade)


async def connect():
    async with websockets.connect("wss://stream.binance.com:9443/ws/btcusdt@trade") as ws:
        await consumer_handler(ws)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(connect())
    loop.run_forever()
