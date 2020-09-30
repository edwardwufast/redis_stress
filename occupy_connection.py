import redis
import asyncio
import sys

host=sys.argv[1]

async def connect_and_sleep(**kwargs):
        connection = redis.Connection(**kwargs)
        try:
            connection.connect()
            await asyncio.sleep(3600)
            connection.disconnect()
        except Exception as e:
            print(e)
        finally:
            del connection

async def main():
    await asyncio.gather(*(connect_and_sleep(host=host, port=6379) for i in range(60000)))


if __name__ == "__main__":
    while True:
        asyncio.run(main())
