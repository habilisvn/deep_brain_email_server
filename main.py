from typing import Any
from aio_pika import connect_robust, Message, ExchangeType
import asyncio
from multiprocessing import Process  # type: ignore


async def worker(worker_id: int):
    # Perform connection
    connection = await connect_robust(
        "amqp://rabbitmq:abcd1234@localhost:5672/"
    )

    # Creating a channel
    channel = await connection.channel()

    # Declare an exchange
    exchange = await channel.declare_exchange(
        'user_events',
        ExchangeType.TOPIC,
        durable=True
    )

    # Declare a queue
    queue = await channel.declare_queue('user.created', durable=True)

    # Bind the queue to the exchange
    await queue.bind(exchange, routing_key='user.created')

    async def process_user_created(message: Message) -> Any:
        async with message.process():  # type: ignore
            user_data = message.body.decode()
            print(f"Worker {worker_id} processing new user event: {user_data}")

            # Add your processing logic here
            # Example: You could save to database, send notifications, etc

    # Start listening the queue with name 'user.created'
    await queue.consume(process_user_created)  # type: ignore

    print(f" [*] Worker {worker_id} waiting for messages. "
          "To exit press CTRL+C")

    try:
        # Wait for a never ending task to keep the worker running
        await asyncio.Future()
    finally:
        print(f" [*] Worker {worker_id} interrupted")
        await connection.close()


def start_worker(worker_id: int):
    asyncio.run(worker(worker_id))


def main():
    # Create multiple worker processes
    # Adjust the range for the number of workers you need
    processes = [  # type: ignore
        Process(target=start_worker, args=(i,)) for i in range(3)
    ]
    for process in processes:  # type: ignore
        process.start()  # type: ignore
    for process in processes:  # type: ignore
        process.join()  # type: ignore


if __name__ == '__main__':
    main()
