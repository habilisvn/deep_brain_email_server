#!/bin/bash

# Start aio-pika consumer
echo "Starting aio-pika consumer..."
python3 main.py &

# Keep script running
echo "Consumer is running. Press Ctrl+C to stop."
wait
