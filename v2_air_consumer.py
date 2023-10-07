"""
Priyanka Gorentla Created on 10/04/2023.
This program listens for work messages contiously. 
"""    
import pika
import time

# Configure logging
from util_logger import setup_logger

logger, logname = setup_logger(__file__)

def flight_status_callback(ch, method, properties, body):
    # This function is called when a message is received from the "flight-status" queue.
    # You can implement your logic for processing flight status messages here.
    flight_status = body.decode()
    departure = body.decode[1]
    
    if flight_status == "On Time":
        # Process the message only if the flight status is "On Time"
        logger.info(f"Received Flight Status Message: {flight_status} and departure: {departure}")
        # Add your logic here for handling "On Time" messages
    
def main():
    # Establish a connection to the RabbitMQ server
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host="localhost",
        credentials=pika.PlainCredentials(username="guest", password="Vijjulu@12")
    ))
    channel = connection.channel()

    # Declare the "flight-status" queue
    channel.queue_declare(queue="flight-status", durable=True)

    # Set up the consumer for the "flight-status" queue
    channel.basic_consume(queue="flight-status", on_message_callback=flight_status_callback, auto_ack=True)

    # Start consuming messages
    print("Waiting for 'On Time' flight status messages. To exit, press CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    main()
