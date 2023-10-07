"""
    Priyanka Gorentla Created on 10/04/2023 
    This program sends a message to a queue on the RabbitMQ server.
    Queue contains details about flight status and all.
"""

import pika
import sys
import webbrowser
import csv
import time

# Configure logging
from util_logger import setup_logger

logger, logname = setup_logger(__file__)

SHOW_OFFER = False

#Sets up a direct link to rabbitmq server.
def offer_rabbitmq_admin_site(show_offer):
    if show_offer == True:
        """Offer to open the RabbitMQ Admin website"""
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print({ans})

def send_message(host: str, queue_name: str, message):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.
    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(
        host="localhost",
        credentials=pika.PlainCredentials(username="guest", password="Vijjulu@12")))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        logger.info(f" [x] Sent {message}")
        # wait 3 seconds before sending the next message to the queue
        time.sleep(3)
    except pika.exceptions.AMQPConnectionError as e:
        logger.info(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()


# read from a file to get some data
input_file = open("Airline_data.csv", "r")

# create a csv reader for our comma delimited data
reader = csv.reader(input_file, delimiter=",")


for row in reader:

    # prepare a binary (1s and 0s) message to stream
    message = ",".join(row)

    send_message("localhost", "airqueue", message)

    # sleep for a few seconds
    time.sleep(2)

input_file.close()



# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # determine if offer_rabbitmq_admin_site() should be run 
    if SHOW_OFFER == True:
    # ask the user if they'd like to open the RabbitMQ Admin site
      offer_rabbitmq_admin_site()