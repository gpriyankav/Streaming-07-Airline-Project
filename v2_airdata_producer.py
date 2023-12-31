"""
    Priyanka Gorentla modified on 10/06/2023 
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

# True = offer to open RabbitMQ admin site upon running this file
# False = do not offer to open RabbitMQ admin site when running this file
SHOW_OFFER = True


def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    logger.info("Offer to monitor RabbitMQ queues.")
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        logger.info(f"Answer is {ans}.")


def send_message(
    host: str,
    first_queue_name: str,
    second_queue_name: str,
    input_file: str,
):
    """
    Creates and sends a message to the queues each execution.
    This process runs and finishes, but can be interrupted by the user.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue names (str): names of the first, second, and third queues
        input_file (str): the name of the CSV file to be read in as messages
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(
        host="localhost",
        credentials=pika.PlainCredentials(username="guest", password="Vijjulu@12")))
        # use the connection to create a communication channel
        ch = conn.channel()

        # delete all 3 queues to start over with fresh queues
        ch.queue_delete(queue=first_queue_name)
        ch.queue_delete(queue=second_queue_name)

        # declare all 3 durable queues again
        ch.queue_declare(queue=first_queue_name, durable=True)
        ch.queue_declare(queue=second_queue_name, durable=True)

        # read each row from an input file, then construct, encode, and send messages to appropriate queues
        with open(input_file, "r") as file:
            reader = csv.reader(file)
            # skip header row
            header = next(reader)
            logger.info("Skipped header row")

            for row in reader:
                # separate row into variables by column
                Passenger_ID, First_Name, Last_Name, Gender, Age, Nationality, Airport_Name, Airport_Country_Code, Country_Name, Airport_Continent, Continents, Departure, Arrival_Airport, Pilot_Name, Flight_Status = row

                # define two messages
                first_message = First_Name, Departure
                second_message = First_Name, Flight_Status

                # encode messages
                first_mess_encode = ",".join(first_message).encode()
                second_mess_encode = ",".join(second_message).encode()

                # use the channel to publish first message to first queue
                # every message passes through an exchange
                ch.basic_publish(
                    exchange="", routing_key=first_queue_name, body=first_mess_encode
                )
                # print a message to the console for the user
                logger.info(f"[x] Sent {first_message} to {first_queue_name}")
                # publish second message to second queue
                ch.basic_publish(
                    exchange="", routing_key=second_queue_name, body=second_mess_encode
                )
                # print a message to the console for the user
                logger.info(f"[x] Sent {second_message} to {second_queue_name}")

                # wait 3 seconds before sending the next message to the queue
                time.sleep(3)

    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # determine if offer_rabbitmq_admin_site() should be run
    if SHOW_OFFER == True:
        # ask the user if they'd like to open the RabbitMQ Admin site
        offer_rabbitmq_admin_site()

    # send the message to the queue
    send_message("localhost", "departure", "flight-status", "Airline_data.csv")