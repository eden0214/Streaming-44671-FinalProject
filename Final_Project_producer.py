"""
    This program sends a message to a queue on the RabbitMQ server.
    We want to stream information from a smart smoker. Read one value every half minute.
    Author: Eden Anderson
    Date: 2/11/2023
    Based on Module 4 Version 3 .py program
"""

# Eden Anderson / 2.11.23 / Creating a Producer

import pika
import sys
import numpy as np
import webbrowser
import socket
import csv
import time

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    if show_offer:True
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

# define variables
input_file = open("Help_Desk.csv", "r")
queue_name = "help_queue"

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.
    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the queue for the help desk ticket submission
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
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
        print(f" [x] Sent {message} on help_queue")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

# define csv reader and set up messages for queue
# use an enumerated type to set the address family to (IPV4) for internet
socket_family = socket.AF_INET 

# use an enumerated type to set the socket type to UDP (datagram)
socket_type = socket.SOCK_DGRAM 

# use the socket constructor to create a socket object we'll call sock
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 

# create a csv reader for our comma delimited data
reader = csv.reader(input_file, delimiter=",")

for row in reader:
    for row in reader:
    # read a row from the file
        Date_Time, First_Name, Help_Request = row

    # use an fstring to create a message from our data
    # notice the f before the opening quote for our string?
        fstring_message = f"[{Date_Time}, {First_Name}, {Help_Request}]"
    
    # prepare a binary (1s and 0s) message to stream
        message = fstring_message.encode()

    # use the socket sendto() method to send the message
        send_message("localhost","help_queue",message)
        print (f"Sent: {message}")


# sleep for a few seconds - 30 seconds in real-time
# set to 1 for practice time
        time.sleep(5)



# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    show_offer=True
    offer_rabbitmq_admin_site(show_offer)
    # get the message from the command line
    # if no arguments are provided, use the default message
    # use the join method to convert the list of arguments into a string
    # join by the space character inside the quotes
    message = " ".join(sys.argv[1:]) or "Final Project....."
    # send the message to the queue
    send_message("localhost","help_queue",message)