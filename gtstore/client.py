import sys
import socket
import pickle

from util import *
from constant import *
from server import *

if __name__ == "__main__":

    ip_addr = 'localhost'
    port = 4210

    # create an INET, STREAMing socket
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # now connect to the server
    client_socket.connect((ip_addr, port))
    
    # send hand-shake msg
    msg = MSG_HELLO.encode()
    send_msg(client_socket, msg)

    # recv node list
    pickled_node_list = recv_msg(client_socket)

    node_list = pickled_node_list
    print(pickle.loads(pickled_node_list))
