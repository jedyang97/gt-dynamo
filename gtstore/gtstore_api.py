'''
GT Store Library API
'''

import socket
import sys
import pickle

from util import *
from constant import *
from server import *
import hashlib

class DBConnection:
    def __init__(self, master_ip_addr, master_port):

        self.master_ip_addr = master_ip_addr
        self.master_port = master_port

        # create an INET, STREAMing socket
        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # now connect to the server
        master_socket.connect((master_ip_addr, master_port))

        # send hand-shake msg
        msg = (HEADER_CLIENT_HELLO, None, None)
        pickled_msg = pickle.dumps(msg)
        send_msg(master_socket, pickled_msg)

        # recv node list
        pickled_node_address_list = recv_msg(master_socket)
        master_socket.close()

        self.node_address_list = pickle.loads(pickled_node_address_list)
        print(self.node_address_list)

    def put(self, key, value):
        pass

    def get(self, key):
        pass

    def finalize(self):
        pass


def write_to_nodes(node_address_list, key, value):
    pickled_key_value_pair = pickle.dumps((key, value))


