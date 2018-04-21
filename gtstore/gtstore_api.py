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
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # now connect to the server
        client_socket.connect((master_ip_addr, master_port))

        # send hand-shake msg
        msg = MSG_HELLO.encode()
        send_msg(client_socket, msg)

        # recv node list
        pickled_node_info_list = recv_msg(client_socket)

        self.node_info_list = pickle.loads(pickled_node_info_list)
        print(self.node_info_list)

    def put(self, key, value):
        pass

    def get(self, key):
        pass

    def finalize(self):
        pass
