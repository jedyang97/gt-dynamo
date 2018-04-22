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
import time

from collections import defaultdict

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

        self.gate_node_address_list = pickle.loads(pickled_node_address_list)
        # print(self.gate_node_address_list)

    def put(self, key, value):
        node_address = self.gate_node_address_list[0] # assume connect to the first gate node

        # create an INET, STREAMing socket
        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # now connect to the node
        node_socket.connect(node_address)

        # send CLIENT PUT msg
        msg = (HEADER_CLIENT_PUT, key, value)
        pickled_msg = pickle.dumps(msg)
        send_msg(node_socket, pickled_msg)

        # recv True/False
        result_msg = pickle.loads(recv_msg(node_socket))

        node_socket.close()

        return result_msg

    def get(self, key):
        #retd_list = self._get_from_nodes(self.key_to_nodes(key), key)
        ## eventual consistency mechanism
        #if not retd_list: # failed to get response from a majority of nodes
        #    return None
        #return max(retd_list, key=lambda x:x[1])[0] # return value with latest timestamp

        node_address = self.gate_node_address_list[0] # assume connect to the first gate node

        # create an INET, STREAMing socket
        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # now connect to the node
        node_socket.connect(node_address)

        # send CLIENT GET msg
        msg = (HEADER_CLIENT_GET, key, None)
        pickled_msg = pickle.dumps(msg)
        send_msg(node_socket, pickled_msg)

        # recv get result
        result_msg = pickle.loads(recv_msg(node_socket))

        node_socket.close()

        return result_msg

    def finalize(self):
        print("Bye!")
