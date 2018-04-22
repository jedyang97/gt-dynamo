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

    def key_to_nodes(self, key):
        key_hashcode = int.from_bytes(hashlib.sha256(key.encode()).digest(), byteorder="little")
        target_node_index = key_hashcode % len(self.node_info_list)
        target_node_indices = [(target_node_index + i) % len(self.node_info_list) for i in range(K)]
        target_node_info_list = [n for i, n in enumerate(self.node_info_list) if i in target_node_indices]
        return target_node_info_list

    def put(self, key, value):
        return write_to_node(key_to_nodes(key), key, value)

    def get(self, key):
        retd_list = read_from_node(key_to_nodes(key))
        if len(retd_list) == 0: # failed to get response from a majority of nodes
            return -1
        latest_timestamp = max(retd_list[:][1])
        for value, timestamp in retd_list:
            if timestamp == latest_timestamp:
                return value

    def finalize(self):
        pass


def write_to_nodes(node_address_list, key, value):
    pickled_key_value_pair = pickle.dumps((key, value))


