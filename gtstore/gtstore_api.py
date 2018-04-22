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
        target_node_index = key_hashcode % len(self.node_address_list)
        target_node_indices = [(target_node_index + i) % len(self.node_address_list) for i in range(K)]
        target_node_address_list = [n for i, n in enumerate(self.node_address_list) if i in target_node_indices]
        return target_node_address_list

    def put(self, key, value):
        return self._put_to_nodes(self.key_to_nodes(key), key, value)

#    def get(self, key):
#        retd_list = self._get_from_nodes(self.key_to_nodes(key))
#        if len(retd_list) == 0: # failed to get response from a majority of nodes
#            return -1
#        latest_timestamp = max(retd_list[:][1])
#        for value, timestamp in retd_list:
#            if timestamp == latest_timestamp:
#                return value

    def finalize(self):
        pass


    def _put_to_nodes(self, node_address_list, key, value):
        t_list = []
        for node_address in node_address_list:
            t_list.append(PutToNodeThread(node_address, key, value))
            t_list[-1].start()

        pickled_key_value_pair = pickle.dumps((key, value))


class PutToNodeThread(threading.Thread):
    def __init__(self, node_address, key, value):
        threading.Thread.__init__(self)
        self.node_address = node_address
        self.key = key
        self.value = value

    def run(self):
        print('PutToNodeThread: Get connected to %s:%s' % self.node_address)

        # create an INET, STREAMing socket
        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # now connect to the node
        node_socket.connect(self.node_address)

        # send hand-shake msg
        msg = (HEADER_CLIENT_PUT, self.key, self.value)
        pickled_msg = pickle.dumps(msg)
        send_msg(node_socket, pickled_msg)

        # recv OK
        OK_msg = recv_msg(node_socket).decode()
        if OK_msg != HEADER_OK:
            print('[ERROR] PutToNodeThread: OK not received')

        node_socket.close()

