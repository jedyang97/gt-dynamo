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

        self.node_address_list = pickle.loads(pickled_node_address_list)
        # print(self.node_address_list)

    def put(self, key, value):
        return self._put_to_nodes(self.key_to_nodes(key), key, value)

    def get(self, key):
        retd_list = self._get_from_nodes(self.key_to_nodes(key), key)
        # eventual consistency mechanism
        if not retd_list: # failed to get response from a majority of nodes
            return None
        return max(retd_list, key=lambda x:x[1])[0] # return value with latest timestamp

    def finalize(self):
        print("Bye!")

    def _put_to_nodes(self, node_address_list, key, value):
        result_dict = defaultdict(bool)
        t_list = []
        for node_address in node_address_list:
            t_list.append(PutToNodeThread(node_address, key, value, result_dict))
            t_list[-1].start()

        for t in t_list:
            t.join(timeout=TIMEOUT)

        # majority count
        success_count = 0
        for node_address in node_address_list:
            if result_dict[node_address]:
                success_count += 1
        if 1.0 * success_count / len(node_address_list) > 0.5:
            return True
        return False

    def _get_from_nodes(self, node_address_list, key):
        result_dict = defaultdict(bool)
        t_list = []
        for node_address in node_address_list:
            t_list.append(GetFromNodeThread(node_address, key, result_dict))
            t_list[-1].start()

        for t in t_list:
            t.join(timeout=TIMEOUT)

        # majority count
        success_list = []
        for node_address in node_address_list:
            if result_dict[node_address]:
                success_list.append(result_dict[node_address])
        if 1.0 * len(success_list) / len(node_address_list) > 0.5:
            return success_list
        return False


class PutToNodeThread(threading.Thread):
    def __init__(self, node_address, key, value, result_dict):
        threading.Thread.__init__(self)
        self.node_address = node_address
        self.key = key
        self.value = value
        self.result_dict = result_dict

    def run(self):
        # print('PutToNodeThread: Get connected to %s:%s' % self.node_address)

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
        if OK_msg == HEADER_OK:
            self.result_dict[self.node_address] = True

        node_socket.close()


class GetFromNodeThread(threading.Thread):
    def __init__(self, node_address, key, result_dict):
        threading.Thread.__init__(self)
        self.node_address = node_address
        self.key = key
        self.result_dict = result_dict

    def run(self):
        # print('GetFromNodeThread: Get connected to %s:%s' % self.node_address)

        # create an INET, STREAMing socket
        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # now connect to the node
        node_socket.connect(self.node_address)

        # send hand-shake msg
        msg = (HEADER_CLIENT_GET, self.key, None)
        pickled_msg = pickle.dumps(msg)
        send_msg(node_socket, pickled_msg)

        # recv OK
        value_ts_msg = pickle.loads(recv_msg(node_socket))
        if type(value_ts_msg) == tuple and len(value_ts_msg) == 2 and value_ts_msg[1] != None:
            self.result_dict[self.node_address] = value_ts_msg

        node_socket.close()
