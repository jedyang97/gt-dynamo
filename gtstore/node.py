'''
Node related code
'''

import socket
import threading
import pickle
import time
import hashlib

from util import *
from constant import *
from collections import defaultdict

class Node:
    def __init__(self, ip_addr, port):
        # create an INET, STREAMing socket
        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # bind the socket to a public host, and a well-known port
        node_socket.bind((ip_addr, port))
        # become a node socket
        node_socket.listen(5)

        self.socket = node_socket
        self.ip_addr = ip_addr
        self.port = port

        self.database = dict()
        self.node_ring = dict()

    def put(self, key, value):
        self.database[key] = (value, time.time())

    def get(self, key):
        if key in self.database:
            return self.database[key]
        else:
            return (None, None)

    def get_database(self):
        return self.database

    def put_to_nodes(self, node_address_list, key, value):
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

    def get_from_nodes(self, node_address_list, key):
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

    def update_node_ring(self, new_node_ring):
        self.node_ring = new_node_ring
        print('Node: %s:%s updated node_ring: %s' % (self.ip_addr, self.port, str(self.node_ring)))

    def key_to_nodes(self, key):
        key_hashcode = int.from_bytes(hashlib.sha256(key.encode()).digest(), byteorder="little")
        target_node_index = key_hashcode % len(self.node_address_list)
        target_node_indices = [(target_node_index + i) % len(self.node_address_list) for i in range(K)]
        target_node_address_list = [n for i, n in enumerate(self.node_address_list) if i in target_node_indices]
        return target_node_address_list

    def notify_master_node_status(self, master_ip_addr, master_port, status):
        '''
        Notify master the status of this node.
        '''
        # create an INET, STREAMing socket
        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # now connect to the server
        master_socket.connect((master_ip_addr, master_port))

        # send node status msg
        msg = (status, self.ip_addr, self.port)
        pickled_msg = pickle.dumps(msg)
        send_msg(master_socket, pickled_msg)

        # recv and decode message
        pickled_msg = recv_msg(master_socket)
        msg = pickle.loads(pickled_msg)

        # msg should have the format (HEADER, ip_addr, port)
        if not (type(msg) == tuple and len(msg) == 3):
            print('[ERROR] notify_master_node_status: msg format not correct, end the connection from %s:%s' % (self.ip_addr, self.port))
            master_socket.close()
            return

        if msg[0] == HEADER_OK:
            pass
        elif msg[0] == HEADER_NEED_FETCH_DATABASE:
            t = NodeGetDatabaseThread((msg[1], msg[2]), self)
            t.start()
            t.join()
            send_msg(master_socket, HEADER_OK.encode())

        master_socket.close()

    def handle_new_connection(self):
        '''
        This function will spawn a new thread to handle ONE incoming request
        '''
        # accept connections from outside
        (client_socket, address) = self.socket.accept()
        # now do something with the client_socket
        nct = NodeClientThread(client_socket, address, self)
        nct.start()
        nct.join() # a node must handle connections sequentially

    def __repr__(self):
        return 'Node: ' + str(self.__dict__)

class NodeClientThread(threading.Thread):
    def __init__(self, client_socket, address, node):
        threading.Thread.__init__(self)
        self.client_socket = client_socket
        self.address = address
        self.node = node

    def run(self):
        print('NodeClientThread: Get connected from %s:%s' % self.address)

        # recv and decode message
        pickled_msg = recv_msg(self.client_socket)
        msg = pickle.loads(pickled_msg)

        # msg should have the format (HEADER, data, data)
        if not (type(msg) == tuple and len(msg) == 3):
            print('[ERROR] NodeClientThread: msg format not correct, end the connection from %s:%s' % self.address)
            self.client_socket.close()
            return

        # msg has correct format, decode header
        if msg[0] == HEADER_CLIENT_PUT:
            # time.sleep(10)
            print('NodeClientThread: CLIENT PUT received from %s:%s! Calculating list of target nodes' % self.address)
            node_address_list = self.node.key_to_nodes(msg[1])
            print('NodeClientThread: List of target node %s' % str(node_address_list))
            if (self.node.ip_addr, self.node.port) in node_address_list:
                print('NodeClientThread: List includes the current node, remove to avoid self sending/recving')
                # TODO: How to incorperate this result to the result list?
                self.node.put(msg[1], msg[2])
                node_address_list.remove((self.node.ip_addr, self.node.port)) # avoid self sending/recving
            print('NodeClientThread: Started sending to target nodes...')
            put_result = self.node.put_to_nodes(self.node.key_to_nodes(msg[1]), msg[1], msg[2])

            print('NodeClientThread: put_to_nodes returned: %s, sending this to %s:%s' % (str(put_result), self.address[0], self.address[1]))
            print('NodeClientThread: Database of %s:%s: %s' % (self.node.ip_addr, self.node.port, str(self.node.database)))
            send_msg(self.client_socket, pickle.dumps(put_result))

        elif msg[0] == HEADER_CLIENT_GET:
#            print('NodeClientThread: CLIENT GET received from %s:%s! Getting key-value pair' % self.address)
#            pickled_value_timestamp_msg = pickle.dumps(self.node.get(msg[1]))
#            print('NodeClientThread: Successfully get key-value pair, sending value and timestamp to %s:%s' % self.address)
#            send_msg(self.client_socket, pickled_value_timestamp_msg)
            print('NodeClientThread: CLIENT GET received from %s:%s! Calculating list of target nodes' % self.address)
            node_address_list = self.node.key_to_nodes(msg[1])
            print('NodeClientThread: List of target node %s' % str(node_address_list))
            if (self.node.ip_addr, self.node.port) in node_address_list:
                print('NodeClientThread: List includes the current node, remove to avoid self sending/recving')
                # TODO: How to incorperate this result to the result list?
                self.node.get(msg[1])
                node_address_list.remove((self.node.ip_addr, self.node.port)) # avoid self sending/recving
            print('NodeClientThread: Started sending to target nodes...')
            get_result = self.node.get_from_nodes(self.node.key_to_nodes(msg[1]), msg[1])

            print('NodeClientThread: get_from_nodes returned: %s, sending this to %s:%s' % (str(get_result), self.address[0], self.address[1]))
            send_msg(self.client_socket, pickle.dumps(get_result))

        elif msg[0] == HEADER_MASTER_UPDATE_NODE_RING:
            print('NodeClientThread: MASTER UPDATE NODE RING received from %s:%s! Updating local node ring' % self.address)
            self.node.update_node_ring(msg[1])
            print('NodeClientThread: Successfully updated node ring, sending OK to master %s:%s' % self.address)
            send_msg(self.client_socket, HEADER_OK.encode())

        elif msg[0] == HEADER_NODE_PUT:
            print('NodeClientThread: NODE PUT received from %s:%s! Puting key-value pair' % self.address)
            self.node.put(msg[1], msg[2])
            print('NodeClientThread: Successfully put key-value pair, sending OK to %s:%s' % self.address)
            print('NodeClientThread: Database of %s:%s: %s' % (self.node.ip_addr, self.node.port, str(self.node.database)))
            send_msg(self.client_socket, HEADER_OK.encode())

        elif msg[0] == HEADER_NODE_GET:
            print('NodeClientThread: NODE GET received from %s:%s! Getting key-value pair' % self.address)
            pickled_value_timestamp_msg = pickle.dumps(self.node.get(msg[1]))
            print('NodeClientThread: Successfully get key-value pair, sending value and timestamp to %s:%s' % self.address)
            send_msg(self.client_socket, pickled_value_timestamp_msg)

        elif msg[0] == HEADER_NODE_GET_DATABASE:
            print('NodeClientThread: NODE GET DATABASE received from %s:%s! Getting key-value pair' % self.address)
            pickled_database_msg = pickle.dumps(self.node.get_database())
            print('NodeClientThread: Successfully get database, sending database to %s:%s' % self.address)
            send_msg(self.client_socket, pickled_database_msg)

        else:
            print('[ERROR] NodeClientThread: msg header not recognized, end the connection from %s:%s' % self.address)


        self.client_socket.close()
        print('NodeClientThread: Successfully ended the connection from %s:%s' % self.address)
        return

class PutToNodeThread(threading.Thread):
    def __init__(self, node_address, key, value, result_dict):
        threading.Thread.__init__(self)
        self.node_address = node_address
        self.key = key
        self.value = value
        self.result_dict = result_dict

    def run(self):
        # create an INET, STREAMing socket
        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # now connect to the node
        node_socket.connect(self.node_address)

        # send hand-shake msg
        msg = (HEADER_NODE_PUT, self.key, self.value)
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
        msg = (HEADER_NODE_GET, self.key, None)
        pickled_msg = pickle.dumps(msg)
        send_msg(node_socket, pickled_msg)

        # recv OK
        value_ts_msg = pickle.loads(recv_msg(node_socket))
        if type(value_ts_msg) == tuple and len(value_ts_msg) == 2 and value_ts_msg[1] != None:
            self.result_dict[self.node_address] = value_ts_msg

        node_socket.close()


class NodeGetDatabaseThread(threading.Thread):
    def __init__(self, target_node_address, node):
        threading.Thread.__init__(self)
        self.target_node_address = target_node_address
        self.node = node

    def run(self):
        # create an INET, STREAMing socket
        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # now connect to the node
        node_socket.connect(self.target_node_address)

        # send get database msg
        msg = (HEADER_NODE_GET_DATABASE, None, None)
        pickled_msg = pickle.dumps(msg)
        send_msg(node_socket, pickled_msg)

        # recv new database
        database_msg = pickle.loads(recv_msg(node_socket))
        if type(database_msg) != dict:
            print('[ERROR] NodeGetDatabaseThread: database got from %s:%s is not of type dict %s:%s' % self.target_node_address)

        # Merge the fetched database with current database
        self.merge_database(database_msg)

        node_socket.close()

    def merge_database(self, new_database):
        common_keys = set(self.node.database.keys()) & set(new_database.keys())
        common_dict = {common_key: max([self.node.database[common_key], new_database[common_key]], key=lambda x: x[1]) for common_key in common_keys} # choose value with latest timestamp

        self.node.database.update(new_database)
        self.node.database.update(common_dict)
        print('NodeGetDatabaseThread: Successfully merged database from %s:%s New database: %s' % (self.target_node_address[0], self.target_node_address[1], str(self.node.database)))

class NodeListenThread(threading.Thread):
    def __init__(self, node):
        threading.Thread.__init__(self)
        self.node = node

    def run(self):
        while True:
            self.node.handle_new_connection()

