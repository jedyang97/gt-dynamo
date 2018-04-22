import socket
import threading
import pickle

from util import *
from constant import *
import time

''' Master related '''

class Master:
    def __init__(self, ip_addr, port):
        # create an INET, STREAMing socket
        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # bind the socket to a public host, and a well-known port
        master_socket.bind((ip_addr, port))
        # become a server socket
        master_socket.listen(5)

        self.socket = master_socket
        self.ip_addr = ip_addr
        self.port = port

        self.node_ring = dict()

    def add_node_address(self, new_node_address):
        '''
        This function update the node ring and 
        broadcast the new node ring to all nodes if necessary.
        '''
        # old_node_ring_items = set(self.node_ring.items())
        # self.node_ring.update(new_node_address)
        # # TODO: better comparison
        # if old_node_ring != self.node_ring:
        #     self.broadcast_node_ring()
        if new_node_address not in self.node_ring:
            if len(self.node_ring) == 0:
                self.node_ring[new_node_address] = 0
                return None
            ring_pos_list = sorted(self.node_ring.items(), key=lambda x:x[1])
            interval_list = []
            for i in range(len(ring_pos_list) - 1):
                interval_list.append((ring_pos_list[i], ring_pos_list[i+1]))
            interval_list.append((ring_pos_list[-1], (ring_pos_list[0][0], ring_pos_list[0][1] + 1)))
            if len(interval_list) == 1: # if only one interval exists
                self.node_ring[new_node_address] = (interval_list[0][0][1] + 0.5) % 1
                node_address_to_copy = interval_list[0][1][0] # node after the inserted node
            else: # general case
                interval_list.sort(key=lambda x:- (x[1][1] - x[0][1]))
                self.node_ring[new_node_address] = (1.0 * (interval_list[0][0][1] + interval_list[0][1][1]) / 2) % 1
                node_address_to_copy = interval_list[0][1][0] # node after the inserted node
            return node_address_to_copy

    def broadcast_node_ring(self):
        '''
        Broadcast the node ring to all nodes.
        '''
        t_list = []
        for node_address in self.node_ring.keys():
            t_list.append(MasterSendNodeRingThread(node_address, self))
            t_list[-1].start()

        for t in t_list:
            t.join()

        print("\n\n\n\n\nBroadcasted node ring: " + str(self.node_ring) + "\n\n\n\n\n")

    def handle_new_connection(self):
        '''
        This function will spawn a new thread to handle ONE incoming request
        '''
        # accept connections from outside
        (client_socket, address) = self.socket.accept()
        # now do something with the client_socket
        mct = MasterClientThread(client_socket, address, self)
        mct.start()
        mct.join() # master must handle connections sequentially

class MasterSendNodeRingThread(threading.Thread):
    def __init__(self, node_address, master):
        threading.Thread.__init__(self)
        self.node_address = node_address
        self.master = master

    def run(self):
        # create an INET, STREAMing socket
        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # now connect to the node
        node_socket.connect(self.node_address)

        # send node ring
        msg = (HEADER_MASTER_UPDATE_NODE_RING, self.master.node_ring, None)
        pickled_msg = pickle.dumps(msg)
        send_msg(node_socket, pickled_msg)

        # recv OK
        OK_msg = recv_msg(node_socket).decode()
        if OK_msg != HEADER_OK:
            print('[ERROR] MasterSendNodeRingThread: OK not received from %s:%s' % self.node_address)

        node_socket.close()

class MasterClientThread(threading.Thread):
    def __init__(self, client_socket, address, master):
        threading.Thread.__init__(self)
        self.client_socket = client_socket
        self.address = address
        self.master = master

    def run(self):
        print('MasterClientThread: Get connected from %s:%s' % self.address)

        # recv and decode message
        pickled_msg = recv_msg(self.client_socket)
        msg = pickle.loads(pickled_msg)

        # msg should have the format (HEADER, ip_addr, port)
        if not (type(msg) == tuple and len(msg) == 3):
            print('[ERROR] MasterClientThread: msg format not correct, end the connection from %s:%s' % self.address)
            self.client_socket.close()
            return

        # msg has correct format, decode header
        if msg[0] == HEADER_CLIENT_HELLO:
            print('MasterClientThread: CLIENT HELLO received from %s:%s! Sending node list to client' % self.address)
            pickled_node_ring = pickle.dumps(self.master.node_ring)
            send_msg(self.client_socket, pickled_node_ring)
        elif msg[0] == HEADER_NODE_ACTIVE:
            print('MasterClientThread: NODE ACTIVE received from %s:%s! Adding node to node ring' % self.address)
            node_address_to_fetch_from = self.master.add_node_address((msg[1], msg[2]))
            if node_address_to_fetch_from == None:
                print('MasterClientThread: New node address added, sending OK to %s:%s' % self.address)
                pickled_ok_msg = pickle.dumps((HEADER_OK, None, None))
                send_msg(self.client_socket, pickled_ok_msg)
            else: # need to fetch database from next node
                print('MasterClientThread: Need to fetch database from next node, sending its address to %s:%s' % self.address)
                pickled_need_fetch_msg = pickle.dumps((HEADER_NEED_FETCH_DATABASE, node_address_to_fetch_from[0], node_address_to_fetch_from[1]))
                send_msg(self.client_socket, pickled_need_fetch_msg)

                ok_msg = recv_msg(self.client_socket).decode()
                if ok_msg != HEADER_OK:
                    print('[ERROR] MasterClientThread: ok msg header not received from %s:%s' % self.address)
            self.master.broadcast_node_ring()

        else:
            print('[ERROR] MasterClientThread: msg header not recognized, end the connection from %s:%s' % self.address)


        self.client_socket.close()
        print('MasterClientThread: Successfully ended the connection from %s:%s' % self.address)
        return

class MasterListenThread(threading.Thread):
    def __init__(self, master):
        threading.Thread.__init__(self)
        self.master = master

    def run(self):
        while True:
            self.master.handle_new_connection()

''' Node related '''

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
            print('[ERROR] notify_master_node_status: msg format not correct, end the connection from %s:%s' % self.address)
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
            print('NodeClientThread: CLIENT PUT received from %s:%s! Puting key-value pair' % self.address)
            self.node.put(msg[1], msg[2])
            print('NodeClientThread: Successfully put key-value pair, sending OK to %s:%s' % self.address)
            print('NodeClientThread: Database of %s:%s: %s' % (self.node.ip_addr, self.node.port, str(self.node.database)))
            send_msg(self.client_socket, HEADER_OK.encode())

        elif msg[0] == HEADER_CLIENT_GET:
            print('NodeClientThread: CLIENT GET received from %s:%s! Getting key-value pair' % self.address)
            pickled_value_timestamp_msg = pickle.dumps(self.node.get(msg[1]))
            print('NodeClientThread: Successfully get key-value pair, sending value and timestamp to %s:%s' % self.address)
            send_msg(self.client_socket, pickled_value_timestamp_msg)

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

if __name__ == "__main__":

    # create master
    master = Master('localhost', 4210)
    mlt1 = MasterListenThread(master)
    mlt1.start()

    # create nodes
    node_list = []
    for i in range(M):
        node_list.append(Node('localhost', 4210 + i + 1))
        t = NodeListenThread(node_list[-1])
        t.start()
        node_list[-1].notify_master_node_status('localhost', 4210, HEADER_NODE_ACTIVE)

    # don't return so that we can see console output
    mlt1.join()
