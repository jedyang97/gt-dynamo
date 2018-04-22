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

        self.node_ring = []

    def add_node_address(self, new_node_address):
        self.node_ring.append(new_node_address)

    def handle_new_connection(self):
        '''
        This function will spawn a new thread to handle ONE incoming request
        '''
        # accept connections from outside
        (client_socket, address) = self.socket.accept()
        # now do something with the client_socket
        mct = MasterClientThread(client_socket, address, self)
        mct.start()

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
            print('MasterClientThread: NODE ACTIVE received from %s:%s! Adding node to node list' % self.address)
            self.master.add_node_address((msg[1], msg[2]))
            print('MasterClientThread: New node address added, sending OK to %s:%s' % self.address)
            send_msg(self.client_socket, HEADER_OK.encode())
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

    def put(self, key, value):
        self.database[key] = (value, time.time())

    def get(self, key):
        if key in self.database:
            return self.database[key]
        else:
            return (None, None)

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

        # recv OK
        OK_msg = recv_msg(master_socket).decode()
        if OK_msg != HEADER_OK:
            print('[ERROR] notify_master_node_status: OK not received')

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
        nct.join()

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

        # msg should have the format (HEADER, key, value)
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
        else:
            print('[ERROR] NodeClientThread: msg header not recognized, end the connection from %s:%s' % self.address)


        self.client_socket.close()
        print('NodeClientThread: Successfully ended the connection from %s:%s' % self.address)
        return

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
