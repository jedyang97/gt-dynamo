'''
Master related
'''

import socket
import threading
import pickle
import time
import random

from util import *
from constant import *


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
            node_addrs = list(self.master.node_ring.keys())
            random.shuffle(node_addrs)
            random_node_addrs = node_addrs[:G]
            send_msg(self.client_socket, pickle.dumps(random_node_addrs))
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

