import socket
import threading
import pickle

from util import *
from constant import *


class ClientThread(threading.Thread):
    def __init__(self, client_socket, address, master):
        threading.Thread.__init__(self)
        self.client_socket = client_socket
        self.address = address
        self.master = master

    def run(self):
        print('ClientThread: Get connected from %s:%s' % self.address)

        # wait for the HELLO message
        msg = recv_msg(self.client_socket)

        if msg.decode() != MSG_HELLO:
            print('ClientThread: First msg received is not MSG_HELLO, end the connection from %s:%s' % self.address)
            self.client_socket.close()
            return

        # Hand-shake succeed! Send all node information to client
        print('ClientThread: Hand-shake succeeded! Sending node list to %s:%s' % self.address)
        pickled_node_list = pickle.dumps(master.node_list)
        send_msg(self.client_socket, pickled_node_list)

        self.client_socket.close()
        print('ClientThread: Node list sent! End the connection from %s:%s' % self.address)
        return


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

        self.node_list = []

    def add_node(self, new_node_list):
        self.node_list += new_node_list

    def handle_new_connection(self):
        '''
        This function will spawn a new thread to handle ONE incoming request
        '''
        # accept connections from outside
        (client_socket, address) = self.socket.accept()
        # now do something with the client_socket
        ct = ClientThread(client_socket, address, self)
        ct.start()

        


class Node:
    def __init__(self, ip_addr, port):
        self.ip_addr = ip_addr
        self.port = port

    def __repr__(self):
        return 'Node: ' + str(self.__dict__)

if __name__ == "__main__":

    master = Master('localhost', 4210)

    node1 = Node('localhost', 4211)
    node2 = Node('localhost', 4212)
    node3 = Node('localhost', 4213)

    master.add_node([node1, node2, node3])

    while True:
        master.handle_new_connection()


