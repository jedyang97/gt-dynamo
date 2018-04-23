import socket
import threading
import pickle
import time

from util import *
from constant import *
from master import *
from node import *

# server.py
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
