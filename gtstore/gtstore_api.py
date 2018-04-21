'''
GT Store Library API
'''

import socket

class DBConnection:
    def __init__(self, master_ip_addr, master_port):

        # create an INET, STREAMing socket
        conn_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # now connect to the server
        conn_socket.connect((master_ip_addr, master_port))

    def put(self, key, value):
        pass

    def get(self, key):
        pass

    def finalize(self):
        pass
