import sys
import socket
import pickle

from util import *
from constant import *
from server import *
from gtstore_api import *

if __name__ == "__main__":

    conn = DBConnection('localhost', 4210) # Initialization

    print("Mohan's value: " + str(conn.get("Mohan")))
    print("Ranjan's value: " + str(conn.get("Ranjan")))
    print("Han's value: " + str(conn.get("Han")))
    print("Jed's value: " + str(conn.get("Jed")))

    conn.finalize()
