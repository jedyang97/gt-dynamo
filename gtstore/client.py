import sys
import socket
import pickle

from util import *
from constant import *
from server import *
from gtstore_api import *

if __name__ == "__main__":

    conn = DBConnection('localhost', 4210) # Initialization

    print(conn.put("Mohan", "I love Ranjan!"))
    print(conn.put("Ranjan", "I love Mohan!"))
    print(conn.put("Han", "I hate Ranjan!"))
    print(conn.put("Jed", "I love Mohan!"))

    print("Mohan's value: " + str(conn.get("Mohan")))
    print("Ranjan's value: " + str(conn.get("Ranjan")))
    print("Han's value: " + str(conn.get("Han")))
    print("Jed's value: " + str(conn.get("Jed")))

    conn.finalize()
