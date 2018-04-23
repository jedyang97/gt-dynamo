import sys
import socket
import pickle

from util import *
from constant import *
from server import *
from gtstore_api import *

# client.py
if __name__ == "__main__":

    conn = DBConnection('localhost', 4210) # Initialization

    conn.put("Jed's cart", "Apple Banana Orange")
    conn.put("Han's cart", "Grape Steak Juice")
    conn.put("Yang's cart", "Noodle Pepper Milk")
    conn.put("Jianing's cart", "Eggs Pork Butter Napkins")

    print("Jed's cart: " + str(conn.get("Jed's cart")))
    print("Han's cart: " + str(conn.get("Han's cart")))
    print("Yang's cart: " + str(conn.get("Yang's cart")))
    print("Jianing's cart: " + str(conn.get("Jianing's cart")))

    conn.finalize()
