import sys
import socket
import pickle

from util import *
from constant import *
from server import *
from gtstore_api import *

if __name__ == "__main__":

    conn = DBConnection('localhost', 4210)