import socket, select, pickle, threading
from enum import Enum
from time import sleep
import json
import struct
import traceback

class MessageType(Enum):
    REQUEST_SEQUENCE = 0
    RESPONSE_SEQUENCE = 1
    CAST_MESSAGE = 2

class Message:
    def __init__(self, message_type, message_data, reply_addr):
        self.type = message_type
        self.data = message_data
        self.reply_addr = reply_addr

class P2P(object):
    def __init__(self, socket):
        self.p2p_addr = ('127.0.0.1', socket)
        self.peer_sockets = {}
        self.p2p_socket = None

    def response_action(self, message):
        return NotImplemented

    def query(self, peer_addr, message):
        threading.Thread(
                target = self.send_message,
                args = (peer_addr, message)
        ).start()

    def get_peers(self):
        return list(self.peer_sockets.keys())

    def get_peer_str(self, peer_tuple):
        return ':'.join(map(str,peer_tuple))

    def get_peer_tuple(self, peer_str):
        vals = peer_str.split(':')
        address = vals[0]
        port = int(vals[1])
        return (address, port)

    def send_message(self, peer_addr, message):
        """Sends a message with provided data to a given address, opening a new
        p2p socket if neccesary"""
        try:
            peer_str = self.get_peer_str(peer_addr)
            if not peer_str in self.peer_sockets or True:
                peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peer_socket.connect(peer_addr)
                self.peer_sockets[peer_str] = peer_socket
            else:
                peer_socket = self.peer_sockets[peer_str]
                peer_socket.connect(peer_addr)
            peer_socket.send(pickle.dumps(message))
        except Exception:
            pass

    def start(self):
        self.p2p_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.p2p_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.p2p_socket.bind(self.p2p_addr)
        self.p2p_socket.listen(5)
        while True:
            conn_socket, addr = self.p2p_socket.accept()

            message = conn_socket.recv(4096)
            try:
                message = pickle.loads(message)
                peer_addr = (addr[0], message.reply_addr[1])
                message.reply_addr = peer_addr
                peer_str = self.get_peer_str(peer_addr)

                # Changes for each subclass, this is the main function which handles the messages
                self.response_action(message)
            except pickle.UnpicklingError as e:
                traceback.print_exc()
                pass

        self.p2p_socket.close()

    def start_threaded(self):
        print("Running on: " + str(self.p2p_addr))
        threading.Thread(
            target = self.start,
            args = ()
        ).start()
