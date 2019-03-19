from p2p_tools import MessageType, Message, P2P
import threading

"""Sender class"""
class Sender(P2P):
    """Initializes the Sequencer object with empy destination and sequencer addresses"""
    def __init__(self, socket):
        super(Sender, self).__init__(socket)
        self.sequencer_addr = None
        self.destination_addrs = []

    """Adds new fixed sequencer"""
    def add_sequencer(self, sequencer_addr_str):
        self.sequencer_addr = self.get_peer_tuple(sequencer_addr_str)

    """Adds new destination"""
    def add_destination(self, destination_addr_str):
        destination_addr = self.get_peer_tuple(destination_addr_str)
        if destination_addr_str in self.destination_addrs:
            return False
        else:
            self.destination_addrs.append(destination_addr_str)
            return True

    """Sends a new request sequence to sequencer"""
    def request_sequence(self, message_data):
        self.query(self.sequencer_addr, Message(MessageType.REQUEST_SEQUENCE, message_data, self.p2p_addr))

    """The main function for this class to take an action on listened messages.
    Handles response of previous sequence request sent to the requester,
    then broadcasts the message with received sequence number to destinations."""
    def response_action(self, message):
        if message.type == MessageType.RESPONSE_SEQUENCE:
            message_data = message.data[0]
            sequence = message.data[1]

            if message.reply_addr != self.sequencer_addr:
                print('Replier is not the fixed-sequencer!')
                return False
            else:
                self.broadcast_message(message_data)
                return True

    """Broadcasts the message with sequence to the destinations."""
    def broadcast_message(self, message):
        for destination_str in self.destination_addrs:
            destination_addr = self.get_peer_tuple(destination_str)
            self.query(destination_addr, Message(MessageType.CAST_MESSAGE, message, self.p2p_addr))

"""Sequencer class"""
class Sequencer(P2P):
    """Initializes the Sequencer object with sequence 1,
    and lock to provide concurreny between multiple sender requests"""
    def __init__(self, socket):
        super(Sequencer, self).__init__(socket)
        self.sequence = 1
        self.lock = threading.Lock()

    """The main function for this class to take an action on listened messages.
    Handles sequence requests made by senders and increases sequence number"""
    def response_action(self, message):
        if message.type == MessageType.REQUEST_SEQUENCE:
            # We lock and release to provide concurrency between each multiple requests.
            self.lock.acquire()
            # Sends the current sequence number with the message to the sender.
            self.query(message.reply_addr, Message(MessageType.RESPONSE_SEQUENCE, [message.data, self.sequence], self.p2p_addr))
            self.sequence += 1
            # The job is done, release the lock.
            self.lock.release()
            return True
        else:
            return False

"""Destination class"""
class Destination(P2P):
    """Initializes the Destination object with next_deliver sequence 1, an empty pending list
    and lock to provide concurreny between multiple sender messages"""
    def __init__(self, socket):
        super(Destination, self).__init__(socket)
        self.lock = threading.Lock()
        self.next_deliver = 1
        self.pendings = []

    """The main function for this class to take an action on listened messages.
    Handles broadcasts made by senders and puts them into a list and processes the list by order. Also recovers the order from lost messages."""
    def response_action(self, message):
        if message.type == MessageType.CAST_MESSAGE:
            self.lock.acquire()
            # Put new message to pending list
            self.pendings.append(message.data)
            for pending_message in self.pendings:
                message_data = pending_message[0]
                sequence = pending_message[1]
                # Process in order
                if sequence == self.next_deliver:
                    self.deliver(pending_message)
                    self.next_deliver += 1
            self.recover_messages()
            self.lock.release()
            return True
        else:
            return False

    """Recovers order if there is a message lost"""
    def recover_messages(self):
        min_pending_sequence = min([_[1] for _ in self.pendings])
        if 2 < min_pending_sequence - self.next_deliver:
            self.next_deliver = min_pending_sequence

    def deliver(self, message):
        print('The message is: ' + str(message[0]) + ' sequence is: ' + str(message[1]))

if __name__ == '__main__':
    sq = Sequencer(5001)
    sq.start_threaded()

    sd = Sender(5002)
    sd.start_threaded()

    dest = Destination(5003)
    dest.start_threaded()

    sd.add_sequencer("127.0.0.1:5001")
    sd.add_destination("127.0.0.1:5003")

    sd.request_sequence("hello")
    sd.request_sequence("hello2")
    sd.request_sequence("hello3")

    sd2 = Sender(5004)
    sd2.start_threaded()


    sd2.add_sequencer("127.0.0.1:5001")
    sd2.add_destination("127.0.0.1:5003")

    sd2.request_sequence("hello4")
    sd2.request_sequence("hello5")
    sd.request_sequence("hello6")
