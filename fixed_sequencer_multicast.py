from p2p_tools import MessageType, Message, P2P
import threading
from collections import defaultdict

class MulticastMessage:
    def __init__(self, message_data, group_id):
        self.message_data = message_data
        self.group_id = group_id

"""Sender class"""
class Sender(P2P):
    """Initializes the Sequencer object with empy destination and sequencer addresses"""
    def __init__(self, socket):
        super(Sender, self).__init__(socket)
        self.sequencer_addr = None
        self.destination_groups = defaultdict(list)
        self.lock = threading.Lock()

    """Adds new fixed sequencer"""
    def add_sequencer(self, sequencer_addr_str):
        self.sequencer_addr = self.get_peer_tuple(sequencer_addr_str)

    """Adds new destination with specified group_id"""
    def add_destination(self, destination_addr_str, group_id):
        destination_addr = self.get_peer_tuple(destination_addr_str)
        if destination_addr_str in self.destination_groups[group_id]:
            return False
        else:
            self.destination_groups[group_id].append(destination_addr_str)
            return True

    """Sends a new request sequence to sequencer"""
    def request_sequence(self, message_data, group_id):
        self.lock.acquire()
        multicast_message = MulticastMessage(message_data, group_id)
        self.query(self.sequencer_addr, Message(MessageType.REQUEST_SEQUENCE, multicast_message, self.p2p_addr))
        self.lock.release()

    """The main function for this class to take an action on listened messages.
    Handles response of previous sequence request sent to the requester,
    then multicasts the message with received sequence number to related destination group."""
    def response_action(self, message):
        if message.type == MessageType.RESPONSE_SEQUENCE:
            multicast_message = message.data[0]
            sequence = message.data[1]

            if message.reply_addr != self.sequencer_addr:
                print('Replier is not the fixed-sequencer!')
                return False
            else:
                self.multicast_message([multicast_message.message_data, sequence], multicast_message.group_id)
                return True

    """Multicast the message with sequence to the destinations."""
    def multicast_message(self, message_data, group_id):
        for destination_str in self.destination_groups[group_id]:
            destination_addr = self.get_peer_tuple(destination_str)
            self.query(destination_addr, Message(MessageType.CAST_MESSAGE, message_data, self.p2p_addr))

"""Sequencer class"""
class Sequencer(P2P):
    """Initializes the Sequencer object with sequence 0,
    and lock to provide concurreny between multiple sender requests"""
    def __init__(self, socket):
        super(Sequencer, self).__init__(socket)
        self.sequence = defaultdict(int)
        self.lock = threading.Lock()

    """The main function for this class to take an action on listened messages.
    Handles sequence requests made by senders and increases sequence number"""
    def response_action(self, message):
        if message.type == MessageType.REQUEST_SEQUENCE:
            # We lock and release to provide concurrency between each multiple requests.
            self.lock.acquire()
            multicast_message = message.data
            group_id = multicast_message.group_id
            # Sends the current sequence number with the message to the sender.
            self.query(message.reply_addr, Message(MessageType.RESPONSE_SEQUENCE, [multicast_message, self.sequence[group_id]], self.p2p_addr))
            self.sequence[group_id] += 1
            # The job is done, release the lock.
            self.lock.release()
            return True
        else:
            return False

"""Destination class"""
class Destination(P2P):
    """Initializes the Destination object with next_deliver sequence 0, an empty pending list
    and lock to provide concurreny between multiple sender messages"""
    def __init__(self, socket):
        super(Destination, self).__init__(socket)
        self.lock = threading.Lock()
        self.next_deliver = 0
        self.pendings = []

    """The main function for this class to take an action on listened messages.
    Handles multicasts made by senders and puts them into a list and processes the list by order. Also recovers the order from lost messages."""
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
        addr_str = str(self.p2p_addr)
        message_str = str(message[0])
        sequence_str = str(message[1])
        print(addr_str + '--' + 'm: ' +  message_str + ' sequence is: ' + sequence_str)

if __name__ == '__main__':
    sq = Sequencer(5001)
    sq.start_threaded()

    sd = Sender(5002)
    sd.start_threaded()

    dest = Destination(5003)
    dest.start_threaded()
    dest2 = Destination(5005)
    dest2.start_threaded()

    sd.add_sequencer("127.0.0.1:5001")
    sd.add_destination("127.0.0.1:5003", 1)
    sd.add_destination("127.0.0.1:5005", 2)


    sd.request_sequence("hello",1) # group 1, seq:0 for g1
    sd.request_sequence("hello2",1) # group 1, seq:1 for g1
    sd.request_sequence("hello3",2) # group 2, seq:0 for g2

    sd2 = Sender(5004)
    sd2.start_threaded()


    sd2.add_sequencer("127.0.0.1:5001")
    sd2.add_destination("127.0.0.1:5003", 1)
    sd2.add_destination("127.0.0.1:5005", 2)

    sd2.request_sequence("hello4",1) # group 1, seq: 2 for g1
    sd2.request_sequence("hello5",2) # group 2, seq: 1 for g2
    sd.request_sequence("hello6",1) # group 1, seq: 3 for g1
