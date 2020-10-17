#!/usr/bin/env python3

import argparse
import logging
import json

from collections import defaultdict
from dslib import Communicator, Message


class Peer:
    def __init__(self, name, addr, peers):
        self._name = name
        self._peers = peers
        self._comm = Communicator(name, addr)

        self.num_msg_sent = 0
        self.user_to_id_to_msg = defaultdict(dict)
        self.user_to_num_delivered = defaultdict(int)

        self.received = defaultdict(int)

    def deliver_local(self, msg):
        deliver_msg = Message('DELIVER', json.loads(msg.headers)['from'] + ': ' + msg.body)
        self._comm.send_local(deliver_msg)

    def run(self):
        while True:
            msg = self._comm.recv()

            # local user wants to send a message to the chat
            if msg.type == 'SEND' and msg.is_local():
                # basic broadcast
                bcast_msg = Message('BCAST', msg.body, json.dumps({'from': self._name, 'msg_id': self.num_msg_sent}))
                self.num_msg_sent += 1
                for peer in self._peers:
                    self._comm.send(bcast_msg, peer)

            # received broadcasted message
            elif msg.type == 'BCAST':
                headers = json.loads(msg.headers)
                user = headers['from']
                msg_id = headers['msg_id']

                if user != self._name:
                    if msg not in self.received.keys():
                        for peer in self._peers:
                            self._comm.send(msg, peer)
                if user == self._name:
                    if msg not in self.received.keys():
                        self.received[msg] -= 1

                self.received[msg] += 1

                if self.received[msg] == int(len(self._peers) / 2):
                    msg_id_to_msg = self.user_to_id_to_msg[user]
                    msg_id_to_msg[msg_id] = msg

                    if self.user_to_num_delivered[user] == msg_id:
                        while msg_id in msg_id_to_msg:
                            self.deliver_local(msg_id_to_msg[msg_id])
                            self.user_to_num_delivered[user] += 1
                            msg_id += 1

                # if msg in self.received:
                #     continue
                # headers = json.loads(msg.headers)
                # self.received.add(msg)
                # if headers['from'] != self._name:
                #     for peer in self._peers:
                #         self._comm.send(msg, peer)
                # self.deliver_local(msg)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', dest='name', 
                        help='peer name (should be unique)', default='peer1')
    parser.add_argument('-l', dest='addr', metavar='host:port', 
                        help='listen on specified address', default='127.0.0.1:9701')
    parser.add_argument('-p', dest='peers', 
                        help='comma separated list of peers', default='127.0.0.1:9701,127.0.0.1:9702')
    parser.add_argument('-d', dest='log_level', action='store_const', const=logging.DEBUG,
                        help='print debugging info', default=logging.WARNING)
    args = parser.parse_args()
    logging.basicConfig(format="%(asctime)s - %(message)s", level=args.log_level)

    peer = Peer(args.name, args.addr, args.peers.split(','))
    peer.run()


if __name__ == "__main__":
    main()
