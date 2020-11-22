#!/usr/bin/env python3

import argparse
import logging
import random

from bisect import bisect_right
from collections import defaultdict
from hashlib import sha256
from struct import unpack
from typing import List, Dict, Tuple, Set

from dslib.process import Context
from dslib import Message, Process, Runtime


class Node(Process):
    DEFAULT_GET_RESPONSE = ''
    DEFAULT_NUM_POINTS = 1000

    def __init__(self, name):
        super().__init__(name)
        self.points = []  # type: List[Tuple[float, str]]  # [(value, owner addr)]
        self.storage = defaultdict(str)  # type: Dict[str, str]
        self.my_addr = ''  # type: str
        self.addr_to_name = dict()  # type: Dict[str, str]
        self.waiting_get_keys = set()  # type: Set[str]
        self.waiting_put_keys_values = set()  # type: Set[str]
        self.waiting_delete_keys = set()  # type: Set[str]

    def gen_and_add_points(self):
        new_points = [(random.random(), self.my_addr) for _ in range(Node.DEFAULT_NUM_POINTS)]
        self.points.extend(new_points)
        self.points.sort()
        return new_points

    @staticmethod
    def to_tuple_list(lst):
        return list(map(tuple, lst))

    def merge_points(self, other_points):
        self.points.extend(other_points)
        self.points = list(set(self.points))
        self.points.sort()

    # stackoverflow.com/a/42909410/3478131
    @staticmethod
    def bytes_to_float(b):
        return float(unpack('L', sha256(b.encode('utf-8')).digest()[:8])[0]) / 2 ** 64

    def whose_node(self, key: str, points=None) -> str:
        if points is None:
            points = self.points
        i = bisect_right([x[0] for x in points], Node.bytes_to_float(key))
        return points[(i + 1) % len(points)][1]

    # remove addr from member list and remove its points
    def remove_member(self, addr):
        self.addr_to_name.pop(addr)
        self.points = list(filter(lambda x: x[1] != addr, self.points))

    def merge_storage(self, kvs):
        for kv in kvs:
            key, value = kv.split('=')
            self.storage[key] = value

    def receive(self, ctx, msg):

        if msg.is_local():

            # Client commands (API) ***************************************************************

            # Add new node to the system
            # - request body: address of some existing node
            # - response: none
            if msg.type == 'JOIN':
                self.my_addr = ctx.addr()
                self.addr_to_name[self.my_addr] = self.name
                seed = msg.body
                self.gen_and_add_points()
                if seed != self.my_addr:
                    ctx.send(Message('get addrs and points', headers=self.name, body=self.points), seed)

            # Remove node from the system
            # - request body: none
            # - response: none
            elif msg.type == 'LEAVE':
                new_points = list(filter(lambda x: x[1] != self.my_addr, self.points))
                addr_to_kv_list = defaultdict(list)
                for key, value in self.storage.items():
                    new_addr = self.whose_node(key, new_points)
                    if new_addr != self.my_addr:
                        addr_to_kv_list[new_addr].append('='.join([key, value]))

                for addr, lst in addr_to_kv_list.items():
                    ctx.send(Message('your kvs', lst), addr)

                for addr in self.addr_to_name.keys():
                    if addr != self.my_addr:
                        ctx.send(Message('i leave'), addr)

            # Get a list of nodes in the system
            # - request body: none
            # - response: MEMBERS message, body contains the list of all known alive nodes
            elif msg.type == 'GET_MEMBERS':
                ctx.send_local(Message('MEMBERS', list(self.addr_to_name.values())))

            # Get key value
            # - request body: key
            # - response: GET_RESP message, body contains value or empty string if record is not found
            elif msg.type == 'GET':
                key = msg.body
                addr = self.whose_node(key)
                if addr == self.my_addr:
                    if key not in self.storage.keys():
                        ctx.send_local(Message('GET_RESP', Node.DEFAULT_GET_RESPONSE))
                    else:
                        ctx.send_local(Message('GET_RESP', self.storage[key]))
                else:
                    ctx.send(Message('get global', key), addr)
                    self.waiting_get_keys.add(key)

            # Store value for the key
            # - request body: string "key=value"
            # - response: PUT_RESP message, body is empty
            elif msg.type == 'PUT':
                key, value = msg.body.split('=')
                addr = self.whose_node(key)
                if addr == self.my_addr:
                    self.storage[key] = value
                    ctx.send_local(Message('PUT_RESP'))
                else:
                    self.waiting_put_keys_values.add(msg.body)
                    ctx.send(Message('put global', msg.body), addr)

            # Delete value for the key
            # - request body: key
            # - response: DELETE_RESP message, body is empty
            elif msg.type == 'DELETE':
                key = msg.body
                addr = self.whose_node(key)
                if addr == self.my_addr:
                    self.storage.pop(key)
                    ctx.send_local(Message('DELETE_RESP'))
                else:
                    ctx.send(Message('delete global', key), addr)
                    self.waiting_delete_keys.add(key)

            # Get node responsible for the key
            # - request body: key
            # - response: LOOKUP_RESP message, body contains the node name
            elif msg.type == 'LOOKUP':
                ctx.send_local(Message('LOOKUP_RESP', self.addr_to_name[self.whose_node(msg.body)]))

            # Get number of records stored on the node
            # - request body: none
            # - response: COUNT_RECORDS_RESP message, body contains the number of stored records
            elif msg.type == 'COUNT_RECORDS':
                ctx.send_local(Message('COUNT_RECORDS_RESP', len(self.storage)))

            # Get keys of records stored on the node
            # - request body: none
            # - response: DUMP_KEYS_RESP message, body contains the list of stored keys
            elif msg.type == 'DUMP_KEYS':
                ctx.send_local(Message('DUMP_KEYS_RESP', list(self.storage.keys())))

            else:
                err = Message('ERROR', 'unknown command: %s' % msg.type)
                ctx.send_local(err)

        else:

            # Node-to-Node messages ***************************************************************

            # You can introduce any messages for node-to-node communication
            if msg.type == 'get addrs and points':
                self.addr_to_name[msg.sender] = msg.headers
                self.merge_points(Node.to_tuple_list(msg.body))
                ctx.send(Message('addrs', headers=self.addr_to_name, body=self.points), msg.sender)
            elif msg.type == 'addrs':
                self.addr_to_name = msg.headers
                self.merge_points(Node.to_tuple_list(msg.body))
                for addr in self.addr_to_name.keys():  # notify other members except seed that I joined
                    if addr != self.my_addr and addr != msg.sender:
                        ctx.send(Message('I joined', headers=self.name, body=self.points), addr)
            elif msg.type == 'I joined':
                self.addr_to_name[msg.sender] = msg.headers
                self.merge_points(Node.to_tuple_list(msg.body))
            elif msg.type == 'get global':
                key = msg.body
                if key not in self.storage:
                    ctx.send(Message('get global NOT FOUND'), msg.sender)
                else:
                    ctx.send(Message('get global resp', headers=key, body=self.storage[key]), msg.sender)
            elif msg.type == 'get global NOT FOUND':
                ctx.send_local(Message('GET_RESP', Node.DEFAULT_GET_RESPONSE))
            elif msg.type == 'get global resp':
                key = msg.headers
                value = msg.body
                if key in self.waiting_get_keys:
                    ctx.send_local(Message('GET_RESP', value))
                    self.waiting_get_keys.remove(key)
            elif msg.type == 'put global':
                key, value = msg.body.split('=')
                self.storage[key] = value
                ctx.send(Message('put global resp', msg.body), msg.sender)
            elif msg.type == 'put global resp':
                key = msg.body
                if key in self.waiting_put_keys_values:
                    ctx.send_local(Message('PUT_RESP'))
                    self.waiting_put_keys_values.remove(key)
            elif msg.type == 'delete global':
                key = msg.body
                assert key in self.storage  # otherwise, wrong routing
                self.storage.pop(key)
                ctx.send(Message('delete global resp', key), msg.sender)
            elif msg.type == 'delete global resp':
                key = msg.body
                if key in self.waiting_delete_keys:
                    ctx.send_local(Message('DELETE_RESP'))
                    self.waiting_delete_keys.remove(key)
            elif msg.type == 'your kvs':
                self.merge_storage(msg.body)
            elif msg.type == 'i leave':
                self.remove_member(msg.sender)
            elif msg.type == '':
                pass

            else:
                err = Message('ERROR', 'unknown message: %s' % msg.type)
                ctx.send(err, msg.sender)

    def on_timer(self, ctx, timer):
        # type: (Context, str) -> None
        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', dest='name',
                        help='node name (should be unique)', default='1')
    parser.add_argument('-l', dest='addr', metavar='host:port',
                        help='listen on specified address', default='127.0.0.1:9701')
    parser.add_argument('-d', dest='log_level', action='store_const', const=logging.DEBUG,
                        help='print debugging info', default=logging.WARNING)
    args = parser.parse_args()
    logging.basicConfig(format="%(asctime)s - %(message)s", level=args.log_level)

    node = Node(args.name)
    Runtime(node, args.addr).start()


if __name__ == "__main__":
    main()