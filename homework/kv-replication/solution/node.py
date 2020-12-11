#!/usr/bin/env python3

import argparse
import bisect
import json
import logging
import random

from collections import defaultdict
from hashlib import sha256
from struct import unpack
from typing import Dict, List, Tuple

from dslib import Message, Process, Runtime

KeyType = str
ValueType = str
NodeAddres = str
VectorClock = Dict[NodeAddres, int]  # aka object version
VersionedValue = Tuple[VectorClock, ValueType]
OperationKeyJson = str


class Node(Process):
    def __init__(self, name):
        super().__init__(name)
        self.NUM_REPLICAS = 3
        self.points = []  # type: List[Tuple[float, NodeAddres]]  # [(value, owner addr)]
        self.storage = dict()  # type: Dict[KeyType, VersionedValue]
        self.my_addr = ''  # type: NodeAddres
        self.addr_to_name = dict()  # type: Dict[NodeAddres, str]
        self.vector_clock = dict()  # type: VectorClock
        self.waiting_get_keys = dict()  # type: Dict[KeyType, list]
        self.waiting_get_keys_quorum = dict()  # type: Dict[KeyType, int]
        self.waiting_put = dict()  # type: Dict[OperationKeyJson, int]
        self.waiting_put_addr = dict()  # type: Dict[OperationKeyJson, NodeAddres]

    def gen_and_add_points(self):
        new_point = (random.random(), self.my_addr)
        self.points.append(new_point)
        self.points.sort()
        return new_point

    @staticmethod
    def to_tuple_list(lst):
        return list(map(tuple, lst))

    def merge_points(self, other_points):
        self.points.extend(Node.to_tuple_list(other_points))
        self.points = list(set(self.points))
        self.points.sort()

    # stackoverflow.com/a/42909410/3478131
    @staticmethod
    def bytes_to_float(b):
        return float(unpack('L', sha256(b.encode('utf-8')).digest()[:8])[0]) / 2 ** 64

    # Returns list of replica addrs, starting with the main (coordinating) replica,
    # followed by back-up replicas in an order to choose from.
    def get_replicas_addrs(self, key: str, points=None) -> List[str]:
        if points is None:
            points = self.points
        i = bisect.bisect_right([x[0] for x in points], Node.bytes_to_float(key))
        return [point[1] for point in points[i:] + points[:i]]

    @staticmethod
    def simplify_vector_clock(vector_clock: VectorClock) -> List[int]:
        items = sorted(list(vector_clock.items()), key=lambda v: v[1])
        return list(map(lambda v: v[0], items))

    @staticmethod
    # -1 <=, 1 =>, 0 ==
    # assume all comparable and same length for now
    def cmp_vector_clocks(vector1: VectorClock, vector2: VectorClock) -> int:
        assert len(vector1.items()) == len(vector2.items())
        simple_vector_clock1 = Node.simplify_vector_clock(vector1)
        simple_vector_clock2 = Node.simplify_vector_clock(vector2)
        if all([v1 == v2 for v1, v2 in zip(simple_vector_clock1, simple_vector_clock2)]):
            return 0
        elif all([v1 <= v2 for v1, v2 in zip(simple_vector_clock1, simple_vector_clock2)]):
            return -1
        else:
            return 1

    @staticmethod
    def get_max_vector_clock(vector_clocks: List[VectorClock]) -> VectorClock:
        assert len(vector_clocks) != 0
        max_vector_clock = vector_clocks[0]
        for v in vector_clocks:
            if Node.cmp_vector_clocks(max_vector_clock, v) < 0:
                max_vector_clock = v
        return max_vector_clock

    @staticmethod
    def resolve_conflicts(versioned_values: List[VersionedValue]) -> List[VersionedValue]:
        # primitive conflict resolution
        if len(versioned_values) == 0:
            return []
        max_clock = Node.get_max_vector_clock(list(map(lambda v: v[0], versioned_values)))
        max_versioned_value = [v for v in versioned_values if v[0] == max_clock][0]
        return [max_versioned_value]

    def update_my_vector_clock(self, other_vector_clock: VectorClock):
        for addr, counter in other_vector_clock.items():
            if addr not in self.vector_clock.keys():
                assert False  # we missed a join
            else:
                self.vector_clock[addr] = max(self.vector_clock[addr], counter)

    def receive(self, ctx, msg):

        if msg.is_local():

            # Client commands (API) ***************************************************************

            # Add new node to the system
            # - request body: address of some existing node
            # - response: none
            if msg.type == 'JOIN':
                self.my_addr = ctx.addr()
                self.addr_to_name[self.my_addr] = self.name  # redundant, will be initialized in 'addrs'
                seed = msg.body
                self.gen_and_add_points()
                if seed != self.my_addr:
                    ctx.send(Message('get addrs and points', headers=self.name, body=self.points), seed)

            # Remove node from the system
            # - request body: none
            # - response: none
            elif msg.type == 'LEAVE':
                pass

            # Get a list of nodes in the system
            # - request body: none
            # - response: MEMBERS message, body contains the list of all known alive nodes
            elif msg.type == 'GET_MEMBERS':
                ctx.send_local(Message('MEMBERS', list(self.addr_to_name.values())))

            # Get key value
            # - request body: 
            #   - key: key (string)
            #   - quorum: quorum size for reading (int)
            # - reponse: GET_RESP message, body contains 
            #   - values: list of value versions (empty list if record is not found)
            #   - metadata: list of metadata (for each values[i] its metadata is provided in metadata[i])
            elif msg.type == 'GET':
                key = msg.body['key']
                quorum = msg.body['quorum']
                addrs = self.get_replicas_addrs(key)[:self.NUM_REPLICAS]
                for addr in addrs:
                    ctx.send(Message('get global', key), addr)
                self.waiting_get_keys[key] = []
                self.waiting_get_keys_quorum[key] = quorum
                # no timeout for now, will be needed for sloppy quorum later

            # Store value for the key
            # - request body: 
            #   - key: key (string)
            #   - value: value (string)
            #   - metadata: metadata of previously read or written value version (optional)
            #   - quorum: quorum size for writing (int)
            # - response: PUT_RESP message, body contains metadata of written version
            elif msg.type == 'PUT':
                key = msg.body['key']
                coordinator_addr = self.get_replicas_addrs(key)[0]
                ctx.send(Message('put for coordinator', msg.body), coordinator_addr)

            # Get nodes responsible for the key
            # - request body: key (string)
            # - response: LOOKUP_RESP message, body contains list with [node_name, node_address] elements
            elif msg.type == 'LOOKUP':
                ctx.send_local(Message('LOOKUP_RESP', [[self.addr_to_name[addr], addr] for addr in
                                                       self.get_replicas_addrs(msg.body)[:self.NUM_REPLICAS]]))

            # Get number of records stored on the node
            # - request body: none
            # - response: COUNT_RECORDS_RESP message, body contains the number of stored records
            elif msg.type == 'COUNT_RECORDS':
                ctx.send_local(Message('COUNT_RECORDS_RESP', len(self.storage)))

            else:
                err = Message('ERROR', 'unknown command: %s' % msg.type)
                ctx.send_local(err)

        else:

            # Node-to-Node messages ***************************************************************

            # You can introduce any messages for node-to-node communcation
            # JOIN messages
            if msg.type == 'get addrs and points':
                self.addr_to_name[msg.sender] = msg.headers
                self.merge_points(msg.body)
                self.vector_clock[msg.sender] = 0
                # self.extract_and_send_alien_kv(msg.sender, ctx)
                ctx.send(Message('addrs', headers=self.addr_to_name, body=self.points), msg.sender)
            elif msg.type == 'addrs':
                self.addr_to_name = msg.headers
                for addr in self.addr_to_name.keys():
                    self.vector_clock[addr] = 0
                self.merge_points(msg.body)
                for addr in self.addr_to_name.keys():  # notify other members except seed that I joined
                    if addr != self.my_addr and addr != msg.sender:
                        ctx.send(Message('I joined', headers=self.name, body=self.points), addr)
            elif msg.type == 'I joined':
                self.addr_to_name[msg.sender] = msg.headers
                self.merge_points(msg.body)
                self.vector_clock[msg.sender] = 0
                # self.extract_and_send_alien_kv(msg.sender, ctx)
            # GET messages
            elif msg.type == 'get global':
                key = msg.body
                versioned_value = None  # no value, no version
                if key in self.storage.keys():
                    versioned_value = self.storage[key]
                ctx.send(Message('get global resp', headers=key, body=versioned_value), msg.sender)
            elif msg.type == 'get global resp':
                key = msg.headers
                versioned_value = msg.body
                if key in self.waiting_get_keys.keys():
                    versioned_values = self.waiting_get_keys[key]
                    versioned_values.append(versioned_value)
                    if len(versioned_values) == self.waiting_get_keys_quorum[key]:
                        versioned_values = list(filter(lambda x: x is not None, versioned_values))
                        versioned_values = Node.resolve_conflicts(versioned_values)
                        ctx.send_local(Message('GET_RESP',
                                               {'values': list(map(lambda v: v[1], versioned_values)),
                                                'metadata': list(map(lambda v: v[0], versioned_values))}))
                        self.waiting_get_keys.pop(key)
                        self.waiting_get_keys_quorum.pop(key)
            # PUT messages
            elif msg.type == 'put for coordinator':
                key = msg.body['key']
                value = msg.body['value']
                if 'metadata' in msg.body.keys():
                    metadata = msg.body['metadata']
                    self.update_my_vector_clock(metadata)
                self.vector_clock[self.my_addr] += 1

                addrs = self.get_replicas_addrs(key)[:self.NUM_REPLICAS]
                msg.body['versioned_value'] = (self.vector_clock, value)
                self.waiting_put[key] = 0
                self.waiting_put_addr[key] = msg.sender
                for addr in addrs:
                    ctx.send(Message('put global', msg.body), addr)
            elif msg.type == 'put global':
                key = msg.body['key']
                versioned_value = tuple(msg.body['versioned_value'])
                self.storage[key] = versioned_value
                ctx.send(Message('put global resp', msg.body), msg.sender)
            elif msg.type == 'put global resp':
                key = msg.body['key']
                quorum = msg.body['quorum']
                if key in self.waiting_put:
                    self.waiting_put[key] += 1
                    if self.waiting_put[key] == quorum:
                        ctx.send(Message('put for coordinator resp', self.storage[key][0]), self.waiting_put_addr[op_key])
                        self.waiting_put.pop(key)
            elif msg.type == 'put for coordinator resp':
                # body should contain the new metadata (version) for the put object
                ctx.send_local(Message('PUT_RESP', msg.body))

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
