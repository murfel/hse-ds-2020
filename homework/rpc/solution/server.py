#!/usr/bin/env python3

import argparse
import json
import logging

from dslib import Communicator, Message

from common import Store


class StoreImpl(Store):
    """This is Store service implementation"""

    def __init__(self):
        self._data = {}

    def put(self, key, value, overwrite):
        if key not in self._data or overwrite:
            self._data[key] = value
            return True
        else:
            return False

    def get(self, key):
        if key not in self._data:
            raise Exception('Key %s not found' % key)
        else:
            return self._data[key]

    def append(self, key, value):
        if key not in self._data:
            raise Exception('Key %s not found' % key)
        else:
            self._data[key] += value
            return self._data[key]

    def remove(self, key):
        if key not in self._data:
            raise Exception('Key %s not found' % key)
        else:
            return self._data.pop(key)


class RpcServer:
    """This is server-side RPC implementation"""

    def __init__(self, addr, service):
        # Your implementation
        self._comm = Communicator('server', addr)
        self._service = service
        self._id_to_msg_response = dict()

    def run(self):
        """Main server loop where it handles incoming RPC requests"""

        # Your implementation
        while True:
            msg = self._comm.recv()
            if msg.type != 'REQUEST':
                continue
            if msg.headers in self._id_to_msg_response.keys():
                self._comm.send(self._id_to_msg_response[msg.headers], msg.sender)
            content = json.loads(msg.body)
            func = content[0]
            args = content[1:]
            try:
                if func == 'put':
                    res = self._service.put(*args)
                elif func == 'get':
                    res = self._service.get(*args)
                elif func == 'append':
                    res = self._service.append(*args)
                elif func == 'remove':
                    res = self._service.remove(*args)
                else:
                    raise Exception('Unknown func: ', func)
            except Exception as e:
                m = Message('ERROR', str(e))
            else:
                m = Message('OK', json.dumps(res))
            self._id_to_msg_response[msg.headers] = m
            self._comm.send(m, msg.sender)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', dest='addr', metavar='host:port', 
                        help='listen on specified address', default='127.0.0.1:9701')
    parser.add_argument('-d', dest='log_level', action='store_const', const=logging.DEBUG,
                        help='print debugging info', default=logging.WARNING)
    args = parser.parse_args()
    logging.basicConfig(format="%(asctime)s - %(message)s", level=args.log_level)
    args = parser.parse_args()

    store = StoreImpl()
    server = RpcServer(args.addr, store)
    server.run()


if __name__ == "__main__":
    main()
