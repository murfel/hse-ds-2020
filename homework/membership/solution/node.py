#!/usr/bin/env python3

import argparse
import logging
import random
import time

from dslib import Message, Process, Runtime


class Node(Process):
    def __init__(self, name):
        super().__init__(name)
        self.counter = 0
        self.mem_list = dict()  # addr -> (counter, when_we_last_updated, name)
        self.addr_to_name = dict()
        self.on_leave()

    def on_leave(self):
        self.mem_list = dict()
        self.addr_to_name = dict()
        pass

    def send_msg(self, ctx, message_type, body=None, recipient_addr=None, multicast=False):
        recipients = self.mem_list.keys() if multicast else [recipient_addr]
        for recipient_addr in recipients:
            if recipient_addr == ctx.addr():
                continue
            ctx.send(Message(message_type, body=body, headers={
                        'name': self.name,
                        'counter': self.counter,
                        # 'reincarnation': self.reincarnation
                    }),
                    recipient_addr)

    def send_gossip(self, ctx):
        if len(self.mem_list) <= 1:
            return
        # update our counter
        self.counter += 1
        self.mem_list[ctx.addr()][1] = self.counter
        # choose a random recipient
        while True:
            random_addr = random.choice(list(self.mem_list.keys()))
            if random_addr != ctx.addr():
                break
        # send the msg
        self.send_msg(ctx, 'gossip', body=self.mem_list, recipient_addr=random_addr)
        # renew the timer
        ctx.set_timer('gossip', 1)

    def merge_mem_list_into_us(self, mem_list):
        for addr, tail in mem_list.items():
            if addr not in self.mem_list:
                self.mem_list[addr] = tail
                self.addr_to_name[addr] = tail[2]
            else:
                if mem_list[addr][0] > self.mem_list[addr][0]:
                    self.mem_list[addr][0] = mem_list[addr][0]
            self.mem_list[addr][1] = time.time()

    def receive(self, ctx, msg):

        if msg.is_local():

            # Client commands (API) ***************************************************************

            # Join the group
            # - message body contains the address of some existing group member
            if msg.type == 'JOIN':
                seed = msg.body
                # add yourself
                self.addr_to_name[ctx.addr()] = self.name
                self.mem_list[ctx.addr()] = [self.counter, time.time(), self.name]
                # notify either ourselves, or another member
                self.send_msg(ctx, 'join', recipient_addr=seed)  # receive the seed's name in their join-response

            # Leave the group
            elif msg.type == 'LEAVE':
                self.on_leave()
                self.send_msg(ctx, 'leave', multicast=True)
                ctx.cancel_timer('gossip')

            # Get a list of group members
            # - return the list of all known alive nodes in MEMBERS message
            elif msg.type == 'GET_MEMBERS':
                ctx.send_local(Message('MEMBERS', list(self.addr_to_name.values())))

            else:
                err = Message('ERROR', 'unknown command: %s' % msg.type)
                ctx.send_local(err)

        else:

            # Node-to-Node messages ***************************************************************

            # You can introduce any messages for node-to-node communcation
            name = msg.headers['name']
            addr = msg.sender
            if msg.type == 'join':
                self.addr_to_name[addr] = name
                self.mem_list[addr] = [msg.headers['counter'], time.time(), name]
                self.send_msg(ctx, 'join-response', body=self.mem_list, recipient_addr=addr)
            elif msg.type == 'join-response':
                self.merge_mem_list_into_us(msg.body)
                ctx.set_timer('gossip', 1)
            elif msg.type == 'leave':
                self.addr_to_name.pop(addr, None)
                self.mem_list.pop(addr, None)
            elif msg.type == 'gossip':
                self.merge_mem_list_into_us(msg.body)
            else:
                err = Message('ERROR', 'unknown message: %s' % msg.type)
                ctx.send(err, msg.sender)

    def on_timer(self, ctx, timer):
        # type: (Context, str) -> None
        if timer == 'gossip':
            self.send_gossip(ctx)
        else:
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
