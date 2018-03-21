import re

from mitmproxy.utils import strutils
from mitmproxy.proxy.protocol import TlsLayer, RawTCPLayer

'''
Use with a command line like this:

mitmdump --rawtcp -p 9702 --mode reverse:localhost:9700 -s tcp_message.py
'''

killed_a_connection = False  # we only want to kill one

def next_layer(layer):
    '''
    mitmproxy wasn't really meant for intercepting raw tcp streams, it tries to wrap the
    upsteam connection (the one to the worker) in a tls stream. This hook intercepts the
    part where it creates the TlsLayer (it happens in root_context.py) and instead creates
    a RawTCPLayer. That's the layer which calls our tcp_message hook
    '''
    if isinstance(layer, TlsLayer):
        replacement = RawTCPLayer(layer.ctx)
        layer.reply.send(replacement)

def tcp_message(flow):
    tcp_msg = flow.messages[-1]

    # 1. Once we know it's the maintenance daemon ignore packets from this connection
    if b'dump_local_wait_edges' in tcp_msg.content:
        flow.ignore = True
        return
    if getattr(flow, 'ignore', False):
        return

    msg = tcp_msg.content
    pat = re.compile(b'(test_[0-9]+)')

    if tcp_msg.from_client and msg.startswith(b'Q') and pat.search(msg):
        shard = pat.search(msg).groups()[0]
        flow.shard = shard
    else:
        shard = getattr(flow, 'shard', None)

    print_message(shard, tcp_msg)

#    global killed_a_connection
#    if killed_a_connection:
#        return

    if not tcp_msg.from_client and msg.startswith(b'H') and shard == b'test_102174':
        # this is a CopyOutResponse
        flow.kill()
        killed_a_connection = True
        tcp_msg.content = b''
        return

    return

    if not tcp_msg.from_client and msg.startswith(b'H') and b'COPY 1' in msg:
        # this is a CopyOutResponse
        flow.kill()
        killed_a_connection = True
        tcp_msg.content = b''
        return

def print_message(shard, tcp_msg):
    print("[{}] from {} to {}:\r\n{}".format(
        shard if shard else "message",
        "client" if tcp_msg.from_client else "server",
        "server" if tcp_msg.from_client else "client",
        strutils.bytes_to_escaped_str(tcp_msg.content)
    ))
