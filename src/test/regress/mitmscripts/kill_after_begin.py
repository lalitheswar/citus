from mitmproxy.utils import strutils
from mitmproxy.proxy.protocol import TlsLayer, RawTCPLayer

'''
Use with a command line like this:

mitmdump --rawtcp -p 9702 --mode reverse:localhost:9700 -s tcp_message.py
'''

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

    print_message(tcp_msg)

    # 2. Look for BEGIN. If you find it... mark the flow to be killed
    if tcp_msg.from_client and b'BEGIN' in tcp_msg.content:
        flow.should_kill = True
        return

    #  The next time the client sends a packet... kill the flow!
    if tcp_msg.from_client and getattr(flow, 'should_kill', False):
        flow.kill()
        return


def print_message(tcp_msg):
    print("[message] from {} to {}:\r\n{}".format(
        "client" if tcp_msg.from_client else "server",
        "server" if tcp_msg.from_client else "client",
        strutils.bytes_to_escaped_str(tcp_msg.content)
    ))
