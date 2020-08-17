"""
This module is used for serving socket-io connections.
Also provides socket-io object for emitting messages to clients.
"""

import socketio

__sio = socketio.AsyncServer()
__app = None


@__sio.event
def connect(sid, environ):
    print('connect ', sid)


@__sio.event
def disconnect(sid):
    print('disconnect ', sid)


def get_sio() -> socketio.AsyncServer:
    if __app is None:
        __init_socket()

    return __sio


def __init_socket():
    global __sio, __app

    __app = socketio.ASGIApp(__sio)
