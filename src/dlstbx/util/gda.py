from __future__ import annotations

import socket


def notify(host, port, message):
    port = int(port)

    UDPSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    UDPSock.settimeout(2.0)
    UDPSock.sendto(message, (host, port))
    UDPSock.close()
