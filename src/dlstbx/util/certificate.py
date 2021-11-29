from __future__ import annotations

import socket
from contextlib import closing
from datetime import datetime
from typing import Optional

import idna
from cryptography import x509
from OpenSSL import SSL


def problems_with_certificate(hostname: str) -> Optional[str]:
    try:
        with closing(socket.socket()) as sock:
            sock.connect((hostname, 443))
            ctx = SSL.Context(SSL.SSLv23_METHOD)  # most compatible
            ctx.check_hostname = False
            ctx.verify_mode = SSL.VERIFY_NONE

            with closing(SSL.Connection(ctx, sock)) as sock_ssl:
                sock_ssl.set_connect_state()
                sock_ssl.set_tlsext_host_name(idna.encode(hostname))
                sock_ssl.do_handshake()
                ssl_cert = sock_ssl.get_peer_certificate()
        cert = ssl_cert.to_cryptography()
        if cert.not_valid_before > datetime.now():
            return "Certificate is not yet valid"
        if cert.not_valid_after < datetime.now():
            return "Certificate has expired"
        expiration = cert.not_valid_after - datetime.now()
        if expiration.days > 0 and expiration.days < 14:
            return f"Certificate will expire in {expiration.days} days"
        elif expiration.days < 14:
            return f"Certificate will expire in {expiration.total_seconds() / 60:.0f} minutes"

        ext = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName)
        cert_names = ext.value.get_values_for_type(x509.DNSName)
        if hostname in cert_names:
            # Exact name match - certificate is valid
            return None
        # Check for wildcard matches
        for allowed_name in cert_names:
            if allowed_name.startswith("*.") and hostname.endswith(allowed_name[1:]):
                return None
        return f"Certificate does not cover {hostname!r}"
    except socket.gaierror as e:
        return f"Socket error: {e.strerror}"
    except ConnectionError as e:
        return f"Connection error: {e.strerror}"
