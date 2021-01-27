import logging
import time

import workflows.frontend
import workflows.transport
import zocalo
from dlstbx.util.colorstreamhandler import ColorStreamHandler
from workflows.services.common_service import CommonService


class FakeNews(CommonService):
    """Service to investigate https://github.com/DiamondLightSource/python-workflows/issues/36"""

    # Human readable service name
    _service_name = "Fake News"

    # Logger name
    _logger_name = "services.fake_news"

    def initializing(self):
        t0 = time.perf_counter()
        N = 1000
        for i in range(N):
            self.log.info(f"FAKE NEWS #{i}")
        t1 = time.perf_counter()
        self.log.info(f"Time taken to log {N} FAKE NEWS messages: {t1 - t0} seconds")
        self._shutdown()


if __name__ == "__main__":
    logger = logging.getLogger()
    logger.setLevel(logging.WARN)

    # Enable logging to console
    console = ColorStreamHandler()
    console.setLevel(logging.INFO)
    logger.addHandler(console)

    logging.getLogger("dlstbx").setLevel(logging.DEBUG)
    logging.getLogger("workflows").setLevel(logging.DEBUG)
    logging.getLogger("zocalo").setLevel(logging.DEBUG)

    log = logging.getLogger("dlstbx.service")
    log.setLevel(logging.DEBUG)

    # Enable logging to graylog
    zocalo.enable_graylog()

    transport_factory = workflows.transport.lookup("StompTransport")
    transport_factory.load_configuration_file(
        "/dls_sw/apps/zocalo/secrets/credentials-live.cfg"
    )
    kwargs = {"service": FakeNews, "transport": transport_factory}

    # Create Frontend object
    frontend = workflows.frontend.Frontend(**kwargs)

    # Start Frontend
    try:
        frontend.run()
    except KeyboardInterrupt:
        print("\nShutdown via Ctrl+C")
