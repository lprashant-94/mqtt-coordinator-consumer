import logging

logger = logging.getLogger(__name__)


class GracefulKiller(object):
    """Listen for Ctrl-C or kill signal, and set variable kill_now."""

    kill_now = False

    def __init__(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s %(name)s: %(message)s')
        import signal
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        logger.warning(
            "Got termination signal from Terminal, Notifying to kill %r ", signum)
        self.kill_now = True
