"""Receiving system signal
"""
# coding: utf-8
import signal
import logging
from signalsdb.api import explain
lg = logging.getLogger(__name__)


class ReceiveSignal(object):
    EXIT_FLAG = False             # Exit flag
    EXIT_SIGN = 0                 # Exit signal
    SIGN_HANDLE = list()          # Need to deal with the signal
    SIGN_IGNORE = list()          # Neglected signal
    SIGN_FUNC = None              # Function to process the signal

    @staticmethod
    def signal_handler(sig, frame):
        """Receive system signal
        :param sig: Signal value
        :param frame:
        :return: None
        """
        # Get signal info
        sig_info = explain(sig)

        # Ignore signal processing
        if sig in ReceiveSignal.SIGN_IGNORE:
            sig_info["action"] = "ignore"
            lg.warning("Received system signal: %s, at frame: %s" %
                       (sig_info, frame.f_code))

        # Stop the process of the signal
        if sig in ReceiveSignal.SIGN_HANDLE:
            sig_info["action"] = "stop"
            # Exit process
            # Receive Signal
            ReceiveSignal.EXIT_FLAG = True
            ReceiveSignal.EXIT_SIGN = sig
            lg.warning("Received system signal: %s, at frame: %s, stop program..." %
                       (sig_info, frame.f_code))

            # Execute stop function
            if ReceiveSignal.SIGN_FUNC:
                lg.info("Execute stop function '%s'" %
                        ReceiveSignal.SIGN_FUNC.__name__)
                ReceiveSignal.SIGN_FUNC()

    @classmethod
    def receive_signal(cls, handle=(2, 15), ignore=(1,), func=None):
        """Receive the Linux signal
        """
        # Check the signal processing
        if not isinstance(handle, list) and not isinstance(handle, tuple):
            raise ValueError("The handle '%s' is not a list or tuple" % handle)
        for sig in handle:
            result = explain(sig)
            signal_name = getattr(signal, result["signal"])
            signal.signal(signal_name, ReceiveSignal.signal_handler)
        ReceiveSignal.SIGN_HANDLE = handle

        # Check ignore signal
        if not isinstance(ignore, list) and not isinstance(ignore, tuple):
            raise ValueError("The ignore '%s' is not a list or tuple" % ignore)
        for sig in ignore:
            result = explain(sig)
            signal_name = getattr(signal, result["signal"])
            signal.signal(signal_name, ReceiveSignal.signal_handler)
        ReceiveSignal.SIGN_IGNORE = ignore

        # Check function
        if func:
            result = callable(func)
            if not result:
                raise AttributeError("Function '%s' can not be called" % func)
            ReceiveSignal.SIGN_FUNC = func
        if ReceiveSignal.SIGN_FUNC:
            lg.info("Receive system signals, handle signals %s, ignore "
                    "signals %s, signal processing function '%s'" %
                    (handle, ignore, func.__name__))
        else:
            lg.info("Receive system signals, handle signals %s, ignore "
                    "signals %s, signal processing function '%s'" %
                    (handle, ignore, func))
