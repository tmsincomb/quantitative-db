import os
import logging


def makeSimpleLogger(name, level=logging.INFO):
    # TODO use extra ...
    logger = logging.getLogger(name)
    if logger.handlers:  # prevent multiple handlers
        return logger

    logger.setLevel(level)
    ch = logging.StreamHandler()  # FileHander goes to disk
    fmt = ('[%(asctime)s] - %(levelname)8s - '
           '%(name)14s - '
           '%(filename)16s:%(lineno)-4d - '
           '%(message)s')
    formatter = logging.Formatter(fmt)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


log = makeSimpleLogger('quantdb')
logd = log.getChild('data')


# from pyontutils.utils_fast import setPS1
def setPS1(script__file__):
    try:
        text = 'Running ' + os.path.basename(script__file__)
        os.sys.stdout.write('\x1b]2;{}\x07\n'.format(text))
    except AttributeError as e:
        log.exception(e)


