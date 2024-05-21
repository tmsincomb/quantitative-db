class QuantdbError(Exception):
    """base"""


class UnknownArg(QuantdbError):
    """url query parameter unknown"""
