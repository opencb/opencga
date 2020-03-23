class OpencgaAuthorisationError(Exception):
    def __init__(self, message):
        super(OpencgaAuthorisationError, self).__init__(message)


class OpencgaInvalidToken(Exception):
    def __init__(self, message):
        super(OpencgaInvalidToken, self).__init__(message)
