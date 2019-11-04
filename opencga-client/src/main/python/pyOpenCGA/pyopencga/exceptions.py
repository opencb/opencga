class OpenCgaAuthorisationError(Exception):
    def __init__(self, message):
        super(OpenCgaAuthorisationError, self).__init__(message)

class OpenCgaInvalidToken(Exception):
    def __init__(self, message):
        super(OpenCgaInvalidToken, self).__init__(message)