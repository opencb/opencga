__author__ = 'antonior'


class LoginException(Exception):
    def __str__(self):
        return repr("Permission Violation. Please use the Login Tool, before to perform this action")


class ServerResponseException(Exception):
    def __init__(self, value):
        self.e = value

    def __str__(self):
        return repr(self.e)


class FileAlreadyExists(Exception):
    def __init__(self, value):
        self.e = value

    def __str__(self):
        return repr(self.e)


class WSErrorException(Exception):
    def __init__(self, value):
        self.e = value

    def __str__(self):
        return repr(self.e)
