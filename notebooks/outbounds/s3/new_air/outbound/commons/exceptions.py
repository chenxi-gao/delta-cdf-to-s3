class SyncFailure(Exception):
    def __init__(self, message, errors=None):
        super().__init__(message)
        self.errors = errors


class SchemaNotFoundError(Exception):
    def __init__(self, message, errors=None):
        super().__init__(message)
        self.errors = errors


class TableException(Exception):
    def __init__(self, message, errors=None):
        super().__init__(message)
        self.errors = errors


class DatabaseOperationException(Exception):
    def __init__(self, message, errors=None):
        super().__init__(message)
        self.errors = errors


class CDCReadException(Exception):
    def __init__(self, message, errors=None):
        super().__init__(message)
        self.errors = errors
