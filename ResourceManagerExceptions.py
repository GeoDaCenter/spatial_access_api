class ResourceManagerBaseException(BaseException):
    """Base exception for ResourceManager"""
    pass


class ResourceDoesNotExistException(ResourceManagerBaseException):
    """Resource does not exist"""
    pass


class UnrecognizedJobTypeException(ResourceManagerBaseException):
    """Unknown job type"""
    pass


class MissingHintsException(ResourceManagerBaseException):
    """Missing hints"""
    pass


class MissingColumnNamesException(ResourceManagerBaseException):
    """Missing hints"""
    pass


class MissingResourceException(ResourceManagerBaseException):
    """Missing resource"""
    pass
