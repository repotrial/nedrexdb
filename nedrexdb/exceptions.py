class NeDRexError(Exception):
    pass


class AssumptionError(NeDRexError):
    pass


class ConfigError(NeDRexError):
    pass


class MongoDBError(NeDRexError):
    pass


class ProcessError(NeDRexError):
    pass
