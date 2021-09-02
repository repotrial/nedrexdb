class MongoMixin:
    @classmethod
    def find(cls, db, query=None):
        if query is None:
            query = {}
        return db[cls.collection_name].find(query)

    @classmethod
    def find_one(cls, db, query=None):
        if query is None:
            query = {}
        return db[cls.collection_name].find_one(query)
