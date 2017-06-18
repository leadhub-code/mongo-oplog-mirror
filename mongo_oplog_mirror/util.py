import pymongo


def get_mongo_client(mongo_cfg, cfg_path):
    client = pymongo.MongoClient(
        mongo_cfg['uri'],
        connectTimeoutMS=3 * 1000,
        serverSelectionTimeoutMS=5 * 1000,
        connect=True)
    return client
