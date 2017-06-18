import argparse
from base64 import b64encode
import bson
from datetime import datetime
import logging
from pathlib import Path
import pymongo
import re
import simplejson as json
from time import sleep
from uuid import UUID
import yaml

from .util import get_mongo_client


logger = logging.getLogger(__name__)

dump_file_size_limit = 100 * 2**20


def dump_oplog_main():
    p = argparse.ArgumentParser()
    p.add_argument('conf_file')
    args = p.parse_args()

    cfg_file = Path(args.conf_file).resolve()
    cfg_path = cfg_file.parent
    with cfg_file.open() as f:
        cfg = yaml.safe_load(f.read())

    setup_logging()

    client = get_mongo_client(cfg['src_mongo'], cfg_path)
    logger.debug('client: %s', client)

    dump_path = cfg_path / cfg['oplog_dump']['path']
    dump_prefix = cfg['oplog_dump']['file_name_prefix']

    c_oplog = client['local']['oplog.rs']


    dump_files = {}
    for p in dump_path.iterdir():
        if p.name.startswith(dump_prefix):
            number = p.name[len(dump_prefix):]
            assert re.match(r'^[0-9]+$', number), repr(number)
            number = int(number)
            dump_files[number] = p

    latest_ts = None
    for n, p in reversed(sorted(dump_files.items())):
        latest_ts = read_latest_ts(p)
        if latest_ts is not None:
            break

    if latest_ts:
        logger.debug('latest_ts: %r - %s', latest_ts, datetime.utcfromtimestamp(latest_ts[0]))

    latest_number = sorted(dump_files.keys())[-1] if dump_files else -1
    current_number = latest_number + 1
    current_file = None

    q = {'ts': {'$gte': bson.Timestamp(*(latest_ts or [0, 0]))}}
    logger.debug('q: %r', q)
    oplog_cursor = c_oplog.find(q, cursor_type=pymongo.CursorType.TAILABLE, oplog_replay=True)
    first = True
    try:
        while oplog_cursor.alive:
            for doc in oplog_cursor:
                try:
                    if first:
                        if latest_ts:
                            if latest_ts != [doc['ts'].time, doc['ts'].inc]:
                                logger.warning('oplog ts %r != latest_ts %r', doc['ts'], latest_ts)
                        else:
                            logger.info('oplog starts at %r - %s', doc['ts'], datetime.utcfromtimestamp(doc['ts'].time))
                        first = False
                    row = {
                        'ts': [doc['ts'].time, doc['ts'].inc],
                        'op': doc['op'],
                        'ns': doc.get('ns'),
                    }
                    if doc['op'] in ('n', 'c'):
                        row['o'] = to_json(doc['o'])
                        row['raw'] = to_json(doc)
                    elif doc['op'] in ('i', 'd'):
                        row['_id'] = to_json(doc['o']['_id'])
                    elif doc['op'] == 'u':
                        row['_id'] = to_json(doc['o2']['_id'])
                    else:
                        raise Exception('Unknown op {!r}'.format(doc['op']))
                    row_json = json.dumps(row, sort_keys=True)
                    assert '\n' not in row_json
                    if current_file and current_file.tell() > dump_file_size_limit:
                        logger.debug('File size limit reached, closing %s', current_file)
                        current_file.close()
                        current_file = None
                        current_number += 1
                    if current_file is None:
                        current_file_path = dump_path / (dump_prefix + '{:09d}'.format(current_number))
                        logger.debug('Writing into %s', current_file_path)
                        current_file = current_file_path.open('x')
                    current_file.write(row_json + '\n')
                except Exception as e:
                    raise Exception('Failed to process oplog doc {!r}: {!r}'.format(doc, e)) from e
            current_file.flush()
            sleep(0.1)
    finally:
        if current_file:
            current_file.close()


def to_json(obj):
    if isinstance(obj, dict):
        return {k: to_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_json(v) for v in obj]
    elif isinstance(obj, bson.ObjectId):
        return {'$oid': str(obj)}
    elif isinstance(obj, UUID):
        return {'$uuid': str(obj)}
    elif isinstance(obj, bytes):
        return {'$base64': b64encode(obj).decode()}
    elif isinstance(obj, bson.Timestamp):
        return {'$ts': [obj.time, obj.inc]}
    else:
        return obj



def read_latest_ts(dump_file):
    latest_ts = None
    with dump_file.open() as f:
        for line in f:
            line = line.rstrip()
            if line:
                if not line.startswith('{') or not line.endswith('}'):
                    raise Exception('Unknown line {!r} in file {}'.format(line, dump_file))
                row = json.loads(line)
                latest_ts = row['ts']
    return latest_ts


def setup_logging():
    logging.basicConfig(
        format='%(asctime)s %(name)s %(levelname)5s: %(message)s',
        level=logging.DEBUG)
