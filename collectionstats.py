#! /usr/bin/python

#Collection-specific mongodb statistics.

#Usage: collectionstats.py [-h] [--host HOST] [--port PORT] [--interval INT] [-s]
#                      [--size MAXSIZE] [-m] [--max-objects NUM]  database
#
#Positional arguments:
#  database             The database to connect to, e.g. `test`.
#
#Optional arguments:
#  -h, --help           show this help message and exit
#  --host HOST          Hostname (or IP) of the MongoDB database to connect to.
#                       Defaults to localhost.
#  --port PORT          Port of the MongoDB server. Defaults to 27017.
#  --size SIZE          Maximum size of the profiling collection, in bytes.
#                       Defaults to 5242880.
#  --max-objects NUM    Max # of documents to hold in profiling collection.
#                       Defaults to 100000.
#  --interval INTERVAL  Number of seconds to turn profiling on.
#                       Defaults to five (5) seconds.

import pymongo
import time

from optparse import OptionParser
from bson.code import Code
from texttable import Texttable

MAP = Code("function() {"
    "var info = this.info;"
    "var matches = info.match(/^(\w+)+\s+([\w\-]+)\.([\w\-\.\$]+)/);"
    "if (matches && matches.length === 4) {"
        "var type = matches[1];"
        "var database = matches[2];"
        "var collection = matches[3];"

        "if (collection !== '$cmd' && database !== 'tmp'"
           " && database !== 'system') {"
           "emit({"
                "\"collection\": collection, \"type\": type}, {\"count\": 1}"
           ");"
        "}"
    "}"
"}")

REDUCE = Code("function(key, values) {"
    "var count = 0;"
    "values.forEach(function(v) {"
        "count += v['count'];"
    "});"
    "return {count: count};"
"}")

PROFILE_COLLECTION = 'system.profile'


def connect(database, host="localhost", port=27017):
    """
    Connects to the database & return the Mongo Database object.

    Arguments:
        database -- The database to connect to.
        host     -- The hostname to connect to.
        port     -- The port to connect to.

    Returns a pymongo database object with the specified configurations.

    """
    connection = pymongo.Connection(host, port)
    return connection[database]


def reset_profiling_collection(db, size=10485760, max=10000,
                               name='system.profile'):
    """
    Removes current system.profile collection & creates a new one.

    Arguments:
        db -- The database object.
        size -- Maximum size of capped collection, in bytes.
                Defaults to 10485760 (5Mb)
        max -- Maximum number of documents that the collection will accept
        before rolling over. Defaults to 10000.
        name -- The name of the profiling collection.

    """

    db.drop_collection(name)
    db.create_collection(name, capped=True, size=size, max=max)


def parse_results(mr_data):
    """
    Parse profiling information stored in collection.

    This is the meat of the collection-level mongo statistics.

    Arguements:
        mr_data -- the mapreduce results to be processed.

    Returns: Dict of parsed results, ready to be output with `print_table()`.

    """

    results = {}

    for doc in mr_data:
        collection = doc['_id']['collection']
        type = doc['_id']['type']
        value = doc['value']['count']

        if collection not in results:
            results.setdefault(collection, {type: value})
        else:
            if type not in results[collection]:
                results[collection].setdefault(type, value)
            else:
                results[collection][type] = results[collection][type] + value
    return results


def print_table(data, time_interval, max_width=100):
    """
    Print ASCII table of results.

    Arguments:
        data          -- The map/reduce data to be tabulated.
        time_interval -- The time interval to normalize the results with.
        max_width     -- Maximum width (in cols) that the table can occupy.

    Returns: Null

    """
    table = Texttable(max_width)
    table.header(['collection', 'queries/sec', 'inserts/sec', 'getmores/sec',
                  'updates/sec', 'removes/sec'])
    norm = float(time_interval)

    for col, v in parse_results(data).items():
        v.setdefault('query', 0)
        v.setdefault('insert', 0)
        v.setdefault('getmore', 0)
        v.setdefault('update', 0)
        v.setdefault('remove', 0)
        table.add_row([col, v['query'] / norm, v['insert'] / norm,
                       v['getmore'] / norm, v['update'] / norm,
                       v['remove'] / norm])

    print table.draw()


if __name__ == "__main__":
    usage = 'collectionstats.py [-h] [--host HOST] [--port PORT] [--interval INTERVAL] database'
    parser = OptionParser(usage=usage)
    parser.add_option('-o', '--host', dest='host', default='locahost',
                      help='Hostname (or IP) of the MongoDB database to connect to. Defaults to localhost.')
    parser.add_option('-p', '--port', dest='port', type="int", default=27017,
                      help='Port of the MongoDB server. Defaults to 27017.')
    parser.add_option('-i', '--interval', dest='interval', type="int", default=5,
                      help='Number of seconds to turn profiling on. Defaults to five (5) seconds.')
    parser.add_option('-s', '--size', dest='size', type="int", default=10485760,
                      help='Maximum size of the profiling collection, in bytes. Defaults to 10485760')
    parser.add_option('-m', '--max-objects', dest='maxobjects', type="int",
                      default=10000,
                      help='Maximum number of documents to store in the profiling collection. Defaults to 10000.')
    (options, args) = parser.parse_args()

    db = connect(args[0], host=options.host, port=options.port)

    # Get current profiling level
    profiling_level = db.profiling_level()
    print "Current profiling level is: %d" % profiling_level

    reset_profiling_collection(db, size=options.size, max=options.maxobjects)
    db.set_profiling_level(pymongo.ALL)

    print "Changing profiling level to %d ... done." % pymongo.ALL
    print "Data collection starting - will stop in %d seconds" % options.interval

    time.sleep(options.interval)

    # Reset profiling level to what it was originally
    count = db[PROFILE_COLLECTION].count()
    print "Done! Collected %d data profiling points." % count
    print "Reverting back to original profile level of %d... done." % profiling_level
    db.set_profiling_level(profiling_level)

    if (count >= options.maxobjects):
        print "\n[WARNING] You have collected more documents than the profiling collection can handle."
        print "Please consider increasing --size and --max-objects."
        print "\n"

    print "Beginning map/reduce..."
    mr_result = db[PROFILE_COLLECTION].inline_map_reduce(MAP, REDUCE)
    print "Done!"

    print_table(mr_result, options.interval)
