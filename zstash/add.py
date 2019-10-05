from __future__ import print_function, absolute_import

import argparse
import logging
import os.path
import sqlite3
import stat
import sys
from datetime import datetime
from .hpss import hpss_get, hpss_put
from .utils import addfiles, excludeFiles
from .settings import config, logger, CACHE, BLOCK_SIZE, DB_FILENAME, TIME_TOL


def add():

    # Parser
    parser = argparse.ArgumentParser(
        usage='zstash add [<args>] files',
        description='Add specified files to an existing zstash archive')
    parser.add_argument('files', type=str, nargs='+', help='files to add to archive')
    required = parser.add_argument_group('required named arguments')
    optional = parser.add_argument_group('optional named arguments')
    optional.add_argument('--hpss', type=str, help='path to HPSS storage')
    optional.add_argument(
        '--keep',
        help='keep tar files in local cache (default off)',
        action="store_true")
    optional.add_argument('-v', '--verbose', action="store_true", 
                          help="increase output verbosity")
    args = parser.parse_args(sys.argv[2:])
    if args.verbose: logger.setLevel(logging.DEBUG)

    # Open database
    logger.debug('Opening index database')
    if not os.path.exists(DB_FILENAME):
        # will need to retrieve from HPSS
        if args.hpss is not None:
            config.hpss = args.hpss
            hpss_get(config.hpss, DB_FILENAME)
        else:
            logger.error('--hpss argument is required when local copy of '
                          'database is unavailable')
            raise Exception
    global con, cur
    con = sqlite3.connect(DB_FILENAME, detect_types=sqlite3.PARSE_DECLTYPES)
    cur = con.cursor()

    # Retrieve some configuration settings from database
    for attr in dir(config):
        value = getattr(config, attr)
        if not callable(value) and not attr.startswith("__"):
            cur.execute(u"select value from config where arg=?", (attr,))
            value = cur.fetchone()[0]
            setattr(config, attr, value)
    config.maxsize = int(config.maxsize)
    config.keep = bool(int(config.keep))

    # The command line arg should always have precedence
    config.keep = args.keep
    if args.hpss is not None:
        config.hpss = args.hpss

    # Start doing actual work
    logger.debug('Running zstash add')
    logger.debug('Local path : %s' % (config.path))
    logger.debug('HPSS path  : %s' % (config.hpss))
    logger.debug('Max size  : %i' % (config.maxsize))
    logger.debug('Keep local tar files  : %s' % (config.keep))

    # List of files
    files = args.files

    # Only keep files that exist
    # note: what should we do about
    # - symbolic links
    # - directories
    addfiles = []
    for fname in files:
        if os.path.isfile(fname):
            addfiles.append(fname)
        else:
            logger.debug('cannot add {}: does not exist or is not a file'.format(fname))

    # Anything to do?
    if len(addfiles) == 0:
        logger.info('Nothing to update')
        return
    logger.debug('List of files to archive {}'.format(addfiles))

    # Find last used tar archive
    itar = -1
    cur.execute(u"select distinct tar from files")
    tfiles = cur.fetchall()
    for tfile in tfiles:
        itar = max(itar, int(tfile[0][0:6], 16))

    # Add files
    failures = addfiles(cur, con, itar, addfiles)

    # Close database and transfer to HPSS. Always keep local copy
    con.commit()
    con.close()
    hpss_put(config.hpss, DB_FILENAME, keep=True)

    # List failures
    if len(failures) > 0:
        logger.warning('Some files could not be archived')
        for file in failures:
            logger.error('Archiving %s' % (file))

