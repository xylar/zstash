from __future__ import print_function, absolute_import

import argparse
import logging
import os.path
import sqlite3
import stat
import sys
from datetime import datetime
from .hpss import hpss_get, hpss_put
from .hpss_utils import add_files
from .settings import config, logger, CACHE, BLOCK_SIZE, DB_FILENAME, TIME_TOL
from .utils import exclude_files

def update():

    # Parser
    parser = argparse.ArgumentParser(
        usage='zstash update [<args>]',
        description='Update an existing zstash archive')
    required = parser.add_argument_group('required named arguments')
    optional = parser.add_argument_group('optional named arguments')
    optional.add_argument('--hpss', type=str, help='path to HPSS storage')
    optional.add_argument(
        '--exclude', type=str,
        help='comma separated list of file patterns to exclude')
    optional.add_argument(
        '--dry-run',
        help='dry run, only list files to be updated in archive',
        action="store_true")
    optional.add_argument(
        '--keep',
        help='keep tar files in local cache (default off)',
        action="store_true")
    optional.add_argument('-v', '--verbose', action="store_true", 
                          help="increase output verbosity")
    args = parser.parse_args(sys.argv[2:])
    if args.hpss and args.hpss.lower() == 'none':
        args.hpss = 'none'
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
    if args.hpss == 'none':
        # If no HPSS is available, always keep the files.
        config.keep = True
    else:
        config.keep = args.keep
    if args.hpss is not None:
        config.hpss = args.hpss

    # Start doing actual work
    logger.debug('Running zstash update')
    logger.debug('Local path : %s' % (config.path))
    logger.debug('HPSS path  : %s' % (config.hpss))
    logger.debug('Max size  : %i' % (config.maxsize))
    logger.debug('Keep local tar files  : %s' % (config.keep))

    # List of files
    logger.info('Gathering list of files to archive')
    files = []
    for root, dirnames, filenames in os.walk('.'):
        # Empty directory
        if not dirnames and not filenames:
            files.append((root, ''))
        # Loop over files
        for filename in filenames:
            files.append((root, filename))

    # Sort files by directories and filenames
    files = sorted(files, key=lambda x: (x[0], x[1]))

    # Relative file path, eliminating top level zstash directory
    files = [os.path.normpath(os.path.join(x[0], x[1]))
             for x in files if x[0] != os.path.join('.', CACHE)]

    # Eliminate files based on exclude pattern
    if args.exclude is not None:
        files = exclude_files(args.exclude, files)

    # Eliminate files that are already archived and up to date
    newfiles = []
    for file_path in files:

        statinfo = os.lstat(file_path)
        mdtime_new = datetime.utcfromtimestamp(statinfo.st_mtime)
        mode = statinfo.st_mode
        # For symbolic links or directories, size should be 0
        if stat.S_ISLNK(mode) or stat.S_ISDIR(mode):
            size_new = 0
        else:
            size_new = statinfo.st_size

        cur.execute(u"select * from files where name = ?", (file_path,))
        new = True
        while True:
            match = cur.fetchone()
            if match is None:
                break
            size = match[2]
            mdtime = match[3]

            if (size_new == size) \
              and (abs((mdtime_new-mdtime).total_seconds()) <= TIME_TOL):
                # File exists with same size and modification time within tolerance
                new = False
                break
            # print(file,size_new,size,mdtime_new,mdtime)
        if (new):
            newfiles.append(file_path)

    # Anything to do?
    if len(newfiles) == 0:
        logger.info('Nothing to update')
        return

    # --dry-run option
    if args.dry_run:
        print("List of files to be updated")
        for file_path in newfiles:
            print(file_path)
        return

    # Find last used tar archive
    itar = -1
    cur.execute(u"select distinct tar from files")
    tfiles = cur.fetchall()
    for tfile in tfiles:
        itar = max(itar, int(tfile[0][0:6], 16))

    # Add files
    failures = add_files(cur, con, itar, newfiles)

    # Close database and transfer to HPSS. Always keep local copy
    con.commit()
    con.close()
    hpss_put(config.hpss, DB_FILENAME, keep=True)

    # List failures
    if len(failures) > 0:
        logger.warning('Some files could not be archived')
        for file_path in failures:
            logger.error('Archiving %s' % (file_path))
