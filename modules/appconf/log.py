#!/usr/bin/env python
# -*- coding: utf-8 -*-
from gluon import *
from gluon import current

import os, logging, logging.handlers


def get_configured_logger(name):
    """ Courtesy of: http://www.web2pyslices.com/slice/show/1416/logging
    """
    logger = logging.getLogger(name)
    if (len(logger.handlers) == 0):
        # This logger has no handlers, so we can assume it hasn't yet been configured
        # (Configure logger)
        # Create RotatingFileHandler
        # Alternatively we can think to use other handler such as:
        # SQLiteHandler (https://github.com/amka/SQLiteHandler/blob/master/sqlite_handler.py)
        formatter="%(asctime)s %(levelname)s %(process)s %(thread)s %(funcName)s():%(lineno)d %(message)s"
        handler = logging.handlers.RotatingFileHandler(
            os.path.join(current.request.folder, 'private', current.request.application+'.log'),
            maxBytes = 5*2**20,
            backupCount = 2
        )
        handler.setFormatter(logging.Formatter(formatter))
        logging_level = logging.DEBUG if current.development else logging.INFO
        handler.setLevel(logging_level)
        logger.addHandler(handler)
        logger.setLevel(logging_level)

        # Test entry:
        if current.development:
            logger.debug(name + ' logger created')
    else:
        # Test entry:
        if current.development:
            logger.debug(name + ' already exists')

    return logger
