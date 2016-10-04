# -*- coding: utf-8 -*-

import os

from appconf.log import get_configured_logger


#                                                            ### CONF LOADER ###

try:
    current.development
except NameError:
    from gluon import current
    from gluon.contrib.appconfig import AppConfig
    myconf = AppConfig(reload=False)
    current.development = False
else:
    if current.development:
        from appconf.conf import AppConfig
        myconf = AppConfig('appconfig-dev.ini', reload=True)
    else:
        from gluon.contrib.appconfig import AppConfig
        myconf = AppConfig(reload=False)

current.myconf = myconf

#                                                                 ### LOGGER ###

logger = get_configured_logger(request.application or "debug")
current.logger = logger

def lpath(p):
    """ Local path """
    if p.startswith("/"):
        mypath = p if not current.development else os.path.expanduser('~'+p)
    else:
        mypath = os.path.join(os.getcwd(), request.folder, p)
    if not os.path.exists(mypath):
        os.makedirs(mypath)
    return mypath

def boolean(v):
    v = v.strip()
    if v:
        # could be: 0 or 1
        if len(v)==1 and isdigit(v):
            return bool(int(v))
        # could be "true" or "false"
        if isinstance(v, basestring):
            import json
            return json.loads(v)
    # could be "foo" or None
    return bool(v)
