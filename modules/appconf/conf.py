#!/usr/bin/env python
# -*- coding: utf-8 -*-
from gluon import *

"""

#################################################################################
Courtesy of:
https://raw.githubusercontent.com/web2py/web2py/master/gluon/contrib/appconfig.py
#################################################################################

Read from configuration files easily without hurting performances

USAGE:
During development you can load a config file either in .ini or .json
format (by default app/private/appconfig.ini or app/private/appconfig.json)
The result is a dict holding the configured values. Passing reload=True
is meant only for development: in production, leave reload to False and all
values will be cached

from gluon.contrib.appconfig import AppConfig
myconfig = AppConfig(path_to_configfile, reload=False)

print myconfig['db']['uri']

The returned dict can walk with "dot notation" an arbitrarely nested dict

print myconfig.take('db.uri')

You can even pass a cast function, i.e.

print myconfig.take('auth.expiration', cast=int)

Once the value has been fetched (and casted) it won't change until the process
is restarted (or reload=True is passed).

"""

import thread
import os
from ConfigParser import SafeConfigParser
from gluon import current
from gluon.serializers import json_parser

locker = thread.allocate_lock()

def nested_update(d, u):
    """ Courtesy of:
    http://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth
    """
    for k, v in u.iteritems():
        if isinstance(v, dict):
            r = nested_update(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d

def AppConfig(*args, **vars):

    locker.acquire()
    reload_ = vars.pop('reload', False)
    try:
        instance_name = 'AppConfig_' + current.request.application
        if reload_ or not hasattr(AppConfig, instance_name):
            setattr(AppConfig, instance_name, AppConfigLoader(*args, **vars))
        return getattr(AppConfig, instance_name).settings
    finally:
        locker.release()


class AppConfigDict(dict):
    """
    dict that has a .take() method to fetch nested values and puts
    them into cache
    """

    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)
        self.int_cache = {}

    def take(self, path, cast=None, default=None):
        parts = path.split('.')
        if path in self.int_cache:
            return self.int_cache[path]
        value = self
        walking = []
        for part in parts:
            if part not in value:
#                 raise BaseException("%s not in config [%s]" %
#                     (part, '-->'.join(walking)))
                value = default
            else:
                value = value[part]
            walking.append(part)
        if cast is None:
            self.int_cache[path] = value
        else:
            try:
                value = cast(value)
                self.int_cache[path] = value
            except (ValueError, TypeError):
                raise BaseException("%s can't be converted to %s" %
                 (value, cast))
        return value


class AppConfigLoader(object):

    priv_folder = os.path.join(current.request.folder, 'private')

    def __init__(self, *configfiles):

        defaultconfigfile = os.path.join(self.priv_folder, 'appconfig.ini')
        if not os.path.isfile(defaultconfigfile):
            defaultconfigfile = os.path.join(self.priv_folder, 'appconfig.json')
        configfiles = (defaultconfigfile, ) + configfiles

        self.files =  configfiles
        self.settings = None
        self.read_config()

    def read_config_ini(self, configfile):
        config = SafeConfigParser()
        config.read(configfile)
        settings = {}
        for section in config.sections():
            settings[section] = {}
            for option in config.options(section):
                settings[section][option] = config.get(section, option)
        if self.settings is None:
            self.settings = AppConfigDict(settings)
        else:
            self.settings = nested_update(self.settings, AppConfigDict(settings))

    def read_config_json(self, configfile):
        if os.path.isfile(configfile):
            with open(self.file, 'r') as c:
                settings = AppConfigDict(json_parser.load(c))
        else:
            settings = {}
        if self.settings is None:
            self.settings = settings
        else:
            self.settings = nested_update(self.settings, settings)

    def read_config(self):
        if self.settings is None:
            for configfile in self.files:
                # I guess it's in private folder
                if not os.path.isfile(configfile):
                    configfile = os.path.join(self.priv_folder, configfile)
                ctype = os.path.splitext(configfile)[1][1:]
                try:
                    getattr(self, 'read_config_' + ctype)(configfile)
                except AttributeError:
                    raise BaseException("Unsupported config file format")
            return self.settings
