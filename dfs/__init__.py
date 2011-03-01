#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2007 Juan Vera del Campo <juanvi@entel.upc.es>
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#m=mdencrypter.encrypt(pmeta.save())
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#

__all__=['utils','filesystem','DHT','ring']

import logging
import utils
utils.configure_logging(level=logging.CRITICAL)
import DHT
import os

# If True, security is not actived at all. ONLY FOR DEVELOPMENT
NO_SECURITY = True

default_config_dir=os.path.expanduser('~%s.dfs'%os.path.sep)
""" The dir to use for configurations """
default_config_file=default_config_dir+os.path.sep+'dfsrc'
default_log_file=default_config_dir+os.path.sep+'dfs.log'
default_config=utils.Config()
""" The default dfs.utils.Config object. Although the configuration object is not mandatory, having a global configuration is really
advisable """
dht=None
""" The DHT to use. You MUST set up this variable before
using any object of the dfs.filesystem module """

def init_default_conf():	
	""" Sets up a default configuration for DFS: uses $HOME/.dfs as
	the config dir, reads the default configuration from
	$HOME/.dfs/dfsrc, and sets up the logging system to use the file
	dfs.log in the configuration directory """
	
	global default_config_dir, default_config, default_log_file, default_config_file, dht
	
	# Creates a directory to the default config, if it does not exists.
	default_config_dir=os.path.expanduser('~%s.dfs'%os.path.sep)
	# Create the default config path if it does not exists
	if not os.path.exists(default_config_dir): os.mkdir(default_config_dir)
	default_config_file=default_config_dir+os.path.sep+'dfsrc'
	default_log_file=default_config_dir+os.path.sep+'dfs.log'
	# Load the default config file
	if not os.path.exists(default_config_file): open(default_config_file,'w').close()
	default_config=utils.Config()
	default_config.load(open(default_config_file,'r'))
	
	# Configures the logging system
	utils.configure_logging(level=logging.INFO,
		format='%(asctime)s %(name)s %(levelname)s %(message)s',
		datefmt='%H:%M:%S',
		filename=default_log_file,
		filemode='w')	
	
	logging.info('Default configuration: %s'%default_config_file)
	
	# sets default configuration, if not set
	changed=False
	if not default_config.get('DHT:datadir'):
		default_config.set('DHT:datadir',default_config_dir+os.path.sep+'dhtdata')
		changed=True
	if not default_config.get('Main:UID'):
		default_config.set('Main:uid',utils.random_string(16))
		changes=True
	if not default_config.get('Main:nick'):
		default_config.set('Main:nick',utils.random_nick())
		changed=True
	if not default_config.get('Keys:kf'):
		logging.warning('There are not file key')
	if not default_config.get('Keys:kd'):
		logging.warning('There are not description key')
	if changed:
		default_config.save(open(default_config_file,'w'))
		
	# Default DHT: a local DHT
	dht=DHT.LocalDHT(default_config)
