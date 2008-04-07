#!/usr/bin/python
# -*- coding: utf-8 -*-
# 
# Several implementations of Hashtables: memory, file and DHT
# Copyright (C) 2007 Juan Vera del Campo, juanvi@entel.upc.edu
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

import logging
import os
import dfs, utils
import xmlrpclib
import ring
from base64 import b32encode, b64encode

logger=logging.getLogger('DHT')

try:
	from Crypto.Hash import SHA
	SECURED=False
except:
	from sha import sha
	SECURED=True
	logger.warn('Not secured')

def get_new_hasher(initial_data=''):
	if SECURED:
		return SHA.new(initial_data)
	else:
		return sha(initial_data)

class MemoryDHT:
	""" Manages a local DHT in memory """
	BLOCK_SIZE=1024
	def __init__(self):
		self.hashtable=dict()
		logger.info('MemoryDHT ready')
        
	def put(self,id,data,key='default'):
		""" Puts a value into the hashtable.
		
		>>> dht=LocalDHT()
		>>> dht.put('123','data')
		"""
		id=b64encode(id)
		logger.info('Putting %s in %s (key=%s)'%(data,id,key))
		try:
			self.hashtable[id][key]=data
		except KeyError:
			self.hashtable[id]=dict({key:data})

	def get(self,id,key='default'):
		""" Gets a value from the hashtable.
		
		>>> dht=LocalDHT()
		>>> dht.put('123','data')
		>>> dht.get('123')
		'data'
		>>> dht.get('456')
		>>>
		"""
		id=b64encode(id)
		logger.info('Getting value for %s (key=%s)'%(id,key))       
		try:
			return self.hashtable[id][key]
		except KeyError:
			return None            

class LocalDHT:
	""" This class manages a local, persistent DHT """
	BLOCK_SIZE=1024
	def __init__(self,config=None):
		""" Creates a persistent DHT. If no 'DHT:datadir' key in
		config, use default dir './dhtdata' to save the data """
		if not config: config=dfs.default_config
		self.dirpath='dhtdata'
		if config:
			self.dirpath=config.get('DHT:datadir','dhtdata')
		if not os.path.isdir(self.dirpath):
			logger.info('Creating dir for DHT: %s'%self.dirpath)
			os.mkdir(self.dirpath)
		logger.info('LocalDHT ready')   
        
	def put(self,id,data,key=None):
		""" Puts a value into the hashtable.
		Returns 0 if OK, or 1 if there were errors (not saved!)
		
		>>> dht=LocalDHT()
		>>> dht.put('123','data')
		"""
		if not key: key='default'
		id=b32encode(id)
		logger.debug('Putting in %s (key=%s)'%(id,key))
		
		filename=self.dirpath+os.sep+'%s-%s'%(id,key)
		try:
			f=open(filename,'w')
			f.write(data)
			f.close()
			return 0
		except IOError, e:
			logger.error('Error writing file: '+e.message)
			return 1

	def get(self,id,key=None):		
		""" Gets a value from the hashtable.
		Returns None if no value.
		
		>>> dht=LocalDHT()
		>>> dht.put('123','data')
		>>> dht.get('123')
		'data'
		>>> dht.get('456')
		>>>
		"""
		if not key: key='default'
		id=b32encode(id)
		logger.debug('Getting value for %s (key=%s)'%(id,key))
		
		filename=self.dirpath+os.sep+'%s-%s'%(id,key)
		try:
			return open(filename,'r').read()
		except IOError,e:
			logger.warn('Error reading file: '+e.message)
			return None

class OpenDHT:
	""" An external DHT that connects to OpenDHT """
	BLOCK_SIZE=1024
	def __init__(self,gateway="http://opendht.nyuld.net:5851/"):
		""" Connects to the OpenDHT server. Get a list in http://opendht.org/servers.txt """
		import xmlrpclib
		logger.info('Connecting with OpenDHT in '+gateway)
		self.pxy=xmlrpclib.ServerProxy(gateway)
	def put(self,id,data,key='default'):
		try:
			res=2
			while res==2:
				res=self.pxy.put(xmlrpclib.Binary(id),xmlrpclib.Binary(data),600000,'dfs')
				if res==1: raise OverflowError, 'No capacity'
		except:
			return None
	def get(self,id,key='default'):
		try:
			vals,pm=self.pxy.get(xmlrpclib.Binary(id),1,xmlrpclib.Binary(''),'dfs')
			return vals[0].data
		except:
			return None

class NetClientDHT:
	""" This class manages a DHT in a remote ring as a client. This one
	if the class that an application of the ring will use """
	BLOCK_SIZE=1024
	def __init__(self,server='http://localhost:8080'):
		""" Connects to a node in the ring.
		server is the address of the node. If None, use 'http://localhost:8080 """
		logger.info('Using NetServerDHT at '+server)
		self.ring=xmlrpclib.ServerProxy(server)

	def __hash(self,data):
		""" Returns a string with a digital integer that represents a 16B hash of the ID """
		# if data has 16B and it is a number, use the data itself
		# as identifier (assume that callers know what they are doing)
		if len(data)!=16:
			h=get_new_hashser(data).digest()[0:16]
		else:
			h=data
		dh=0
		for i in range(0,16):
			dh+=pow(256,i)*ord(h[i])
		return str(dh)
        
	def put(self,id,data,key=None):
		""" Puts a value into the hashtable.
		id is the identifier of the value (any string)
		data is the data to save
		key is an optional key, to cover collisions in id
		Returns 0 if OK, 1 otherwise """
		if not key: key='default'
		logger.debug('Net-putting in %s (key=%s)'%(id,key))
		h=self.__hash(id)
		return self.ring.msg(h,'PUT',key,xmlrpclib.Binary(data))

	def get(self,id,key=None):
		""" Gets a value from the hashtable. """
		if not key: key='default'
		logger.debug('Net-getting from %s (key=%s)'%(id,key))
		h=self.__hash(id)
		try:
			return self.ring.msg(h,'GET',key).data
		except:
			return None

class NetServerDHT(ring.RingListener):
	""" The server of a net DHT. It is set on top of a ring. The ring
	is intended to be run as a daemon of the system. """
	def __init__(self,ring,config=None):
		""" Creates a new NetServerDHT. This server uses a
		internal LocalDHT to manage the values of the DHT.
		ring of the network. If not joined to the network, joins the node.
		config to use (if None, use the config of the ring)
		"""
		if not config: config=ring.config
		self.localdht=LocalDHT(config)
		ring.listener=self
		if not ring.joined: ring.start()
		logger.info('NetServerDHT ready')
	def message(self,to,*args):
		""" Receives a message from the ring """
		try:
			if args[0]=='GET':
				return self.__get(to,args[1])
			elif args[0]=='PUT':
				return self.__put(to,args[1],args[2])
			else:
				return 'No such method: %s'%args[0]
		except:
			return utils.format_error()
	def __get(self,id,key):
		""" Manages a GET message """
		return xmlrpclib.Binary(self.localdht.get(id,key))
	def __put(self,id,key,data):
		""" Manages a PUT message """
		try:
			return self.localdht.put(id,data.data,key)
		except:
			logger.warn(utils.format_error)
			return 1
