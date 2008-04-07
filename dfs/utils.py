#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Several util functions for the method
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
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#

import ConfigParser
import os
import random
import base64
try:
	from Crypto.Cipher import AES
	SECURED=True
except:
	SECURED=False

class Config:
	def __init__(self,key=None):
		""" Initializes the class. The optional key parameter
		is used to encrypt some important data """
		self.config=ConfigParser.SafeConfigParser()
		self.key=key
	def get(self,key,default=None):
		""" Gets a value from the configuration file.
		key Is the key to the property with the colon-separated section.
		default is the default value of the property if it is not in the configuration.
		
		>>> config=Config()
		>>> config.set('Main:Fruit','apple')
		>>> config.get('Main:Fruit')
		'apple'
		
		"""
		try:
			(section,property)=key.split(':')
		except:
			(section,property)=(ConfigParser.DEFAULTSECT,key)
		try:
			return self.config.get(section,property)
		except:
			return default
	def getbool(self,key,default=False):
		""" A convenience method to get a boolean
		
		>>> config=Config()
		>>> config.set('Main:Save',False)
		>>> config.getbool('Main:Save')
		False
		
		"""
		if not default:
			default = 'false'
		else:
			default = 'true'
		return not (self.get(key,default).lower()=='false')
	def getint(self,key,default=0):
		""" A convenience method to get an integer
		 
		>>> config=Config()
		>>> config.set('Main:Apples',3)
		>>> config.getint('Main:Apples')
		3
		 
		"""
		try:
			return int(self.get(key,str(default)))
		except:
			return default
	def set(self,key,value):
		""" Sets a property.
		key Is the key to the property with the colon-separated section. If there is no colon,
			the property will be saved in the default section
		value is the value of the property. Currently, it can be a string, an integer or a boolean.
		This method returns a reference to the Config object, in order to
		easily chain configurations.
		
		>>> config=Config()
		>>> config.set('Main:Name','Jesse James')
		>>> config.get('Main:Name')
		'Jesse James'
		
		"""
		
		# Get the section and property, or use the DEFAULT section
		try:
			(section,property)=key.split(':')
		except:
			(section,property)=(ConfigParser.DEFAULTSECT,key)
		# convert integers and booleans into strings
		if not value: value=''
		if type(value)==int: value='%d'%value
		if type(value)==bool:
			value='true'
			if not value: value='false'
		# create the section, if not pressent
		if not self.config.has_section(section):
			self.config.add_section(section)
		# save the value
		self.config.set(section,property,value)
		# return the same object (to chain .set() statements)
		return self
	def load(self,source):
		""" Loads the configuration from a source.
		If source is a file object, loads the configuration from the file.
		Else, it is managed as a string and reads configuration from string """
		if type(source)==str:
			import StringIO
			self.config.readfp(StringIO.StringIO(source))
		else:
			self.config.readfp(source)
	def save(self,fileobject=None):
		""" Saves the content in a destination.
		If a fileobject is provided, configuration is saved in the file and returns None.
		Otherwise, the configuration is returned as a string """		
		if fileobject:
			self.config.write(fileobject)
			return None
		else:
			import StringIO
			s=StringIO.StringIO()
			self.config.write(s)
			return s.getvalue()
	def get_key(self,id,enc_key=None):
		""" A convenience method to get a key from the config file.
		If enc_key is provided, the key are decrypted with AES.
		If enc_key is None, use the key provided during the
		initialization of this class. """
		if not enc_key: enc_key=self.key
		kf = None
		if enc_key:
			kf=self.get('Keys:'+id,None)
			if kf: kf=AES.new(enc_key).decrypt(base64.b32decode(kf))
		else:
			kf=self.get('Keys:'+id,None)
			if kf: kf=base64.b32decode(kf)
		return kf
	def set_key(self,id,key,enc_key=None):
		""" A convenience method to set a key.
		If enc_key is provided, the key are crypted with AES. Else,
		the key are just Base32 encoded. If enc_key is None, use
		the key provided during the initialization of this class """
		if not key: return
		if not enc_key: enc_key=self.key
		if enc_key:
			self.set('Keys:'+id,base64.b32encode(AES.new(key).encrypt(key)))
		else:
			self.set('Keys:'+id,base64.b32encode(key))

def format_error():
	""" Returns a formated string with information about the last error """
	import traceback, sys
	ei=sys.exc_info()
	fn,ln,fun,t=traceback.extract_tb(ei[2],1)[0]
	traceback.print_exc()
	return '%s (file=%s line=%s text="%s")'%(ei[1],fn,ln,t)

random_string_seed='ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz'
def random_string(length,printable=True):
	"Returns a random string with a given length, optionally printable"
	s=[]
	for i in range(0,length):
		if printable:
			s.append(random.choice(random_string_seed))
		else:
			s.append(chr(random.randint(0,255)))
	return ''.join(s)

def password_to_key(pwd):
	""" Returns a 16B key (suitable for AES) based on a password """
	if SECURED:
		from Crypto.Hash import SHA
		return SHA.new(pwd).digest()[0:16]
	else:
		import sha
		return sha.sha(pwd).digest()[0:16]

def random_nick():
	" Returns a random nick "
	return random_string(6,printable=True)

def configure_logging(**kwargs):
	""" A convenience method to call to basicConfig a couple of times.
	This method has the same argument as logging.basicConfig plus:
		preserve If True, preserve previosly configured handlers (default False)
	"""
	import logging
	logging.raiseExceptions=0
	if not kwargs.get('preserve', False):
		for h in logging.root.handlers: logging.root.handlers.remove(h)
	logging.basicConfig(**kwargs)
