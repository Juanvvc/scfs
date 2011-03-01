#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# An File object implementation over a Distributed Hashtable
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

# Note: the nick support could be dropped in later versions

import utils
import re
import dfs
import logging
from base64 import b32encode, b32decode
from types import FileType

logger=logging.getLogger('DFS')

# Name of keys in the configuration directory
KEY_NAMES=('kd', 'kf', 'ks', 'kss', 'kff', 'kgg')

try:
	if dfs.NO_SECURITY: raise Error('')
	from Crypto.Cipher import AES
	from Crypto.Hash import SHA
	SECURED = True
except:
	SECURED = False
	from sha import sha
	logger.warn('No crypto module')
	class DummyEncrypter():
		def encrypt(self, data): return data
		def decrypt(self, data): return data

def get_new_hasher(initial_data=''):
	""" Gets the configured hasher """
	if SECURED:
		return SHA.new(initial_data)
	else:
		return sha(initial_data)


class URI:
	""" A description of a resource in the network. Three formats:
	dfs://nick@uid/path Human readable reference to a file or directory
		in the path of the user uid.
	dfsf://nick@hd Static reference to a file with identifier Hd
	dfsd://nick@hd Static reference to a directory with identifier Hd
	
	Warning: the nick support coluld be dropped in later versions
	"""
	_dfsexp=re.compile('dfs://(?P<nick>\w*@)?(?P<uid>\w*/)?(?P<path>.*)$')
	_dfsxexp=re.compile('dfs[fd]://(?P<nick>\w*@)(?P<hd>\w+)$')
	def __init__(self,uid=None,nick=None,path='/',config=None,kd=None):
		""" Initializes the URI with the values of uid, nick and path.
		conf is the configuration to use.
		kd is the identifier of the used to codify the URI. If None, uses 'kd' in one
		of the provided configuration.
		configuration. You can set the Hd before initialization with
		URI.hd."""
		self.uid = uid
		self.nick = nick
		self.path = path
		if config:
			if not kd: kd = KEY_NAMES[0]
			self.key = config.get_key(kd)
		else:
			self.key = None
		self.hd = None
	def get_hd(self):
		""" Gets the descriptor Hd of the URI  (binary, 16B).
		The description could be used to locate a resource in a secure
		way. If there are a Kd in the configuration, the human readable
		URI is hashed and then crypted with that Key. IN this way, it is
		not enough to know an human readable reference to a file, but
		you will also need Kd. If there is no Kd, the Hd is just a
		hashing of the human readable URI.
		"""
		if self.hd: return self.hd
		if SECURED and self.key:
			# AES needs blocks of 16B, so get only the first 16B of the SHA hashing
			h=get_new_hasher(self.get_readable()).digest()[0:16]
			# Encode in AES using the key[1]
			return AES.new(self.key,AES.MODE_ECB).encrypt(h)
		else:
			# Get only the first 16B of the SHA hashing
			return get_new_hasher(self.get_readable()).digest()[0:16]
	def get_readable(self):
		""" Gets a human readable version of the URI """
		if self.nick:
			return 'dfs://%s@%s/%s'%(self.nick,self.uid,self.path)
		elif self.uid:
			return 'dfs://%s/%s'%(self.uid,self.path)
		else:
			return 'dfs:///%s'%(self.path)		
	def get_static(self):
		""" Gets an ofuscated URI that uses the Hd """
		# Use the base32 encoded hd... removing the trailing 6B!
		#(they are always ======, since len(hd)=16B)
		rhd=b32encode(self.get_hd())[:-6]
		if self.nick:
			return 'dfsf://%s@%s'%(self.nick,rhd)
		else:
			return 'dfsf://%s'%(rhd)
	def __str__(self):
		return self.get_readable()

def random_uri(config=None, kd=None):
	""" Creates a random URI """
	u = URI('', utils.random_nick(), '', config=config, kd=kd)
	u.hd=utils.random_string(16, printable=False)
	return u

def uri_from_string(uri,config=None,kd=None):
	""" Creates an URI from an string. Uses the default nick and UID if none is provided.
	
	>>> u=uri_from_string('dfs://nick@uid/path')
	>>> print u.nick
	nick
	>>> print u.uid
	uid
	>>> print u.get_readable()
	dfs://nick@uid/path
	>>> print u.get_static()
	dfs://nick@1bb5195ec909e7dc91a6c1de6f66380f
	
	"""
	if not config: config=dfs.default_config
	u=URI('','','',config=config,kd=kd)
	m=re.match(URI._dfsexp,uri)
	if m:
		u.uid=config.get('Main:UID')
		v=m.group('uid')
		if v and len(v)>1: u.uid=v[:-1]
		u.nick=config.get('Main:nick','')
		v=m.group('nick')
		if v and len(v)>1: u.nick=v[:-1]
		u.path='/'
		v=m.group('path')
		if v: u.path=v
		u.hd=None
		return u
	else:
		m=re.match(URI._dfsxexp,uri)
		if m:
			u.uid=config.get('Main:UID','')
			u.nick=config.get('Main:nick','')
			v=m.group('nick')
			if v and len(v)>1: u.nick=v[:-1]
			u.hd=b32decode(m.group('hd')+'======')
			return u
		else:
			return None

def create_dir(name, uri=None, parent=None, config=None, atomic=True, keys=None):
	""" Creates a new directory.
	parent: Optional directory parent of this directory. The directory
	will be inmediately added to the parent.
	returns the new directory """
	if not uri: uri = random_uri(keys)
	if name and not name.endswith(Dir.DIR_SEP): name += Dir.DIR_SEP
	f=File(uri, 'w', config=config, keys=keys)
	if parent:
		parent.add(f, name=name)
		f.write('%s:%s%s'%(Dir.PARENT_DIR, parent.uri.get_static(), Dir.EOL))
	if name:
		f.write('%s:%s%s'%(Dir.THIS_DIR, name, Dir.EOL))
	f.close()
	return Dir(uri,parent,keys,config,atomic)
	
class Dir:
	""" Implements a directory in the filesystem.
	A directory is a file that includes pairs name:uri to its contents.
	It is up to the shell to distinguish between files and dirs
	Paths have little sense in this approach: /home/user/ may be
	a child of /home/ or not """
	EOL = '\x0a'
	DIR_SEP = '/'
	PARENT_DIR = '../'
	THIS_DIR = './'
	def __init__(self, uri, parent=None, keys=None, config=None, atomic=True):
		""" Reads an existing dir.
		Call to Dir.create() to create a new directory.
		atomic: if true, save for each writing to the directory (slow if there
		are many changes) If true, save only in close() or save()
		"""
		self.closed = True
		if not uri: raise ValueError('URI is None')
		logging.debug("Opening dir "+uri.get_readable())
		self.atomic = atomic
		self.config = config
		self.keys = keys
		self.uri = uri
		self.parent = parent
		f = File(uri,'r', keys, config)
		self.files = dict()
		line = ''
		for l in f.read():
			if l == self.EOL:
				name, uri = line.split(':', 1)
				self.files[name] = uri
				logger.debug('%s: has file %s in %s'%(self.uri.get_readable(),name,uri))
				line = ''
			else:
				line += l
		if self.files.has_key(self.THIS_DIR):
			self.dirname = self.files.pop(self.THIS_DIR)
			logger.debug('%s is called %s'%(self.uri.get_readable(),self.dirname))
		else:
			self.dirname = None
		if len(self.files)==0:
			logger.debug('%s has no files'%self.uri.get_readable())	
		self.closed = False
		self.modified = False
	def list(self):
		""" Returns a list of readable names of the files in the directory """
		return self.files.keys()
	def add(self, entry, name=None):
		""" Add a new file/directory to the directory.
		If name is None, uses the path of the uri as name of the file """
		if self.closed: raise IOError('Dir is closed')
		if not name: name=entry.uri.path
		self.files[name]=entry.uri.get_static()
		self.modified = True
		if self.atomic: self.save()
	def remove(self, name):
		""" Removes a file/directory from the directory """
		if self.closed: raise IOError('Dir is closed')
		if not self.files.has_key(name): raise IOError('Entry not in dir')
		self.files.pop(name)
		self.modified = True
		if self.atomic: self.save()
	def rename(self, name1, name2):
		""" Renames a existing file with a new name inside the dir """
		if self.closed: raise IOError('Dis is closed')
		if not self.files.has_key(name1): raise IOError('File not in Dir')
		self.files[name2] = self.files[name1]
		self.files.pop(name1)
		self.modified = True
		if self.atomic: self.save()
	def save(self):
		""" Saves the data to the directory. You won't need to call
		to this method, since it is called from close(), add() or remove() """
		if self.closed: raise IOError('Dir is closed')
		if not self.modified: return
		logger.debug('%s dir saves its information'%self.uri.get_readable())
		f=File(self.uri,'w',self.keys,self.config)
		if self.dirname:
			f.write('%s:%s%s'%(self.THIS_DIR, self.dirname, self.EOL))
		for k in self.list():
			f.write('%s:%s%s'%(k, self.files[k], self.EOL))
		f.close()
		self.modified = False
	def close(self):
		""" Closes the directory. If the opration mode wasn't atomic,
		saves the content of the directory in the DFS """
		if self.closed: return
		if not self.atomic: self.save()
	def __del__(self):
		self.close()

class File():
	"""
	Implements a file in the DFS. Note that in the current version, this class is not
	a complete implementation of a Python file object: the management of lines
	is not implemented.
	"""
	def __init__(self,uri,mode,config=None,save_metadata=True,keys=None):
		""" Initializes the file object.
		- uri is a URI or str object with the address of the file.
		- mode is the mode of the file. Currently, just 'w' and 'r'
		- keys are a pair (Kf, Kd) to use. If None, use the pair in the
		configuration. Kf is used to crypt/decrypt the file and Kd to
		secure the resource localization
		- config is the configuration to use. In None, use the default 
		configuration
		- keys is an array of identifiers for the keys used in the file: in None,
		it uses KEY_NAMES (see extended documentation)
		- If save_metadata is True, the file descriptor is saved in
		the DHT. You can access to the file descriptor with
		File.metadata """
		
		if not config: config=dfs.default_config
		self.config=config
		if not keys: keys=KEY_NAMES
		self.keys=[]
		for k in keys:
			self.keys.append(self.config.get_key(k))

		if type(uri)==str: uri=uri_from_string(uri,config=config,kd=self.keys[0])

		logger.debug("Accesing file %s (%s)"%(uri, mode))
		
		self.uri=uri
		self.mode = mode
		self.closed = True
		
		self.buffer=[]
		# The maximum block size
		self.BLOCK_SIZE=self.config.getint('File:block',dfs.dht.BLOCK_SIZE)
		# The file descriptor could be bigger than the block size. To
		# prevent this, the metadata is chained in several blocks
		# Use this parameter to control how many part references
		# a block of metadata holds.
		self.DESC_PER_METAPART=self.config.getint('File:descPerMetapart',12)
		# The max length of the internal buffer before an automatic flush()
		self.MAX_BUFFER=self.config.getint('File:maxbuffer',4096)
		self.save_metadata=save_metadata
		
		if mode=='r':
			# crypter used to decrypt the metadata. There is always
			# a crypter to protect against casual atackers, but
			# if there is no Kff the crypter is nearly useless
			if SECURED:
				if self.keys and self.keys[0]:
					mdencrypter=AES.new(self.keys[4],AES.MODE_CBC,self.uri.get_hd())
				else:
					mdencrypter=AES.new(self.uri.get_hd(),AES.MODE_CBC,self.uri.get_hd())
			else:
				mdencrypter=DummyEncrypter()
			# get the metadata from the DHT
			md=dfs.dht.get(uri.get_hd(),uri.nick)
			if not md: raise IOError('No reference to that file: ' + uri.get_static())
			self.metadata=utils.Config()
			md=mdencrypter.decrypt(md)
			try:
				self.metadata.load(md)
			except:
				raise IOError('The reference is not metatada: %s'%utils.format_error())
			# get basic information from the metadata
			self.uri.uid=self.metadata.get('Main:UID')
			self.uri.nick=self.metadata.get('Main:nick')
			np=self.metadata.getint('Main:parts', 0)
			self.filelength=self.metadata.getint('Main:length')
			# get info about each one of the parts
			self.parts=[]
			cmd=self.metadata
			for next_part in range(0,np):
				self.parts.append(cmd.get("Part:%d"%next_part))
				# load chained metadata
				if next_part<np-1 and next_part%self.DESC_PER_METAPART==self.DESC_PER_METAPART-1:
					nuri=uri_from_string(cmd.get('Main:n'))
					md=dfs.dht.get(nuri.get_hd(),nuri.nick)
					if not md: raise IOError('No reference to %s (%d)'%(nuri.get_static(),next_part))
					md=mdencrypter.decrypt(md)
					cmd=utils.Config()
					try:
						cmd.load(md)
					except:
						raise IOError('The reference is not metadata: %s'%utils.format_error())
					
			self.eof=bool(len(self.parts)==0)
		elif mode=='w':
			if not self.uri.uid: self.uri.uid=dfs.default_config.get('Main:UID')
			if not self.uri.nick:
				self.uri.nick=dfs.default_config.get('Main:nick')
				#if not self.uri.nick: self.uri.nick=utils.random_nick()
			self.metadata=utils.Config()
			self.metadata.set('Main:UID',self.uri.uid)
			if self.uri.nick:
				self.metadata.set('Main:nick',self.uri.nick)
			self.parts=[]
			self.filelength=0
		else:
			raise IOError,'Mode not supported: %s'%mode
		
		s='File %s in mode "%s" '%(self.uri.get_readable(),self.mode)
		for k in self.keys:
			if k:
				s+='1'
			else:
				s+='0'
		logger.info(s)
		
		# Create hasher and crypter
		self.hasher=get_new_hasher()
		if self.keys[1] and SECURED:
			# The crypter is AES in CBC mode, with IV=Hd of the file
			self.crypter=AES.new(self.keys[1],AES.MODE_CBC,self.uri.get_hd())
		else:
			self.crypter=None
		
		logger.info('Opening %s in mode=%s'%(self.uri.get_readable(),self.mode))
		self.closed = False
	
	def close(self):
		""" Closes the file and returns a URI with the final address of the file.
		If save_metadata is set, the file metadata is saved in the DHT.
		You can always access to the metadata with File.metadata """
		if self.closed: return
		logger.info('Closing %s'%self.uri.get_readable())
		if self.mode == 'w':
			self.flush(True)
			self.metadata.set('Main:parts', len(self.parts))
			self.metadata.set('Main:length', self.filelength)
			self.metadata.set('Main:hash', self.hasher.hexdigest())
			self.metadata.set('Main:p', '')
			if self.save_metadata:
				# variables used to chain metadata blocks
				puri = self.uri
				pmeta = self.metadata
				not_saved = True
				# crypter used to encrypt the metadata. There is always
				# a crypter to protect against casual atackers, but
				# if there is no Kf the crypter is nearly useless
				if SECURED:
					if self.keys[4]:
						mdencrypter = AES.new(self.keys[4], AES.MODE_CBC, self.uri.get_hd())
					else:
						mdencrypter = AES.new(self.uri.get_hd(), AES.MODE_CBC, self.uri.get_hd())
				else:
					mdencrypter = DummyEncrypter()
				for i in range(0, len(self.parts)):
					pmeta.set('Part:%d'%i, self.parts[i])
					# chain the metadata blocks, each block only with
					# DESC_PER_METAPART references to parts of the file
					if i<len(self.parts)-1 and i%self.DESC_PER_METAPART == self.DESC_PER_METAPART-1:
						nuri=URI(self.uri.uid,utils.random_nick(),'',self.keys)
						nuri.hd=utils.random_string(16,False)
						pmeta.set('Main:n',nuri.get_static())
						m=pmeta.save()
						pmeta.set('Main:p',utils.random_string(self.BLOCK_SIZE-len(m)))
						m=mdencrypter.encrypt(pmeta.save())
						dfs.dht.put(puri.get_hd(),m,puri.nick)
						pmeta=utils.Config()
						pmeta.set('Main:p','')
						puri=nuri
						not_saved=False
					else:
						not_saved=True
				if not_saved:
					m=pmeta.save()
					pmeta.set('Main:p',utils.random_string(self.BLOCK_SIZE-len(m)))
					m=mdencrypter.encrypt(pmeta.save())
					dfs.dht.put(puri.get_hd(),m,puri.nick)

			# Create the final metadata
			for i in range(0,len(self.parts)):
				self.metadata.set('Part:%d'%i,self.parts[i])
		else:
			# In read, free the buffer
			self.buffer = None
		self.closed = True
		return self.uri
	def flush(self, alldata=False):
		""" Flushes the contents of the file.
		Actually, only multiples of BLOCK_SIZE are flushed. If alldata is
		set, all the data in the buffer is flushed (padding the last data to
		a block of 1024 bytes) Warning: do NOT use alldata=True except
		in the last block of the file (close() internally calls to
		flush(True) """
		if self.closed: raise IOError('Closed')
		if not self.mode == 'w': raise IOError('In read mode')
		logger.info('Flushing %s'%self.uri.get_readable())
		
		bl = len(self.buffer)
		fl = bl / self.BLOCK_SIZE
		if alldata and not bl == fl * self.BLOCK_SIZE: fl = fl + 1
		
		for i in range(0,fl):
			p = ''.join(self.buffer[i * self.BLOCK_SIZE:(i + 1) * 1024])
			if len(p) < self.BLOCK_SIZE:
				# pad random data at the end of the block
				p += utils.random_string(self.BLOCK_SIZE-len(p))
			# encrypt data if there is a crypter
			if self.crypter: p = self.crypter.encrypt(p)
			# create a random nick and calculate the hash of the part
			u = random_uri(self.config)
			self.hasher.update(p)

			logger.info('Saving part ' + u.get_static())
			# Save the part in the DHT
			dfs.dht.put(u.get_hd(), p, u.nick)

			# Save the reference to the part
			self.parts.append(u.get_static())
		
		if fl * self.BLOCK_SIZE >= bl:
			self.buffer = []
		else:
			self.buffer = self.buffer[fl * self.BLOCK_SIZE:]

	def _complete_read(self):
		""" Reads the contents of the file."""
		s = []
		try:
			# read and return the whole file
			for p in self.parts:
				logger.info('Reading part ' + p)
				uri = uri_from_string(p)
				d = dfs.dht.get(uri.get_hd(),uri.nick)
				# TODO: do not decrypt now, but in the actual read
				if self.crypter:	d = self.crypter.decrypt(d)
				s.append(d)
			s=''.join(s)
			self.eof = True
			# TODO: check the file hashing before returning
			return s[0:self.filelength]
		except:
			raise IOError('Cannot read: %s'%utils.format_error())
	def read(self, size=0):
		""" Reads the contents of the file. The first call reads the complete file, since
		it may be suffled. """
		# TODO: optionally use a temporal file instead of a memory buffer
		if self.eof: return []
		if self.closed: raise IOError,'Closed'
		if not self.mode == 'r': raise IOError,'In write mode'

		if len(self.buffer) == 0 and not self.eof:
			self.buffer = self._complete_read()
		r = None
		if not size or size>len(self.buffer):
			self.eof = True
			r = self.buffer
			self.buffer = None
		else:
			r = self.buffer[0:size]
			self.buffer = self.buffer[size:]
		return r
	def write(self,data):
		""" Writes data in the file """
		if self.closed: raise IOError,'Closed'
		if not self.mode == 'w': raise IOError,'In read mode'
		self.filelength = self.filelength+len(data)
		self.buffer.extend(data)
		if len(self.buffer) > self.MAX_BUFFER: self.flush()
	def __del__(self):
		""" Closes the file when there is no further reference """
		try:
			self.close()
		except:
			logger.warn('Error closing file %s'%self.uri.get_readable())
			utils.format_error()
	
	# Not supported methods:
	def seek(offset, whence=None): raise IOError('seek() not supported')
	def tell(): raise IOError('tell() not supported')
	def truncate(size=0): raise IOError('truncate() not supported')
