#!/usr/bin/env python
# A implementation of a 'shell' for DFS
# Currently, only supports copy files from and to the DFS to local files,
# and starting a DFS node

import logging
import dfs
import dfs.utils
from optparse import OptionParser
import sys, os

def shell(options, args):
	import dfs.filesystem
	class Shell:
		def __init__(self, config=None, keys=None):
			if config:
				self.config = config
			else:
				self.config = dfs.default_config
			if keys:
				self.keys = keys
			else:
				self.keys = None
			self.root = self.config.get('Main:root')
			if not self.root:
				raise ValueError('No root in the configuration')
			# if the root does not exist, create one
			try:
				self.root = dfs.filesystem.Dir(dfs.filesystem.uri_from_string(self.root))
			except:
				logging.info('Creating root dir '+self.root)
				self.root = dfs.filesystem.create_dir('root', uri=dfs.filesystem.uri_from_string(self.root), parent=None)
			self.current = self.root
		def mkdir(self, name):
			""" Makes a directory in the current dir """
			dirname = name[0]
			dfs.filesystem.create_dir(dirname, parent=self.current)
			logging.info('Dir %s created'%dirname)
		def cd(self, name):
			""" Change the current dir to this one """
			name = name[0]
			if not name.endswith(dfs.filesystem.Dir.DIR_SEP):
				name += dfs.filesystem.Dir.DIR_SEP
			if not name:
				self.current = self.root
			else:
				if name == dfs.filesystem.Dir.THIS_DIR: return
				u = self.current.files[name]
				try:
					self.current.close()
					u = dfs.filesystem.uri_from_string(u)
					self.current = dfs.filesystem.Dir(u, keys=self.keys, config=self.config)
				except:
					dfs.utils.format_error()
		def mv(self, files):
			self.current.rename(files[0], files[1])
		def ls(self, ignored):
			for i in self.current.list(): print i
		def put(self, file):
			""" Puts a file in the DFS """
			remotefilename = file[0]
			if len(file) > 1: remotefilename = file[1]
			u = dfs.filesystem.random_uri(self.keys)
			f = dfs.filesystem.File(u, mode='w', config=self.config, keys=self.keys)
			f.write(open(file[0], 'r').read())
			f.close()
			self.current.add(f, remotefilename)
		def get(self, file):
			""" Gets a file from the DFS """
			u = dfs.filesystem.uri_from_string(self.current.files[file[0]], keys=self.keys, config=self.config)
			localfilename = file[0]
			if len(file) > 1: localfilename = file[1]
			f = dfs.filesystem.File(u, mode='r', config=self.config, keys=self.keys)
			open(localfilename,'w').write(f.read())
			f.close()
		def rm(self, name):
			""" Removes a file from the DFS """
			filename = name[0]
			self.current.remove(filename)
		def run(self):
			exit = False
			commands = { 'ls':self.ls, 'get':self.get, 'put':self.put, 'mkdir':self.mkdir, 'md':self.mkdir, 'rm':self.rm, 'cd':self.cd, 'mv':self.mv, 'rename':self.mv}
			while not exit:
				sys.stdout.write('%s# '%self.current.dirname)
				try:
					command = sys.stdin.readline().strip().split(' ')
				except KeyboardInterrupt:
					command = ['exit']
				if not command[0]: continue
				if command[0]=='exit':
					exit = True
				else:
					try:
						if commands.has_key(command[0]):
							commands[command[0]](command[1:])
						else:
							print 'Command not found: '+command[0]
					except:
						dfs.utils.format_error()
			self.current.close()
	try:
		sh=Shell()
		sh.run()
	except:
		dfs.utils.format_error()

def server(options,args):
	" Starts a Ring node "
	try:
		ring = dfs.ring.Ring((args[0], int(args[1])))
		dfs.DHT.NetServerDHT(ring, dfs.default_config)
		ring.join()
	except:
		sys.stderr.write(dfs.utils.format_error() + '\n')
		sys.stderr.write('Please, pass the IP and PORT of this server\n')

def copy(options, args):
	""" Copy two files, local file to DFS (publishing) or the other way
	(retrieving) """
	global dfs
	import dfs.filesystem
	if len(args) != 2:
		raise IOError('Use copy file1 file2')
	file1 = args[0]
	file2 = args[1]
	try:
		if dfs.filesystem.uri_from_string(file2):
			# writing
			f = dfs.filesystem.File(file2, 'w')
			f.write(open(file1, 'r').read())
			f.close()
		else:
			# reading
			f = dfs.filesystem.File(file1, 'r')
			open(file2, 'w').write(f.read())
	except:
		sys.stderr.write(dfs.utils.format_error() + '\n')

def configure(options, args):
	""" Creates a configuration in the default_config_file """
	global dfs
	try:
		c = dfs.utils.Config()
		if os.path.exists(options.conf):
			if not read_value(options.conf + ' exists. Overwrite?', False):
				return
		
		# User configuration
		print '\n***** User Information *****'
		c.set('Main:UID', read_value('User identifier', dfs.utils.random_string(16, True), False))
		c.set('Main:nick', read_value('User nick', dfs.utils.random_nick(), False))
		
		# Keys configuration
		print '\n***** Security Settings *****'
		kpwd = None
		while not options.pwd:
			r = read_value('Please, enter a password to access to your keys', None, True)
			if not r:
				if read_value('Please, confirm: empty password', False):
					options.pwd = None
					break
			else:
				r2 = read_value('Please, type again your password', None, False)
				if r == r2:
					options.pwd = r
					kpwd = dfs.utils.password_to_key(r)	
				else:
					print 'Passwords do not match. Type again'
		print 'I will generate a random set of keys'
		for kid in dfs.filesystem.KEY_NAMES:
			c.set_key(kid, dfs.utils.random_string(16, False), kpwd)

		# Filesystem configuration
		c.set('File:block', 1024)
		c.set('File:descPerMetapart', 12)
		c.set('File:maxbuffer', 4096)

		# DHT configuration
		print '\n***** DHT Settings *****'
		c.set('DHT:datadir', read_value('DHT datadir', dfs.default_config_dir + os.path.sep + 'dhtdata'))
		
		# Ring configuration
		print '\n***** Ring Settings *****'
		r = read_value('Ring identifier', 'random', False)
		if r == 'random' or not r.isdigit():
			import dfs.ring, random
			r = str(random.randint(0, dfs.ring.MAX_ID))
		c.set('Ring:ID', r)
		print 'Comma separed list of IP:PORT of known nodes in the ring'
		c.set('Ring:known', read_value('Known ring nodes', None, True))

		# Save configuration
		print 'Configuration is ready. Writing to file ' + options.conf
		c.save(open(options.conf, 'w'))
	except KeyboardInterrupt:
		print '\nConfiguration process canceled'
		return
	except:
		sys.stderr.write(dfs.utils.format_error() + '\n')

def read_value(question, default=None, let_empty=False):
	""" Ask a question in the standard output and gest a response in the
	standard input. If the response is empty, returns the default value
	"""
	if type(default) == bool:
		yesno = True
		yesnodef = default
		if default:
			default = 'Y'
		else:
			default = 'N'
	else:
		yesno = False
	d = ''
	if default: d = default
	sys.stdout.write('%s [%s] ? ' % (question,d))
	resp = sys.stdin.readline().rstrip()
	if not resp: resp = default
	while not resp and not let_empty:
		d = ''
		if default: d = default
		sys.stdout.write('Value cannot be empty [%s]: ' % d)
		resp = sys.stdin.readline().rstrip()
		if not resp: resp = default
	if not yesno:
		return resp
	else:
		return resp[0].upper() == 'Y'

if __name__ == '__main__':
	# Read configuration from the command line
	parser = OptionParser(usage="%prog [options] COMMAND command-opts", version="%prog 0.8")
	parser.add_option('-c', '--conf', dest='conf', metavar='FILE',
		help='Read configuration from FILE', default=dfs.default_config_file)
	parser.add_option('-p', '--pass', dest='pwd', metavar='PWD',
		help='PWD for the configuration file')
	parser.add_option('-d', '--debug-level',
		dest='debug_level', type='int', metavar='LEVEL',
		help='Debug LEVEL for information', default=logging.INFO)
	parser.add_option('-q', '--quiet', action='store_false', dest='log',
		help='Do not log messages', default=False)
	parser.add_option('-v', '--verbose', action='store_true', dest='log',
		help='Log messages')
	parser.add_option('-l','--logfile', dest = 'logfile', metavar = 'FILE',
		help='Log messages in FILE', default=dfs.default_log_file)
	parser.add_option('-S','', dest='nosec', help='Development mode',
		action='store_true', default=False)
	(options, args) = parser.parse_args()

	dfs.NO_SECURITY = options.nosec
	
	# Configure the logging system
	if options.log:
		dfs.utils.configure_logging(level=logging.DEBUG,
			format = '%(asctime)s %(name)s %(levelname)s %(message)s',
			filename = options.logfile,
			datefmt = '%H:%M:%S')
	else:
		# disable logging system
		dfs.utils.configure_logging(level=logging.CRITICAL)
	
	# read default configuration
	if options.pwd:
		dfs.default_config = dfs.utils.Config(dfs.utils.password_to_key(options.pwd))
	else:
		dfs.default_config = dfs.utils.Config()
	if os.path.exists(options.conf):
		dfs.default_config.load(open(options.conf, 'r'))
	else:
		logging.warning('Configuration file does not exists: %s'%options.conf)
		
	import dfs.DHT
	ring_node = dfs.default_config.get('Ring:server')
	if not ring_node:
		dfs.dht = dfs.DHT.LocalDHT(dfs.default_config)
	else:
		dfs.dht = dfs.DHT.NetClientDHT(ring_node)
	
	if len(args) == 0:
		parser.error('You must supply a command to run')

	# execute the command
	commands = {'server':server, 'cp':copy, 'copy':copy, 'configure':configure, 'shell':shell}
	if not commands.has_key(args[0]):
		parser.error('COMMAND is one of ' + str(commands.keys()))
	try:
		commands[args[0]](options, args[1:])
	except:
		parser.error(sys.exc_value)
