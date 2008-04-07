import cherrypy
import os
from cherrytemplate import renderTemplate
import dfs, dfs.utils, dfs.filesystem
from cherrypy.lib.static import serve_download

def render(template_name, **kwargs):
	""" A helper method to render templates """
	if not kwargs.has_key('msg_error'): kwargs['msg_error'] = ''
	kwargs['template_file'] = 'templates/%s.html'%template_name
	return renderTemplate(file='templates/skel.html', loc=kwargs)

class WebDFS:
	_cp_config = {'tools.sessions.on': True}
	
	def __init__(self, ringconfig):
		self.ringconfig = ringconfig
	
	def index(self, msg_error=None):
		return render('index', msg_error=msg_error)
	index.exposed = True
	
	def login(self, uid=None, password=None):
		try:
			if not uid: raise Error()
			config = dfs.utils.Config()
			config.load(open('configs/' + uid,'r'))
		except:
			dfs.utils.format_error()
			return self.index('Please provide a valid user')
		cherrypy.session['config'] = config
		cherrypy.session['uid'] = config.get('Main:UID')
		cherrypy.session['current'] = config.get('Main:root')
		return self.listdir()
	login.exposed = True

	def logout(self):
		cherrypy.session['config'] = None
		cherrypy.session['current'] = None
		cherrypy.session['uid'] = None
		return self.index()
	logout.exposed = True
	
	def listdir(self, msg_error=None, newuri=None):
		""" Lists the content of a distributed directory.
		if newuri is not provided, uses the root folder of the current user
		"""
		try:	
			config = cherrypy.session.get('config')
			if not newuri:
				strcurrent = cherrypy.session.get('current')
			else:
				strcurrent = newuri
				cherrypy.session['current'] = newuri
			uri = dfs.filesystem.uri_from_string(strcurrent, config=config)
			if not uri:
				return self.index('URI cannot be decoded: ' + strcurrent)
			current = dfs.filesystem.Dir(uri, config=config)
			return render('listfiles', files=current.files, msg_error=msg_error)
		except:
			dfs.utils.format_error()
			return self.index('Some error happened')		
	listdir.exposed = True
	
	def createprofile(self, msg_error=None):
		""" Shows the form to create/upload a new profile """
		return render('createprofile', msg_error=msg_error)
	createprofile.exposed = True
	
	def saveprofile(self, uid=None, password=None, config=None):
		""" Saves a new profile """
		if not uid and not config.filename:
			return self.createprofile('Empty UID and file')
		if uid and config.filename:
			return self.createprofile('createprofile', 'Both UID and configuration file provided')
		if uid:
			# Creates a new user, keys and root folder
			c = dfs.utils.Config()
			c.set('Main:UID', uid)
			nick = dfs.utils.random_nick()
			c.set('Main:nick', nick)
			if password:
				kpwd = dfs.utils.password_to_key(password)
				for kid in dfs.filesystem.KEY_NAMES:
					c.set_key(kid, dfs.utils.random_string(16, False), kpwd)
			# create the root folder
			try:
				root = dfs.filesystem.random_uri(config=c)
				dfs.filesystem.create_dir(None, root, config=c)
			except:
				dfs.utils.format_error()
				return self.createprofile('Cannot create the root folder')
			c.set('Main:root', root.get_static())
		else:
			# Reads the configuration file for the UID and root folder
			c = dfs.utils.Config()
			c.load(config.file.read())
			uid = c.get('Main:UID')
			try:
				root = dfs.filesystem.uri_from_string(c.get('Main:root'), config=c)
				dfs.filesystem.Dir(root, config=c)
			except:
				dfs.utils.format_error()
				return self.createprofile('Cannot access to root folder')
		# Save the configuration, checking that it is a unique user
		confpath = 'configs/%s'%uid
		if(os.path.exists(confpath)):
			return self.createprofile('Configuration exists')
		c.save(open(confpath, 'w'))
			
		return self.index()
	saveprofile.exposed = True
	
	def get(self, uri=None):
		""" Gets a distributed file as a download """
		try:
			config = cherrypy.session.get('config')
			f = dfs.filesystem.File(dfs.filesystem.uri_from_string(uri), 'r', config=config)
			data = f.read()
			f.close()
		except:
			return self.listdir(msg_error='Not found')
		
		response = cherrypy.response
		response.headers['Content-Type'] = 'application/x-download'
		response.headers["Content-Disposition"] = 'attachment'		
		response.headers['Content-Length'] = len(data)
		response.body = data
		return response.body
	get.exposed = True
	
	def put(self, file=None):
		""" Uploads a file """
		try:
			config = cherrypy.session.get('config')
			f = dfs.filesystem.File(dfs.filesystem.random_uri(config), 'w', config=config)
			f.write(file.file.read())
			f.close()
			strcurrent = cherrypy.session.get('current')
			current = dfs.filesystem.Dir(dfs.filesystem.uri_from_string(strcurrent, config=config), config=config)
			current.add(f, name=file.filename)
		except:
			print dfs.utils.format_error()
			return self.listdir(msg_error='File not saved')
		return self.listdir()
	put.exposed = True
	
	def mkdir(self, name=None):
		""" Creates a new directory """
		if not name:
			return self.listdir('No name of the new dir')
		config = cherrypy.session.get('config')
		current = dfs.filesystem.Dir(dfs.filesystem.uri_from_string(cherrypy.session.get('current'), config=config), config=config)
		dfs.filesystem.create_dir(name, config=config, parent=current)
		return self.listdir()
	mkdir.exposed = True
	
	def remove(self, name=None):
		""" Removes a file or directory """
		if not name:
			return self.listdir(msg_error='No name to remove')
		config = cherrypy.session.get('config')
		current = dfs.filesystem.Dir(dfs.filesystem.uri_from_string(cherrypy.session['current'], config=config), config=config)
		current.remove(name)
		return self.listdir()
	remove.exposed = True
	
	def listlocal(self, msg_error=None):
		return render('listlocal', files=os.listdir(self.ringconfig.get('DHT:datadir')), msg_error=msg_error)
	listlocal.exposed = True

	def getlocal(self, file=None):
		if not file:
			return listlocal(msg_error="No file to download")
		return serve_download(os.path.join(self.ringconfig.get('DHT:datadir'), file))
	getlocal.exposed = True

	def getconfig(self):
		uid = cherrypy.session.get('uid', None)
		if not uid:
			return self.index()		
		return serve_download(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'configs', uid))
	getconfig.exposed = True

if __name__ == '__main__':	
	# Start the DHT client
	dfs.utils.configure_logging(level=10,
		format = '%(asctime)s %(name)s %(levelname)s %(message)s',
		filename = 'dhtwebserver.log',
		datefmt = '%H:%M:%S')
	ringconfig = dfs.utils.Config()
	ringconfig.load(open('ringserver.conf', 'r'))
	#dfs.dht = dfs.DHT.NetClientDHT(ringconfig.get('Ring:server'))
	dfs.dht = dfs.DHT.LocalDHT()
	
	# Config: read from webserver.conf and add the static directory for statics
	thisdir = os.path.dirname(os.path.abspath(__file__))
	cherrypy.config.update(os.path.join(thisdir, 'webserver.conf'))
	conf = {'/static': {
			'tools.staticdir.on': True,
			'tools.staticdir.root': thisdir,
			'tools.staticdir.dir': 'static'
			}}
	# Start the web server
	cherrypy.quickstart(WebDFS(ringconfig), config=conf)
