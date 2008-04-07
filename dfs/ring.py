#!/usr/bin/python
# -*- coding: utf-8 -*-
# 
# Implementation of a structured P2P
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
import os, sys
import dfs, utils
import random

import SimpleXMLRPCServer
import xmlrpclib

import threading

logger = logging.getLogger('Ring')

MAX_ID = pow(2,128)-1
""" Identifiers in the ring are 128 bits integers, but in every method
are represented as strings since XMLRPC does not support longs. Only
in the Ring.manage() method the conversion between str and long takes
place """

class Ring(threading.Thread):	
	""" Manager of a ring. A ring is a structured P2P overlay with
	a ring shape. This class is intended to manage joins and leaves
	of nodes in the network and route messages between nodes. If you
	want to use a ring as a network in your application, set the
	Ring.listener property to a class that extends RingListener to
	get messages from other nodes.
	Call to start() to actually start the server """
	def __init__(self, address, config=None, known=None):
		""" Creates a node in the ring. Call to the start() method to start the
		ring process.
		- address a pair (ip,port) of the node in the ring
		- config. The configuration to use. In None, use the default.
		- known. A tuple of pairs (ip,port) of other nodes in the network
		"""
		threading.Thread.__init__(self)
		self.listener = None
		self.joined = False
		self.contacted = dict()
		self.next = self.prev = None
		self.config = dfs.default_config
		if config: self.config = config

		# read the preknow nodes from config
		if not known:
			k = self.config.get('Ring:known')
			self.known = list()
			if k:
				for n in k.split(','):
					ip,port = n.split(':')
					self.known.append((ip,int(port)))
		else:
			self.known = known
		
		# get an identifier from conf or a random one
		# Identifiers are numbers up to MAX_ID
		self.int_id = self.config.getint('Ring:ID',random.randint(0,MAX_ID))
		self.id = str(self.int_id)
		
		# register the functions in the XML-RPC server
		self.address = address
		logger.info('Ring server at %s:%d'%address)
		self.server = MyXMLRPCServer(address, allow_none=True, logRequests=False)
		self.server.register_introspection_functions()
		self.server.register_function(self.msg, 'msg')
		self.server.register_function(self.join_msg, 'join_msg')
		self.server.register_function(self.who_msg, 'who_msg')
		self.server.register_function(self.leave_msg, 'leave_msg')
		self.server.register_function(self.id_msg, 'id_msg')
	
	def run(self):
		""" Executes the ring server. This method calls to join_node() """
		self.join_node()
		self.running = True
		logger.info('%s: Server ready'%self.id)
		self.server.serve_forever()
		logger.info('%s: Server finished'%self.id)
		self.running = False
			
	def join_node(self):
		""" Joins the current node to the ring """
		logger.info('%s: starting the joining process'%self.id)
		if self.known:
			# Join to an existing ring
			# Ask for the manager of my id
			(w, wip, wport) = self.__next(random.choice(self.known)).who_msg(self.id)
			logger.info('%s: %s (%s:%d) manages my key'%(self.id, w, wip, wport))
			self.contacted[w] = (wip, wport)
			# Join to the manager of my ID. It will answer with the
			# address of the next node
			(n, nip, nport) = self.__next(w).join_msg(self.address[0], self.address[1], self.id)
			logger.info('%s: next=%s (%s:%d)'%(self.id, n, nip, nport))
			self.contacted[n] = (nip, nport)
			self.next = n
			self.prev = w
			self.int_next = int(self.next)
			self.int_prev = int(self.prev)
			logger.info('%s: I manage keys [%s,%s)'%(self.id, self.id, self.next))
		else:
			# I am the first node of my own ring
			logger.info('%s: empty ring'%self.id)
			self.next = None
			self.prev = None
		if self.listener: self.listener.joined()
		self.joined = True
	
	def leave(self):
		""" Leaves the network """
		try:
			logger.debug('%s: leaving the network'%self.id)
			if self.joined and self.next:
				na = self.contacted[self.next]
				if self.prev: self.__next(self.prev).leave_msg(self.id, self.next, na[0], na[1])
				if self.listener: self.listener.left(None)
				self.joined = False
				logger.info('%s: left the network'%self.id)
			else:
				logger.info('%s: I was alone in the network'%self.id)
		except:
			utils.format_error()
		self.server.shutdown()

	def manage(self, id):
		""" Returns true if the current node manages the segment that
		includes the point id """
		if not self.next: return True
		int_id = int(id)
		if self.int_next > self.int_id:
			return int_id >= self.int_id and int_id < self.int_next
		else:
			return int_id < self.int_next or (int_id >= self.int_id and int_id < MAX_ID)
	
	def id_msg(self):
		""" Returns the identifier of this node """
		return self.id
	
	def msg(self, to, *args):
		""" Sends a message to other nodes
		- to The identifier of the point of the ring that gets the
		message. Note that may or not be the same that the identifier of
		the node that manages that segment of the ring.
		- *args A list of arguments to pass to the other node ('the
		message'). Binary arguments must be wrapped in a xmlprclib.Binary
		object, and decoded in the destinity """	
		logger.info('%s: new application message'%self.id)
		if self.manage(to):
			# it is for me
			if not self.listener: return None
			try:
				return self.listener.message(to, *args)
			except:
				return "ERROR: "%utils.format_error()
		else:
			# it is not for me: inform to the listeners
			# and answers if there is a response
			r=None
			try:
				if self.listener:
					r=self.listener.routing(to, *args)
			except:
				logger.warn('Routing app: ' + utils.format_error())
			if r:
				return r
			else:
				return self.__next(to).msg(to, *args)
	
	def join_msg(self, ip, port, id):
		""" Manages a join message from other node """
		logger.info('%s: join message from %s (%s:%d)'%(self.id, id, ip, port))
		an = self.next
		self.next = id
		self.int_next = int(self.next)
		self.contacted[id] = (ip,port)
		self.joined = True
		self.contacted[id] = (ip,port)
		if not an:
			an = self.id
			ana = self.address
		else:
			ana = self.contacted[an]
		return (an,ana[0], ana[1])
	
	def leave_msg(self, id, next, ip, port):
		""" Manages a leave message from other node """
		logger.info('%s: leave message from %s (%s:%d)'%(self.id, id, ip, port))
		self.contacted.pop(id)
		if self.prev == id:
			self.next = self.prev = None
			logger.info('%s: I am alone in the ring'%self.id)
			return True
		else:
			import time
			time.sleep(1)
			logger.info('%s: joining to %s (%s:%d)'%(self.id, next, ip, port))
			self.__next((ip,port)).join_msg(self.address[0],self.address[1],self.id)
			return True
	
	def who_msg(self,id):
		""" Manages a who message from other node """
		logger.info('%s: who message for %s'%(self.id,id))
		if self.manage(id):
			return (self.id,self.address[0],self.address[1])
		else:
			return self.__next(id).who_msg(id)
		
	def __next(self,to):
		""" route a message to the point 'to' """
		try:
			if type(to)==tuple:
				p=xmlrpclib.ServerProxy("http://%s:%d"%to)
			else:
				# TODO: improve the routing. This one just circle the
				# message in the ring
				if self.contacted.has_key(to):
					na=self.contacted[to]
				else:
					na=self.contacted[self.next]	
				p=xmlrpclib.ServerProxy('http://%s:%d'%(na[0],na[1]))
			return p
		except:
			if type(to)==str:
				st=to
			else:
				st='%s:%d'%(to[0],to[1])
			logger.warn("%s: error routing message to %s"%(self.id,st))
			raise IOError(utils.format_error())
	
	def __del__(self):
		self.leave()

class MyXMLRPCServer(SimpleXMLRPCServer.SimpleXMLRPCServer):
	# methods from ServerSocket to redefine to allow interruptions (from pyCherryPy 2.0)
	def __init__(self, *argv, **kwargv):
		SimpleXMLRPCServer.SimpleXMLRPCServer.__init__(self, *argv, **kwargv)
	def server_activate(self):
		SimpleXMLRPCServer.SimpleXMLRPCServer.server_activate(self)
		self.socket.settimeout(1)
	def handle_request(self):
		try:
			SimpleXMLRPCServer.SimpleXMLRPCServer.handle_request(self)
		except socket.timeout:
			return 1
		except KeyboardInterrupt:
			self.shutdown()
	def serve_forever(self):
		"""Override serve_forever to handle shutdown."""
		self.__running = 1
		while self.__running:
			self.handle_request()
	def shutdown(self):
        	self.__running = 0
	def get_request(self):
		# With Python 2.3 it seems that an accept socket in timeout (nonblocking) mode
		#  results in request sockets that are also set in nonblocking mode. Since that doesn't play
		#  well with makefile() (where wfile and rfile are set in SocketServer.p	y) we explicitly set
		#  the request socket to blocking	
		request, client_address = self.socket.accept()
		request.setblocking(1)
		return request, client_address

class RingListener:
	" An interface for a listener of the ring "
	def message(to,*args):
		""" Gets a message from a remote node to a point 'to' """
		pass
	def routing(to,*args):
		""" The current node is routing a message to a point 'to'
		that it does not manage. Useful in applications with caches.
		If this method returns None, the normal routing takes place.
		Else, the value is returned to the original node """
		return None
	def left(*args):
		""" An event: the node leaves the network """
		pass
	def joined(*args):
		""" An event: the node joins the network """
		pass
