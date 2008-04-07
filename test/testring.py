import sys, os

import dfs
from dfs.ring import Ring, RingListener
from dfs.utils import Config, configure_logging

dfs.init_default_conf()
# Configures the logging system
configure_logging(level=10,
	format='%(asctime)s %(name)s %(levelname)s %(message)s',
	datefmt='%H:%M:%S',
	filemode='w')	

class ServerListener(RingListener):
	def message(self,to,msg): return msg.upper()

# Create two nodes
ring1=ring2=None
ring1=Ring(('localhost',15150),Config().set('Ring:id',123))
ring2=Ring(('localhost',15151),Config().set('Ring:id',12))
ring3=Ring(('localhost',15152),Config().set('Ring:id',500))
ring3.listener=ServerListener()

# Join them
ring2.known.append(('localhost',15150))
ring3.known.append(('localhost',15150))
ring1.start()
ring3.start()
ring2.start()

print 'Waiting 3 seconds'
import time
time.sleep(3)

# Send a message from 2 to 1
print ring2.msg('600','Hello world')

# Leave the network
ring2.leave()
time.sleep(1)
print 'ring 2 ended'
ring1.leave()
time.sleep(1)
print 'ring 1 ended'
ring3.leave()
print 'ring 3 ended'
