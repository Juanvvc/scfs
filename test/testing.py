import sys, os
import dfs
from dfs.filesystem import File,uri_from_string
from dfs.ring import Ring
from dfs.DHT import NetServerDHT, NetClientDHT
from dfs.utils import Config

dfs.init_default_conf()

##################################################
print 'Textual file'
f=File(uri_from_string('dfs://juanvi/textual'),'w')
f.write('probando, proban')
u=f.close()
print "Saved in %s (%s)"%(u.get_readable(),u.get_static())
f=File(u,'r')
print "Reading static: %s"%f.read()
f.close()
f=File(uri_from_string('dfs://juanvi/textual'),'r')
print "Reading dinamic: %s"%f.read()
f.close()

##################################################
print 'Digital file'
f=File(uri_from_string('dfs://juanvi/digital'),'w')
f2=open('test/digital.jpg','r')
f.write(f2.read())
u=f.close()
f2.close()
print "Saved in %s (%s)"%(u.get_readable(),u.get_static())
f=File(u,'r')
f2=open('test/digital2.jpg','w')
f2.write(f.read())
f.close()
f2.close()

###################################################
print 'The whole thing'

# This is a daemon in one computer
ring1=Ring(('localhost',15150),Config().set('DHT:datadir','dhtdata1'))
NetServerDHT(ring1)

# Another daemon in another computer
ring2=Ring(('localhost',15151),Config().set('DHT:datadir','dhtdata2'))
ring2.known.append(('localhost',15150))
NetServerDHT(ring2)

# An other daemon in other computer
ring3=Ring(('localhost',15152),Config().set('Ring:id',500).set('DHT:datadir','dhtdata3'))
ring3.known.append(('localhost',15150))
NetServerDHT(ring3)

# Finally, a client application (in computer 1, for example)
dfs.dht=NetClientDHT('http://localhost:15150')
# This is the same that the second test
f=File(uri_from_string('dfs://juanvi/digital'),'w')
f2=open('test/digital.jpg','r')
f.write(f2.read())
u=f.close()
f2.close()
print "Saved in %s (%s)"%(u.get_readable(),u.get_static())
f=File(u,'r')
f2=open('test/digital3.jpg','w')
f2.write(f.read())
f.close()
f2.close()
