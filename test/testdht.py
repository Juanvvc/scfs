import sys, os

from dfs.ring import Ring
from dfs.utils import Config
from dfs.DHT import NetServerDHT, NetClientDHT
import dfs

dfs.init_default_conf()

# Create three nodes with DHTs
ring1=Ring(('localhost',15150),Config().set('Ring:id',123).set('DHT:datadir','dhtdata1'))
ring2=Ring(('localhost',15151),Config().set('Ring:id',12).set('DHT:datadir','dhtdata2'))
ring3=Ring(('localhost',15152),Config().set('Ring:id',500).set('DHT:datadir','dhtdata3'))
ring2.known.append(('localhost',15150))
ring3.known.append(('localhost',15150))

# Start the DHT (they set themselves as listeners of the ring and
# join to the network, if not joined)
NetServerDHT(ring1)
NetServerDHT(ring2)
NetServerDHT(ring3)

# Create a client DHT (connected to any node)
dht=NetClientDHT('http://localhost:15150')
# save data and test it
print dht.put('testing','This is test data')
print dht.get('testing')
