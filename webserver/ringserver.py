# Starts a Ring server with DHT with the configuration in ringserver.conf
import dfs, dfs.utils, dfs.ring, dfs.DHT
import logging

dfs.utils.configure_logging(level=logging.DEBUG,
	format = '%(asctime)s %(name)s %(levelname)s %(message)s',
	filename = 'ringserver.log',
	datefmt = '%H:%M:%S')

config = dfs.utils.Config()
config.load(open('ringserver.conf', 'r'))
ring = dfs.ring.Ring((config.get('Ring:ip'), config.getint('Ring:port')))
dfs.DHT.NetServerDHT(ring, config)
ring.join()