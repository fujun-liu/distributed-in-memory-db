# -*- coding: utf-8 -*-
"""
Created on Thu Dec  4 01:25:25 2014

@author: fujun
"""

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time,sleep

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from xmlrpclib import Binary
import sys, pickle, xmlrpclib
import md5

class RingHash:
    def __init__(self, urls):
        self.dict = {} # server_hash -> server url
        self.server_list = [] # server hash in sorted order
        for url in urls:
            server_hash = self.key_hash(url)
            self.dict[server_hash] = url
            self.server_list.append(server_hash)
        # sort server keys in increasing order
        self.server_list.sort()
        for server in self.server_list:
		        print '%d: %s' %(server, self.dict[server])
    
    # return the server after current one in the ring
    def get_server_next(self, url):
        server_hash = self.key_hash(url)
        if server_hash in self.server_list and len(self.server_list) > 1:
            idx = self.server_list.index(server_hash)
            idx_next = (idx+1)%len(self.server_list)
            return self.dict[self.server_list[idx_next]]     
        else:
            return
    
    # return the coordinate server for certain key
    def get_server(self, data_key):
        # same hash function
        if not self.dict:
            return
            
        data_hash = self.key_hash(data_key)
        # find the first server whose hash >= data hash
        for server_hash in self.server_list:
            if data_hash <= server_hash:
                return self.dict[server_hash]
       
        # this is a special case
        return self.dict[self.server_list[0]]
    
    def remove_server(self, url):
        # del the key hash
        server_hash = self.key_hash(url)
        if server_hash in self.dict:
            del self.dict[server_hash]
        
        if server_hash in self.server_list:
            self.server_list.remove(server_hash)
        
    # use md5 hash. The servers and data entries use same hash space    
    def key_hash(self, key):
        m = md5.new()
        m.update(key)
        return long(m.hexdigest(), 16)
        
class HtProxy:
  """ Wrapper functions so the FS doesn't need to worry about HT primitives."""
  # A hashtable supporting atomic operations, i.e., retrieval and setting
  # must be done in different operations
  def __init__(self, urls, replicas):
      # build a ring hash
      self.ring_hash = RingHash(urls)
      self.replicas = replicas
      self.no_servers = len(urls)
      self.servers = []
      self.servers_lookup = {}
      idx = 0
      for url in urls:
        self.servers.append(xmlrpclib.Server(url))
        self.servers_lookup[url] = idx
        idx = idx + 1
             
  # Retrieves a value from the SimpleHT, returns KeyError, like dictionary, if
  # there is no entry in the SimpleHT
  def __getitem__(self, key):
    
      rv = self.get_ring(key)
      if rv == None:
          raise KeyError()
      return pickle.loads(rv)
    
    
  # Stores a value in the SimpleHT
  def __setitem__(self, key, value):
      self.put_ring(key, pickle.dumps(value))

  # Sets the TTL for a key in the SimpleHT to 0, effectively deleting it
  def __delitem__(self, key):
      self.put_ring(key, "", 0)
      
  # Retrieves a value from the DHT, if the results is non-null return true,
  # otherwise false
  def __contains__(self, key):
      return self.get_ring(key) != None
  
  def get_key_server_mod(self, key):
      return hash(key) % self.no_servers
  
  # consistent hashing used to get the servers
  def get_key_server_ring(self, key):
      url = self.ring_hash.get_server(key)
      if url == None:
          print 'no server found on the ring'
          raise IOError()
      else:
          return url
      
  def get(self, key):
      idx = self.get_key_server_ring(key)
      res = self.servers[idx].get(Binary(key))
      if "value" in res:
          return res["value"].data
      else:
          return

  def put(self, key, val, ttl=10000):
      idx = self.get_key_server_ring(key)
      return self.servers[idx].put(Binary(key), Binary(val), ttl)
  
  def get_ring(self, key):
      url = self.get_key_server_ring(key)
      while True:
          idx = self.servers_lookup[url]
          try:
              res = self.servers[idx].get(Binary(key))
              if "value" in res:
                  #print 'data --%s-- is read from server %s' %(res["value"].data, url)
                  return res["value"].data
              else:
                  return
          except:
              dead_node = url
              url = self.ring_hash.get_server_next(url)
              self.ring_hash.remove_server(dead_node)
                      
  # put data on ring, the replicas are specified by user  
  def put_ring(self, key, val, ttl=10000):
      if len(self.ring_hash.server_list) < self.replicas:
          print 'only %d machines are avaliable, %d replicas are needed' %(len(self.ring_hash.server_list), self.replicas)
          raise IOError
      count = 0
      dead_nodes = []
      url = self.get_key_server_ring(key)
      while count < self.replicas:
          idx = self.servers_lookup[url]
          try:
              self.servers[idx].put(Binary(key), Binary(val), ttl)
              count = count + 1
              print 'data --%s-- is successfully stored on server %s' %(key, url)
              # make sure data is consistent
              res = self.servers[idx].get(Binary(key))
              val = res["value"].data
          except:
              dead_nodes.append(url)
              print 'server %s is down now, will be removed from ring' %url
          finally:
              url = self.ring_hash.get_server_next(url)
              if url == None:
                  break
              
      for node in dead_nodes:
          self.ring_hash.remove_server(node)
          
      if count < self.replicas:
          print 'only %d replicas are written, %d replicas are needed' %(count, self.replicas)
          return False
      else:
          return True
  # acquire lock
  
#  def acquire_lock(self, key):
#    #idx = hash(key) % self.no_servers
#    idx = self.get_ket_server_ring(key)
#    return self.servers[idx].acquire_lock(Binary(key))
# 
#  def release_lock(self, key):
#   #idx = hash(key) % self.no_servers
#   idx = self.get_ket_server_ring(key)
#   self.servers[idx].release_lock(Binary(key))      

  #def read_file(self, filename):
  #  return self.rpc.read_file(Binary(filename))

  #def write_file(self, filename):
  #  return self.rpc.write_file(Binary(filename))

class Memory(LoggingMixIn, Operations):
  """Example memory filesystem. Supports only one level of files."""
  def __init__(self, ht):
    self.files = ht
    self.fd = 0
    now = time()
    if '/' not in self.files:
      self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
        st_mtime=now, st_atime=now, st_nlink=2, contents=['/'])

  def chmod(self, path, mode):
    ht = self.files[path]
    ht['st_mode'] &= 077000
    ht['st_mode'] |= mode
    self.files[path] = ht
    return 0

  def chown(self, path, uid, gid):
    ht = self.files[path]
    if uid != -1:
      ht['st_uid'] = uid
    if gid != -1:
      ht['st_gid'] = gid
    self.files[path] = ht
  
  def create(self, path, mode):
    print '**********create operation***************'
    self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1, st_size=0,
        st_ctime=time(), st_mtime=time(), st_atime=time(), contents='')

    ht = self.files['/']
    ht['st_nlink'] += 1
    ht['contents'].append(path)
    self.files['/'] = ht

    self.fd += 1
    return self.fd
  
  def getattr(self, path, fh=None):
      if path not in self.files['/']['contents']:
          raise FuseOSError(ENOENT)
      return self.files[path]
  
  
  def getxattr(self, path, name, position=0):
    attrs = self.files[path].get('attrs', {})
    try:
        return attrs[name]
    except KeyError:
        return ''    # Should return ENOATTR
  
  def listxattr(self, path):
    return self.files[path].get('attrs', {}).keys()
  
  def mkdir(self, path, mode):
    self.files[path] = dict(st_mode=(S_IFDIR | mode),
        st_nlink=2, st_size=0, st_ctime=time(), st_mtime=time(),
        st_atime=time(), contents=[])
    ht = self.files['/']
    ht['st_nlink'] += 1
    ht['contents'].append(path)
    self.files['/'] = ht

  def open(self, path, flags):
    self.fd += 1
    return self.fd
  
  def read(self, path, size, offset, fh):
    ht = self.files[path]
    if 'contents' in self.files[path]:
      return self.files[path]['contents']
    return None
  
  def readdir(self, path, fh):
    return ['.', '..'] + [x[1:] for x in self.files['/']['contents'] if x != '/']
  
  def readlink(self, path):
    return self.files[path]['contents']
  
  def removexattr(self, path, name):
    ht = self.files[path]
    attrs = ht.get('attrs', {})
    if name in attrs:
      del attrs[name]
      ht['attrs'] = attrs
      self.files[path] = ht
    else:
      pass    # Should return ENOATTR
  
  def rename(self, old, new):
    f = self.files[old]
    self.files[new] = f
    del self.files[old]
    ht = self.files['/']
    ht['contents'].append(new)
    ht['contents'].remove(old)
    self.files['/'] = ht
  
  def rmdir(self, path):
    del self.files[path]
    ht = self.files['/']
    ht['st_nlink'] -= 1
    ht['contents'].remove(path)
    self.files['/'] = ht
  
  def setxattr(self, path, name, value, options, position=0):
    # Ignore options
    ht = self.files[path]
    attrs = ht.get('attrs', {})
    attrs[name] = value
    ht['attrs'] = attrs
    self.files[path] = ht
  
  def statfs(self, path):
    return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)
  
  def symlink(self, target, source):
    self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
      st_size=len(source), contents=source)

    ht = self.files['/']
    ht['st_nlink'] += 1
    ht['contents'].append(target)
    self.files['/'] = ht
 
  def truncate(self, path, length, fh=None):
    print '********truncate operation**********'
    ht = self.files[path]
    if 'contents' in ht:
        ht['contents'] = ht['contents'][:length]
    ht['st_size'] = length
    self.files[path] = ht
#  def truncate(self, path, length, fh=None):
#    if self.files.acquire_lock(path):
#        try:
#            print 'lock acquired to truncate --%s-- to %d bytes' %(path, length)
#            ht = self.files[path]
#            if 'contents' in ht:
#              ht['contents'] = ht['contents'][:length]
#            ht['st_size'] = length
#            self.files[path] = ht
#        finally:
#            self.files.release_lock(path)
#            print 'truncate is done, lock released'
#            #pass
#    else:
#        print '********blocked, truncated operation aborted*********'
  
  def unlink(self, path):
    ht = self.files['/']
    ht['contents'].remove(path)
    self.files['/'] = ht
    del self.files[path]
  
  def utimens(self, path, times=None):
    now = time()
    ht = self.files[path]
    atime, mtime = times if times else (now, now)
    ht['st_atime'] = atime
    ht['st_mtime'] = mtime
    self.files[path] = ht
  
  def write(self, path, data, offset, fh):
     # Get file data
    print '*************write operation***************'
    ht = self.files[path]
    tmp_data = ht['contents']
    toffset = len(data) + offset
    if len(tmp_data) > toffset:
        # If this is an overwrite in the middle, handle correctly
        ht['contents'] = tmp_data[:offset] + data + tmp_data[toffset:]
    else:
        # This is just an append
        ht['contents'] = tmp_data[:offset] + data
    ht['st_size'] = len(ht['contents'])
    self.files[path] = ht
    return len(data)
#  def write(self, path, data, offset, fh):
#    
#    if self.files.acquire_lock(path):
#        try:
#            print 'lock acquired to write --%s-- to %s' %(data[:-1], path)
#            # Get file data
#            ht = self.files[path]
#            tmp_data = ht['contents']
#            toffset = len(data) + offset
#            if len(tmp_data) > toffset:
#              # If this is an overwrite in the middle, handle correctly
#              ht['contents'] = tmp_data[:offset] + data + tmp_data[toffset:]
#            else:
#              # This is just an append
#              ht['contents'] = tmp_data[:offset] + data
#            ht['st_size'] = len(ht['contents'])
#            self.files[path] = ht
#            # release lock at this point
#            return len(data)
#        finally:
#            self.files.release_lock(path)
#            print 'lock release, finished writing --%s-- to %s' %(data[:-1], path)
#            #pass
#    else:
#        print '******blocked, write operation is aborted*******'
#        return 0
        

if __name__ == "__main__":
  if len(argv) < 4:
    print 'usage: %s <mountpoint> <replicas> <remote hashtable>' % argv[0]
    exit(1)
  replicas = int(argv[2])
  url = argv[3:]
  if replicas > len(url):
      print 'there are only %d machines, not enough for %d replicas' %(len(url), replicas)
      exit(1)
  # Create a new HtProxy object using the URL specified at the command-line
  fuse = FUSE(Memory(HtProxy(url, replicas)), argv[1], foreground=True)
