#!/usr/bin/env python

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
import xmlrpclib, pickle, sys, pprint
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from xmlrpclib import Binary
#from StringIO import StringIO


server=xmlrpclib.ServerProxy('http://localhost:51234')

class Memory(LoggingMixIn, Operations):
    """Example memory filesystem. Supports only one level of files."""
    
    def __init__(self):
	#File Dictionary
        #self.files = {}
	now = time()
	attrs = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
            st_mtime=now, st_atime=now, st_nlink=2)
	file_dict = dict([['/', attrs]])
	pf=pickle.dumps(file_dict)
	server.put(Binary("files"), Binary(pf), 3000)
        #Data Dictionary
	self.data = defaultdict(str)
	pd=pickle.dumps(self.data)#string obj
	server.put(Binary("data"), Binary(pd), 3000)
        #fd 
        self.fd = 0
	#pfd=pickle.dumps(self.fd)
	#server.put(Binary("fd"), Binary(pfd), 3000)
        
	
        #self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
            #st_mtime=now, st_atime=now, st_nlink=2)
        
    def chmod(self, path, mode):
        #self.files[path]['st_mode'] &= 0770000
	#get
        rv=server.get(Binary("files"))
	data_str = rv['value'].data
	myFiles = pickle.loads(data_str)
	#endGet
	myFiles[path]['st_mode'] &= 0770000
        #self.files[path]['st_mode'] |= mode
	myFiles[path]['st_mode'] |= mode
	#set
	p=pickle.dumps(myFiles)
	server.put(Binary("files"), Binary(p), 3000)
	#endSet
	return 0

    def chown(self, path, uid, gid):
        #self.files[path]['st_uid'] = uid
	#get
        rv=server.get(Binary("files"))
	data_str = rv['value'].data
	myFiles = pickle.loads(data_str)
	#endGet
	myFiles[path]['st_uid'] = uid
        #self.files[path]['st_gid'] = gid
	myFiles[path]['st_gid'] = gid
	#set
	p=pickle.dumps(myFiles)
	server.put(Binary("files"), Binary(p), 3000)
	#endSet
    
    def create(self, path, mode):
	#get
        rv=server.get(Binary("files"))
	data_str = rv['value'].data
	myFiles = pickle.loads(data_str)
	#endGet
        #self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
            #st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())
	myFiles[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
            st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())
	#set
	p=pickle.dumps(myFiles)
	server.put(Binary("files"), Binary(p), 3000)
	#endSet
        self.fd += 1
        return self.fd
    
    def getattr(self, path, fh=None):
	#get
        rv=server.get(Binary("files"))
	data_str = rv['value'].data
	myFiles = pickle.loads(data_str)
	#endGet
        if path not in myFiles:
            raise FuseOSError(ENOENT)
        st = myFiles[path]
        return st
    
    def getxattr(self, path, name, position=0):
	#get
        rv=server.get(Binary("files"))
	data_str = rv['value'].data
	myFiles = pickle.loads(data_str)
	#endGet        
	attrs = myFiles[path].get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR
    
    def listxattr(self, path):
	#get
        rv=server.get(Binary("files"))
	data_str = rv['value'].data
	myFiles = pickle.loads(data_str)
	#endGet
        attrs = myFiles[path].get('attrs', {})
        return attrs.keys()
    
    def mkdir(self, path, mode):
	#get
        rv=server.get(Binary("files"))
	data_str = rv['value'].data
	myFiles = pickle.loads(data_str)
	#endGet
        myFiles[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())
        myFiles['/']['st_nlink'] += 1
	#set
	p=pickle.dumps(myFiles)
	server.put(Binary("files"), Binary(p), 3000)
	#endSet   	

    def open(self, path, flags):
        self.fd += 1
        return self.fd
    
    def read(self, path, size, offset, fh):
	#get
        rv=server.get(Binary("data"))
	data_str = rv['value'].data
	myData = pickle.loads(data_str)
	#endGet
        return myData[path][offset:offset + size]
    
    def readdir(self, path, fh):
	#get
        rv=server.get(Binary("files"))
	data_str = rv['value'].data
	myFiles = pickle.loads(data_str)
	#endGet
        return ['.', '..'] + [x[1:] for x in myFiles if x != '/']
    
    def readlink(self, path):
	#get
	rv=server.get(Binary("data"))
	data_str = rv['value'].data
	Data = pickle.loads(data_str)
	#endGet
        return Data[path]
    
    def removexattr(self, path, name):
	#get
        rv=server.get(Binary("files"))
	data_str = rv['value'].data
	myFiles = pickle.loads(data_str)
	#endGet
        attrs = myFiles[path].get('attrs', {})
        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR
	#set
	p=pickle.dumps(myFiles)
	server.put(Binary("files"), Binary(p), 3000)
	#endSet    
	
    def rename(self, old, new):
	#get
        rv=server.get(Binary("files"))
	data_str = rv['value'].data
	myFiles = pickle.loads(data_str)
	#endGet
        myFiles[new] = myFiles.pop(old)
	#set
	p=pickle.dumps(myFiles)
	server.put(Binary("files"), Binary(p), 3000)
	#endSet 
	    
    def rmdir(self, path):
	#get
        rv=server.get(Binary("files"))
	data_str = rv['value'].data
	myFiles = pickle.loads(data_str)
	#endGet
        myFiles.pop(path)
        myFiles['/']['st_nlink'] -= 1
	#set
	p=pickle.dumps(myFiles)
	server.put(Binary("files"), Binary(p), 3000)
	#endSet 
    
    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
	#get
        rv=server.get(Binary("files"))
	data_str = rv['value'].data
	myFiles = pickle.loads(data_str)
	#endGet
        attrs = myFiles[path].setdefault('attrs', {})
        attrs[name] = value
	#set
	p=pickle.dumps(myFiles)
	server.put(Binary("files"), Binary(p), 3000)
	#endSet 
    
    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)
    
    def symlink(self, target, source):
	#get
        rv=server.get(Binary("files"))
	data_str = rv['value'].data
	myFiles = pickle.loads(data_str)
	#endGet
        myFiles[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
            st_size=len(source))
	#set
	p=pickle.dumps(myFiles)
	server.put(Binary("files"), Binary(p), 3000)
	#endSet
	#get
        rv=server.get(Binary("data"))
	data_str = rv['value'].data
	Data = pickle.loads(data_str)
	#endGet 
        Data[target] = source
	#set
	p=pickle.dumps(Data)
	server.put(Binary("data"), Binary(p), 3000)
	#endSet 
    
    def truncate(self, path, length, fh=None):
	#get
	rv=server.get(Binary("data"))
	data_str = rv['value'].data
	Data = pickle.loads(data_str)
        #endGet
	Data[path] = Data[path][:length]
	#set
	p=pickle.dumps(Data)
	server.put(Binary("data"), Binary(p), 3000)
	#get
        rv=server.get(Binary("files"))
	data_str = rv['value'].data
	myFiles = pickle.loads(data_str)
	#endGet        
	myFiles[path]['st_size'] = length
    	#set
	p=pickle.dumps(myFiles)
	server.put(Binary("files"), Binary(p), 3000)
	#endSet

    def unlink(self, path):
	#get
        rv=server.get(Binary("files"))
	data_str = rv['value'].data
	myFiles = pickle.loads(data_str)
	#endGet        
	myFiles.pop(path)
	#set
	p=pickle.dumps(myFiles)
	server.put(Binary("files"), Binary(p), 3000)
	#endSet
    
    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
	#get
        rv=server.get(Binary("files"))
	data_str = rv['value'].data
	myFiles = pickle.loads(data_str)
	#endGet 
        myFiles[path]['st_atime'] = atime
        myFiles[path]['st_mtime'] = mtime
	#set
	p=pickle.dumps(myFiles)
	server.put(Binary("files"), Binary(p), 3000)
	#endSet
    
    def write(self, path, data, offset, fh):
	#get
	rv=server.get(Binary("data"))
	data_str = rv['value'].data
	Data = pickle.loads(data_str)
	#endGet
        Data[path] = Data[path][:offset] + data
	#set
	p=pickle.dumps(Data)
	server.put(Binary("data"), Binary(p), 3000)
	#endSet
	#get
        rv=server.get(Binary("files"))
	data_str = rv['value'].data
	myFiles = pickle.loads(data_str)
	#endGet 
        myFiles[path]['st_size'] = len(Data[path])
        #set
	p=pickle.dumps(myFiles)
	server.put(Binary("files"), Binary(p), 3000)
	#endSet
	return len(data)


if __name__ == "__main__":
    if len(argv) != 2:
        print 'usage: %s <mountpoint>' % argv[0]
        exit(1)
    fuse = FUSE(Memory(), argv[1], foreground=True)
