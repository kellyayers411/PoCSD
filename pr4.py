#!/usr/bin/env python

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import xmlrpclib, pickle, hashlib, math 
from xmlrpclib import Binary 

server = xmlrpclib.ServerProxy('http://localhost:51234')

class Memory(LoggingMixIn, Operations):
    """Example memory filesystem. Supports only one level of files."""
   
    def __init__(self):
	self.ttl = 3000
	#roottest = server.get(Binary("files"))

	server_count = server.list_nodes()
	self.server_cnt = len(server_count)
	print "server count = ", server_count

	#create ('files')
	now = time()
        attrs = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
        st_mtime=now, st_atime=now, st_nlink=2)

	file_dict = dict([['/', attrs]])
	pick1 = pickle.dumps(file_dict)
	server.put(1, Binary('files'), Binary(pick1), 3000)	

	#create ('data')
	self.d_val = defaultdict(str)
	pick_dict = pickle.dumps(self.d_val)
	server.put(2, Binary('data'), Binary(pick_dict), 3000)

	#create ('fd')
	self.fd = 0		
        fd = str(self.fd)
	#fd_val = defaultdict(int)
	pick_fd = pickle.dumps(fd)
	server.put(1, Binary('fd'), Binary(pick_fd), 3000)
 

	#I left the original "locals" for reference, at least for a few defs
	#md5_er returns the server number where the data is being stored     
    def md5_er(self, path):
    	print 'md5, path in = ', path
    	m = md5.new(path)
    	print 'm = ', m
    	m_hex = m.hexdigest()
    	print "m_hex = ", m_hex
    	print 'm_hex type = ', type(m_hex)
    	x = int(m_hex, 16)
    	print 'x = ', x
    	print 'x type = ', type(x)
    	#server_num = x % self.server_cnt
    	#print 'server_num = ' , server_num
	return x	#may need to change this to m_hex

    def chmod(self, path, mode):
        #self.files[path]['st_mode'] &= 0770000
        files = self.getRemote(1, 'files')
	files[path]['st_mode'] &= 0770000
	
	#self.files[path]['st_mode'] |= mode
	files[path]['st_mode'] |= mode
	
	#self.setRemote('files', files, self.ttl)
	pick = pickle.dumps(files)
        server.put(1, Binary('files'), Binary(pick), self.ttl)
        return 0

    def chown(self, path, uid, gid):
        #self.files[path]['st_uid'] = uid
        files = self.getRemote(1, 'files')
	files[path]['st_uid'] = uid

	#self.files[path]['st_gid'] = gid
	files[path]['st_gid'] = gid

	#self.setRemote('files', files, self.ttl)
	pick = pickle.dumps(files)
        server.put(1, Binary('files'), Binary(pick), self.ttl)    

    def create(self, path, mode):
	files = self.getRemote(1, 'files')
	files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
            st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())
        #self.setRemote('files', files, self.ttl)
	pick = pickle.dumps(files)
	server.put(1, Binary('files'), Binary(pick), self.ttl)
	fd = self.getRemote(1, 'fd')
	fd = int(fd)
	fd += 1
	#self.setRemote('fd', fd, self.ttl)
	#self.fd += 1
	pick = pickle.dumps(fd)
        server.put(1, Binary('fd'), Binary(pick), self.ttl)
        return fd
    
    def getattr(self, path, fh=None):
	files = self.getRemote(1, 'files')
        if path not in files:
            raise FuseOSError(ENOENT)
        st = files[path]
        return st
    
    def getRemote(self, path, key):
	#print "getting " + key
	node_id = self.md5_er(path)
	rv = server.get(node_id, Binary(key))
	d = rv['value'].data
	var = pickle.loads(d)
	#print var
	return var

     def getdataRemote(self, path):
        node_id = self.md5_er(path)
        rv = server.get(node_id, Binary('data'))
	d = rv['value'].data
        var = pickle.loads(d)
	bnts = var[0:8] 
        bnts_int = int(bnts)
        #this section pads the bnts out to 8 Bytes
     
        if bnts_int == 1
		data_dump = var[8:]
              
        else
                data_dump = var[8:]
                node_id = self.md5_er(path)
                pick = pickle.dumps(data_dump)
                server.put(Binary(node_id), Binary('data'), Binary(pick), self.ttl)
                int(n) = 1
                i = 0
                for d in range(1,(bnts_int -1))
                        newpath = path + i
			node_id = self.md5_er(newpath)
			rv = server.get(node_id, Binary('data')
			d2 = rv['value'].data
			var = pickle.loads(d2)
                        data_dump = data_dump + var[:]
			i +=
        return data_dump


    def getxattr(self, path, name, position=0):
        files = self.getRemote(1, 'files')
	attrs = files[path].get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR
    
    def listxattr(self, path):
	files = self.getRemote(1, 'files')
        attrs = files[path].get('attrs', {})
        return attrs.keys()
    
    def mkdir(self, path, mode):
	files = self.getRemote(1, 'files')
        files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())
        files['/']['st_nlink'] += 1
	#self.setRemote('files', files, self.ttl)    
	pick = pickle.dumps(files)
        server.put(1, Binary('files'), Binary(pick), self.ttl)

    def open(self, path, flags):
	fd = self.getRemote(1, 'fd')
	fd = int(fd)
        fd += 1
	pick = pickle.dumps(fd)
        server.put(1, Binary('fd'), Binary(pick), self.ttl)
        return fd
    
    def read(self, path, size, offset, fh):
        #return self.data[path][offset:offset + size]
	
	#PSEUDOCODE- assuming u created an array named files_[server_num]
	#
	#
	data_ = self.getRemote(path, 'data')
	return data_[path][offset:offset + size]
	
    def readdir(self, path, fh):
	files = self.getRemote(1, 'files')
        return ['.', '..'] + [x[1:] for x in files if x != '/']
    
    def readlink(self, path):
        #return self.data[path]
	data_ = self.getRemote(path, 'data')
    	return data_[path]

    def removexattr(self, path, name):
	files = self.getRemote(1, 'files')
        attrs = files[path].get('attrs', {})
        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR
	#self.setRemote('files', files, self.ttl)    
	pick = pickle.dumps(files)
        server.put(1, Binary('files'), Binary(pick), self.ttl)

    def rename(self, old, new):
	files = self.getRemote(1, 'files')
	print files	#test
        files[new] = files.pop(old)
	#self.setRemote('files', files, self.ttl)    
	pick = pickle.dumps(files)
        server.put(1, Binary('files'), Binary(pick), self.ttl)

    def rmdir(self, path):
	files = self.getRemote(1, 'files')
        files.pop(path)
        files['/']['st_nlink'] -= 1
	#self.setRemote('files', files, self.ttl)
	pick = pickle.dumps(files)
        server.put(1, Binary('files'), Binary(pick), self.ttl)

    def setdataRemote(self, data_, ttl):
	data_size = len(data_['value'].data)    #may shit on u here!!!
                                #consider a = data_[path]['value']
                                # data_size = len(a)
        blocks_needed_to_store = math.ceil((data_size+8)/1024.)
        #"+8" to acct for using the first 8 bytes to store bnts
        bnts = blocks_needed_to_store
        bnts_int = int(bnts)
        #this section pads the bnts out to 8 Bytes
        d = len(bnts)
        n = 8 - d
        for d in range(0,n):
                bnts = '0' + bnts
                d = len(bnts)

        data_['value'].data = bnts + data_['value'].data
        if bnts_int == 1
                node_id = self.md5_er(path)
                pick = pickle.dumps(data_)
                server.put(Binary(node_id), Binary('data'), Binary(pick), self.ttl)
        else
                data_dump = data_[path][0:1024]
                node_id = self.md5_er(path)
                pick = pickle.dumps(data_dump)
		server.put(Binary(node_id), Binary('data'), Binary(pick), self.ttl)
                int(n) = 1
                i = 0
                for d in range(1,(bnts_int -1))
                        newpath = path + i
                        data_dump = data_[path][(1024*n):(1024*n+1024)]
                        node_id = self.md5_er(newpath)
                        pick = pickle.dumps(data_dump)
                        server.put(Binary(node_id), Binary('data'), Binary(pick), self.ttl)
                        i +=

	#server.put(Binary('key'), d, ttl)
	#print "setting key = " + key
	#print "and value = " + value
	#print "valuetype = " + type(value)
	#pickled_dict = pickle.dumps(value)
	#server.put(Binary(key),Binary(pickled_dict), ttl)
	return True

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
	files = self.getRemote(1, 'files')
        attrs = files[path].setdefault('attrs', {})
        attrs[name] = value
	#self.setRemote('files', files, self.ttl)
	pick = pickle.dumps(files)
        server.put(1, Binary('files'), Binary(pick), self.ttl)    

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)
    
    def symlink(self, target, source):
	files = self.getRemote(1, 'files')
        files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
            st_size=len(source))
	#self.setRemote('files', files, self.ttl)
	pick = pickle.dumps(files)
        server.put(1, Binary('files'), Binary(pick), self.ttl)
	
	data_ = self.getRemote(target, 'data') 
        data_[target] = source
	#self.setRemote('data', data_, self.ttl)
	pick = pickle.dumps(data_)
        server.put(m, Binary('data'), Binary(pick), self.ttl)

    def truncate(self, path, length, fh=None):
        #self.data[path] = self.data[path][:length]
	data_ = self.getRemote(path, 'data')
	data_[path] = data_[path][:length]

	#self.setRemote('data', data_, self.ttl)

	data_size = len(data_['value'].data)	#may shit on u here!!!
				#consider a = data_[path]['value']
				# data_size = len(a)
	blocks_needed_to_store = math.ceil((data_size+8)/1024.)
	#"+8" to acct for using the first 8 bytes to store bnts
	bnts = blocks_needed_to_store
	bnts_int = int(bnts)
	#this section pads the bnts out to 8 Bytes
	d = len(bnts)
	n = 8 - d
	for d in range(0,n):
		bnts = '0' + bnts
		d = len(bnts)

	data_['value'].data = bnts + data_['value'].data
	if bnts_int == 1
		node_id = self.md5_er(path)
		pick = pickle.dumps(data_)
        	server.put(Binary(node_id), Binary('data'), Binary(pick), self.ttl)
	else
		data_dump = data_[path][0:1024]
		node_id = self.md5_er(path)
                pick = pickle.dumps(data_dump)
                server.put(Binary(node_id), Binary('data'), Binary(pick), self.ttl)
		int(n) = 1
		i = 0
		for d in range(1,(bnts_int -1))
			newpath = path + i
			data_dump = data_[path][(1024*n):(1024*n+1024)]
     		        node_id = self.md5_er(newpath)
                	pick = pickle.dumps(data_dump)
                	server.put(Binary(node_id), Binary('data'), Binary(pick), self.ttl)
			i +=

	files = self.getRemote(1, 'files')
	files[path]['st_size'] = length    
	#self.setRemote('files', files, self.ttl)
	pick = pickle.dumps(files)
        server.put(1, Binary('files'), Binary(pick), self.ttl)

    def unlink(self, path):
	files = self.getRemote(1, 'files')
        files.pop(path)
	#self.setRemote('files', files, self.ttl)
	pick = pickle.dumps(files)
        server.put(1, Binary('files'), Binary(pick), self.ttl)    

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        files = self.getRemote(1, 'files')
	files[path]['st_atime'] = atime
        files[path]['st_mtime'] = mtime
	#self.setRemote('files', files, self.ttl)    
	pick = pickle.dumps(files)
        server.put(1, Binary('files'), Binary(pick), self.ttl)

    def write(self, path, data, offset, fh):
        #self.data[path] = self.data[path][:offset] + data
	key = self.md5_er(path)
	pick = pickle.dumps(key)
	data_ = self.getRemote(Binary(pick), 'data')
	
	data_[path] = data_[path][:offset] + data	#put this after the last block has been retrieved

	#self.setRemote('data', data_, self.ttl)
	pick = pickle.dumps(data_)
	server.put(Binary(md5_out), Binary('data'), Binary(pick), self.ttl)
	
	files = self.getRemote(1, 'files')
	files[path]['st_size'] = len(data_[path])
        #self.setRemote('files', files, self.ttl)
	pick = pickle.dumps(files)
        server.put(1, Binary('files'), Binary(pick), self.ttl)
	#self.close(self.fd)	#added
	return len(data)

if __name__ == "__main__":
    if len(argv) != 2:
        print 'usage: %s <mountpoint>' % argv[0]
        exit(1)
    fuse = FUSE(Memory(), argv[1], foreground=True)

