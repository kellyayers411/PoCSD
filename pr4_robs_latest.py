#!/usr/bin/env python

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import xmlrpclib, pickle, hashlib, math, random 
from xmlrpclib import Binary 
from StringIO import StringIO
from random import randint

server = xmlrpclib.ServerProxy('http://localhost:51234')
md5 = hashlib.md5


class Memory(LoggingMixIn, Operations):
    """Example memory filesystem. Supports only one level of files."""
    buf = str()
    
    def __init__(self):
	#self.buf = str()
	#buf = self.buf
	self.ttl = 3000
	#roottest = server.get(Binary("files"))

	server_count = server.list_nodes()
	self.server_cnt = len(server_count)
	print "server count = ", self.server_cnt

	#create ('files')
	now = time()
        attrs = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
        st_mtime=now, st_atime=now, st_nlink=2)

	file_dict = dict([['/', attrs]])
	pick1 = pickle.dumps(file_dict)
	server.put('1', Binary('files'), Binary(pick1), 3000)	

	#create ('data')
	'''for i in range(0, self.server_cnt):
		self.d_val = defaultdict(str)
		pick_dict = pickle.dumps(self.d_val)
		j = md5(str(randint(1, 100000)))
		j = j.hexdigest()
		jj = str(int(j,16))
		server.put(jj, Binary('data'), Binary(pick_dict), 3000)
	'''
	self.d_val = defaultdict(str)
        pick_dict = pickle.dumps(self.d_val)
	server.put('20', Binary('data'), Binary(pick_dict), 3000)

	#create ('fd')
	self.fd = 0		
        fd = str(self.fd)
	#fd_val = defaultdict(int)
	pick_fd = pickle.dumps(fd)
	server.put('1', Binary('fd'), Binary(pick_fd), 3000)
 

	#I left the original "locals" for reference, at least for a few defs
	#md5_er returns the server number where the data is being stored     
    def md5_er(self, path):
    	#print 'md5, path in = ', path
    	m = md5(path)
    	#print 'm = ', m
    	m_hex = m.hexdigest()
	#m_hex = m_hex[0:2]
    	#print "m_hex = ", m_hex
    	#print 'm_hex type = ', type(m_hex)
    	x = int(m_hex, 16)
	x = str(x)
	x = x[0:32]	 
    	print 'x = ', x
    	#print 'x type = ', type(x)
    	#server_num = x % self.server_cnt
    	#print 'server_num = ' , server_num
	return x	#may need to change this to m_hex, from x

    def chmod(self, path, mode):
        #self.files[path]['st_mode'] &= 0770000
        files = self.getRemote('1', 'files')
	files[path]['st_mode'] &= 0770000
	
	#self.files[path]['st_mode'] |= mode
	files[path]['st_mode'] |= mode
	
	#self.setRemote('files', files, self.ttl)
	pick = pickle.dumps(files)
        server.put('1', Binary('files'), Binary(pick), self.ttl)
        return 0

    def chown(self, path, uid, gid):
        #self.files[path]['st_uid'] = uid
        files = self.getRemote('1', 'files')
	files[path]['st_uid'] = uid

	#self.files[path]['st_gid'] = gid
	files[path]['st_gid'] = gid

	#self.setRemote('files', files, self.ttl)
	pick = pickle.dumps(files)
        server.put('1', Binary('files'), Binary(pick), self.ttl)    

    def create(self, path, mode):
	files = self.getRemote('1', 'files')
	files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
            st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())
        #self.setRemote('files', files, self.ttl)
	pick = pickle.dumps(files)
	server.put('1', Binary('files'), Binary(pick), self.ttl)
	
	
	fd = self.getRemote('1', 'fd')
	fd = int(fd)
	fd += 1
	#self.setRemote('fd', fd, self.ttl)
	#self.fd += 1
	pick = pickle.dumps(fd)
        server.put('1', Binary('fd'), Binary(pick), self.ttl)
        print 'end CREATE'
	return fd
    
    def getattr(self, path, fh=None):
	files = self.getRemote('1', 'files')
        if path not in files:
            raise FuseOSError(ENOENT)
        st = files[path]
	print 'st = ', st
        return st
    
    def getRemote(self, node_id, key):
	print "getting " + key
	#node_id = self.md5_er(path)
	rv = server.get('1', Binary(key))
	d = rv['value'].data	#changed 'value' to path
	#print 'd = ', d
	#print 'd type = ', type(d)
	var = pickle.loads(d)
	#print var
	return var

    def getdataRemote(self, path):
	print "GETDATAREMOTE()"
	node_id = self.md5_er(path)
	rv = server.get(node_id, Binary('data'))
	print 'rv = ', rv
	d = rv['value'].data
	#print 'd = ', d
        var= pickle.loads(d)
	print 'var type = ', type(var)
	print 'var value = ', var
	v = var[path]
	print 'v type = ', type(v)
        print 'v value = ', v

	if not v:
	   v = '00000001'
	bnts = v[0:8] 
	print 'bnts = ', bnts
	#print 'bnts type = ', type(bnts)
        bnts_int = int(bnts)
        #this section pads the bnts out to 8 Chars/Bytes
     
        if bnts_int == 1:
		print 'in IF'
		data_dump = v[8:]
		print 'data_dump = ', data_dump
        	print 'data_dump size = ', len(data_dump)      
        else:
		print 'in ELSE'
                data_dump = v[8:]
                i = '1'
		print 'd = ', d
                for d in range(0,(bnts_int -1)):
                        print 'in for loop(get), d = ', d
			newpath = path + i
			node_id = self.md5_er(newpath)
			rv = server.get(node_id, Binary('data'))
			e = rv['value'].data
			print 'e type = ', type(e)
        		print 'e value = ', e
			returned_dict = pickle.loads(e)
			value_field = returned_dict[newpath]
                        data_dump = data_dump + value_field[:]
			int_i = int(i)	#str to int, increment, then back
			int_i = int_i + 1	#for concat w/pathname
			i = str(int_i)
        var[path] = data_dump
	#print 'var type = ', type(var)
	print 'var value = ', var
	print 'END GETDATAREMOTE()'
	return var

    def getxattr(self, path, name, position=0):
        files = self.getRemote('1', 'files')
	attrs = files[path].get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR
    
    def listxattr(self, path):
	files = self.getRemote('1', 'files')
        attrs = files[path].get('attrs', {})
        return attrs.keys()
    
    def mkdir(self, path, mode):
	files = self.getRemote('1', 'files')
        files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())
        files['/']['st_nlink'] += 1
	#self.setRemote('files', files, self.ttl)    
	pick = pickle.dumps(files)
        server.put('1', Binary('files'), Binary(pick), self.ttl)

    def open(self, path, flags):
	fd = self.getRemote('1', 'fd')
	fd = int(fd)
        fd += 1
	pick = pickle.dumps(fd)
        server.put('1', Binary('fd'), Binary(pick), self.ttl)
        return fd
    
    def read(self, path, size, offset, fh):
        #return self.data[path][offset:offset + size]
	data_ = self.getdataRemote(path)
	return data_[path][offset:offset + size]
	
    def readdir(self, path, fh):
	files = self.getRemote('1', 'files')
        return ['.', '..'] + [x[1:] for x in files if x != '/']
    
    def readlink(self, path):
        #return self.data[path]
	data_ = self.getdataRemote(path)
    	return data_[path]

    def removexattr(self, path, name):
	files = self.getRemote('1', 'files')
        attrs = files[path].get('attrs', {})
        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR
	#self.setRemote('files', files, self.ttl)    
	pick = pickle.dumps(files)
        server.put('1', Binary('files'), Binary(pick), self.ttl)

    def rename(self, old, new):
	files = self.getRemote('1', 'files')
	print files	#test
        files[new] = files.pop(old)
	#self.setRemote('files', files, self.ttl)    
	pick = pickle.dumps(files)
        server.put('1', Binary('files'), Binary(pick), self.ttl)

    def rmdir(self, path):
	files = self.getRemote('1', 'files')
        files.pop(path)
        files['/']['st_nlink'] -= 1
	#self.setRemote('files', files, self.ttl)
	pick = pickle.dumps(files)
        server.put('1', Binary('files'), Binary(pick), self.ttl)

    def setdataRemote(self, path, data_, ttl):
	print "SETDATAREMOTE()"
	print 'data_ = ', data_
        #print 'data_ type = ', type(data_)
	ds = data_[path]
	print 'path = ', path
        #print 'path type, p = ', type(path)
	print 'ds = ', ds
	data_size = len(ds)    #may shit on u here!!!
                                #consider a = data_[path]['value']
                                # data_size = len(a)
        print 'data_size = ', data_size
	blocks_needed_to_store = math.ceil((data_size+8)/1008.)
        #"+8" to acct for using the first 8 bytes to store bnts
        bnts = blocks_needed_to_store
        print 'bnts = ', bnts
	#print 'bnts type = ', type(bnts)
	bnts_int = int(bnts)
        #this section pads the bnts out to 8 Bytes
	bnts_str = str(bnts_int)
	print 'bnts_str = ', bnts_str
        #print 'bnts_str type = ', type(bnts_str)
        d = len(bnts_str)
        n = 8 - d
        for d in range(0,n):
                bnts_str = '0' + bnts_str
		#print 'bnts_str = ', bnts_str
                d = len(bnts_str)
	data_out = bnts_str + ds
	print 'data_out = ', data_out
        print 'data_out type = ', type(data_out)
	data_[path] = data_out
	print 'data_[path] = ', data_[path]
        if bnts_int == 1:
                print 'IN IF setdataRemote()'
		node_id = self.md5_er(path)
		print 'data_ = ', data_
                pick = pickle.dumps(data_)
		#print 'pick = ', pick
                server.put(node_id, Binary('data'), Binary(pick), self.ttl)
		qq = server.get(node_id, Binary('data'))
		print 'qq = ', qq
		qqq = qq['value'].data
		qqqq = pickle.loads(qqq)
		print 'qqqq = ', qqqq
        else:
		print 'IN ELSE, setdataRemote()'
		out_ = data_[path]
		print 'out_ = ', out_
		datd = out_[0:1008]
                #data_dump = data_[path][0:1008]
                print 'datd = ', datd
		print 'len of datd = ', len(datd)
		data_[path] = datd
		node_id = self.md5_er(path)
                pick = pickle.dumps(data_)
		server.put(node_id, Binary('data'), Binary(pick), self.ttl)
                n = 1
                i = '1'
		print 'd = ', d
                for d in range(0,(bnts_int -1)):
			print 'inside for loop, d = ', d
                        newpath = path + i
			print 'newpath = ', newpath
                        data_dump = out_[(1008*n):(1008*n+1008)]
			print 'else data_dump = ', data_dump
			data_[newpath] = data_dump
                        node_id = self.md5_er(newpath)
                        pick = pickle.dumps(data_)
                        server.put(node_id, Binary('data'), Binary(pick), self.ttl)
                        #fd_local = self.create(newpath, "rw")
			
			int_i = int(i)  #str to int, increment, then back
                        int_i = int_i + 1       #for concat w/pathname
                        i = str(int_i)

	#server.put(Binary('key'), d, ttl)
	#print "setting key = " + key
	#print "and value = " + value
	#print "valuetype = " + type(value)
	#pickled_dict = pickle.dumps(value)
	#server.put(Binary(key),Binary(pickled_dict), ttl)
	return True

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
	files = self.getRemote('1', 'files')
        attrs = files[path].setdefault('attrs', {})
        attrs[name] = value
	#self.setRemote('files', files, self.ttl)
	pick = pickle.dumps(files)
        server.put('1', Binary('files'), Binary(pick), self.ttl)    

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)
    
    def symlink(self, target, source):
	files = self.getRemote('1', 'files')
        files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
            st_size=len(source))
	#self.setRemote('files', files, self.ttl)
	pick = pickle.dumps(files)
        server.put('1', Binary('files'), Binary(pick), self.ttl)
	
	data_ = self.getdataRemote(target) 
        data_[target] = source
	self.setdataRemote(target, data_, self.ttl)
	#pick = pickle.dumps(data_)
        #server.put(m, Binary('data'), Binary(pick), self.ttl)

    def truncate(self, path, length, fh=None):
        #self.data[path] = self.data[path][:length]
	print 'IN TRUNCATE()'
	print 'length = ', length
	print 'length type = ', type(length)
	data_ = self.getdataRemote(path)
	print 'returning from getdataRemote in truncate()'
	print ' data_ = ', data_
	#buf = data_[path]
	#print 'buf = ', buf
	data_[path] = data_[path][:length]
	print 'data_ = ', data_
	self.setdataRemote(path, data_, self.ttl)
	print 'truncate() get files'	
	files = self.getRemote('1', 'files')
	files[path]['st_size'] = length    
	#self.setRemote('files', files, self.ttl)
	print 'truncate() set files'
	pick = pickle.dumps(files)
        server.put('1', Binary('files'), Binary(pick), self.ttl)
	print 'END TRUNCATE()'
	
    def unlink(self, path):
	files = self.getRemote('1', 'files')
        files.pop(path)
	#self.setRemote('files', files, self.ttl)
	pick = pickle.dumps(files)
        server.put('1', Binary('files'), Binary(pick), self.ttl)    

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        files = self.getRemote('1', 'files')
	files[path]['st_atime'] = atime
        files[path]['st_mtime'] = mtime
	#self.setRemote('files', files, self.ttl)    
	pick = pickle.dumps(files)
        server.put('1', Binary('files'), Binary(pick), self.ttl)

    def write(self, path, data, offset, fh):
        print 'offset in write() = ',offset
	data_ = self.getdataRemote(path)
	print 'back from getdataRemote'
	#self.data[path] = self.data[path][:offset] + data
	#key = self.md5_er(path)
	#pick = pickle.dumps(key)
	if not offset:
		print 'in not offset'
		self.buf = data
		print 'buf = ', self.buf
	if offset:
		print 'in offset'
		self.buf = self.buf + data
		print 'buf = ', self.buf
	print 'data to write to file = ', data
	#data_ = self.getdataRemote(path)
	#with open(path) as output:
		#data_ = self.getdataRemote(path)
	#buf = data_[path].read(1024)
	#print 'buf = ', buf
	print 'returning to WRITE() from getdataRemote'
	print 'data_ = ', data_[path]
	data_[path] = data_[path][:offset] + self.buf	#put this after the last block has been retrieved
	print 'IN WRITE, data size to setdataRemote() = ', len(data_[path])
	print 'data to setdataRemote = ', data_[path]
	print 'len(data_[path]) = ',len(data_[path])
	self.setdataRemote(path, data_, self.ttl)
	print 'len(data_[path])2 = ',len(data_[path])
	#pick = pickle.dumps(data_)
	#server.put(Binary(md5_out), Binary('data'), Binary(pick), self.ttl)
	print 'getRemote files in WRITE()'
	files = self.getRemote('1', 'files')
	files[path]['st_size'] = len(data_[path])
        #self.setRemote('files', files, self.ttl)
	pick = pickle.dumps(files)
        server.put('1', Binary('files'), Binary(pick), self.ttl)
	#self.close(self.fd)	#added
	return len(data)

if __name__ == "__main__":
    if len(argv) != 2:
        print 'usage: %s <mountpoint>' % argv[0]
        exit(1)
    fuse = FUSE(Memory(), argv[1], foreground=True)

