#!/usr/bin/env python
"""
example of building a simple xmlrpc server and proxy,
and then demonstrate the handling of a few basic requests

To run: python xmlrpc_server.py
"""

from pathos.XMLRPCServer import XMLRPCServer


if __name__ == '__main__':
    
    import os, time, xmlrpclib

    s = XMLRPCServer('', 0)
    print 'port=%d' % s.port
    port = s.port

    pid = os.fork()
    if pid > 0: #parent
        def add(x, y): return x + y
        s.register_function(add)
        s.activate()
        #s._selector._info.activate()
        s.serve()
    else: #child
        time.sleep(1)
        s = xmlrpclib.ServerProxy('http://localhost:%d' % port)
        print '1 + 2 =', s.add(1, 2)
        print '3 + 4 =', s.add(3, 4)

# End of file
