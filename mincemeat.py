#!/usr/bin/env python


################################################################################
# Copyright (c) 2010 Michael Fairley
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
################################################################################

import asynchat
import asyncore
import cPickle as pickle
import hashlib
import hmac
import logging
import marshal
import optparse
import os
import random
import socket
import sys
import types
import repr
import new
import time

VERSION = 0.0


DEFAULT_PORT = 11235

    
def generator( function ):
    """
    Decorator takes a simple function with signature "function( key, [
    value, ...] ) ==> value", and turns it into an iterator driven
    generator function suitable for use as a Server.collectfn or
    Server.finishfn (yields (k,v) tuples.)
    """
    def decorator( kvi ):
        for k, v in kvi:
            yield k, function( k, v )
    return decorator

def applyover( function, iterator ):
    """
    Takes a function, which may take either an iterator argument or a
    simple key/value(s) pair, and returns a generator over the given
    dictionary item iterator.
    
    This allows the user to supply the older style simple functions,
    or newer style generators that have access to the whole result
    dictionary for the .collectfn after Map, or the .finishfn after
    Reduce.
    """
    try:
        return function( iterator )
    except TypeError:
        return generator( function )( iterator )
        

class Protocol(asynchat.async_chat):
    """
    Implements the basic protocol used by mincement Client instances
    (back to one server), and ServerChannel instances (spawned by
    Server for each Client connection).  Implements basic
    challenge/response security, and knows how to freeze-dry and
    reconstitute basic functions and simple lexical closures (only
    closures which do not call external functions).
    """
    def __init__(self, conn=None):
        if conn:
            asynchat.async_chat.__init__(self, conn)
        else:
            asynchat.async_chat.__init__(self)

        self.set_terminator("\n")
        self.buffer = []
        self.auth = None
        self.mid_command = False

    def collect_incoming_data(self, data):
        self.buffer.append(data)

    def send_command(self, command, data=None):
        if not ":" in command:
            command += ":"
        if data:
            pdata = pickle.dumps(data)
            command += str(len(pdata))
            logging.debug( "<- %s( %s )" % ( command, repr.repr( data )))
            self.push(command + "\n" + pdata)
        else:
            logging.debug( "<- %s" % command)
            self.push(command + "\n")

    def found_terminator(self):
        if not self.auth == "Done":
            command, data = (''.join(self.buffer).split(":",1))
            self.process_unauthed_command(command, data)
        elif not self.mid_command:
            logging.debug("-> %s" % ''.join(self.buffer))
            command, length = (''.join(self.buffer)).split(":", 1)
            if command == "challenge":
                self.process_command(command, length)
            elif length:
                self.set_terminator(int(length))
                self.mid_command = command
            else:
                self.process_command(command)
        else: # Read the data segment from the previous command
            if not self.auth == "Done":
                logging.fatal("Recieved pickled data from unauthed source")
                sys.exit(1)
            data = pickle.loads(''.join(self.buffer))
            self.set_terminator("\n")
            command = self.mid_command
            self.mid_command = None
            self.process_command(command, data)
        self.buffer = []

    def send_challenge(self):
        self.auth = os.urandom(20).encode("hex")
        self.send_command(":".join(["challenge", self.auth]))

    def respond_to_challenge(self, command, data):
        mac = hmac.new(self.password, data, hashlib.sha1)
        self.send_command(":".join(["auth", mac.digest().encode("hex")]))
        self.post_auth_init()

    def verify_auth(self, command, data):
        mac = hmac.new(self.password, self.auth, hashlib.sha1)
        if data == mac.digest().encode("hex"):
            self.auth = "Done"
            logging.info("Authenticated other end")
        else:
            self.handle_close()

    def respond_to_ping(self, command, data):
        self.send_command(':'.join(['pong', 'Reply from %s' % os.gethostname()]))

    def pong(self, command, data):
        logging.info( command )

    def process_command(self, command, data=None):
        commands = {
            'ping': self.respond_to_ping,
            'pong': self.pong,
            'challenge': self.respond_to_challenge,
            'disconnect': lambda x, y: self.handle_close(),
            }

        if command in commands:
            commands[command](command, data)
        else:
            logging.critical("Unknown command received: %s" % (command,)) 
            self.handle_close()

    def process_unauthed_command(self, command, data=None):
        commands = {
            'challenge': self.respond_to_challenge,
            'auth': self.verify_auth,
            'disconnect': lambda x, y: self.handle_close(),
            }

        if command in commands:
            commands[command](command, data)
        else:
            logging.critical("Unknown unauthed command received: %s" % (command,)) 
            self.handle_close()
        
    def store_func(self, fun):
        """
        Package up simple, self-contained functions (or functions that
        call modules/methods that exist in both Server and Client
        environments).
        """
        code_blob = marshal.dumps( fun.func_code )
        name = fun.func_name
        dflt = fun.func_defaults
        clos_tupl = None
        if fun.func_closure:
            clos_tupl = tuple( c.cell_contents for c in fun.func_closure )
        return pickle.dumps( ( code_blob, name, dflt, clos_tupl ),
                             pickle.HIGHEST_PROTOCOL )
        
    def load_func(self, blob, globs):
        """
        Load a pickled function.  Attempts to also handle some simple
        closures. See:
        
            http://stackoverflow.com/questions/573569/python-serialize-lexical-closures

        """
        code_blob, name, dflt, clos_tupl = pickle.loads( blob )
        code = marshal.loads( code_blob )
        clos = None
        if clos_tupl:
            ncells = range( len( clos_tupl ))
            src = '\n'.join(
                [ "def _f(arg):" ] +
                [ "  _%d = arg[%d] "     % ( n, n ) for n in ncells ] +
                [ "  return lambda:(%s)" % ','.join( "_%d" %n for n in ncells ) ] +
                [ "" ]
              )
            try:
                exec src
            except:
                raise SyntaxError( src )
            clos = _f( clos_tupl ).func_closure

        return new.function( code, globs, name, dflt, clos )
        

class Client(Protocol):
    """
    Connect's to a specified server:port, and processes commands
    (authentication is handled by the Protocl superclass).
    """
    def __init__(self):
        Protocol.__init__(self)
        self.mapfn = self.reducefn = self.collectfn = None
        
    def conn(self, server, port):
        logging.info( "Connecting to server at %s:%s" % ( server, port ))
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((server, port))
        asyncore.loop()

    def handle_connect(self):
        logging.info( "Server connected." )
        pass

    def handle_close(self):
        self.close()

    def set_mapfn(self, command, mapfn):
        self.mapfn = self.load_func( mapfn, globals() )

    def set_collectfn(self, command, collectfn):
        self.collectfn = self.load_func( collectfn, globals() )

    def set_reducefn(self, command, reducefn):
        self.reducefn = self.load_func( reducefn, globals() )

    def call_mapfn(self, command, data):
        """
        In the Map phase, the result is always:
        
            {
              key: [ value, value, ... ],
              key: [ value, value, ... ]
              ...
            }

        Therefore, since result of the .collectfn is a simple value,
        it must be wrapped to produce a ( key, [ value ] ) tuple, as
        would be produced by the normal Map phase.
        """
        logging.info("Mapping %s" % str(data[0]))
        results = {}
        for k, v in self.mapfn(data[0], data[1]):
            if k not in results:
                results[k] = []
            results[k].append(v)

        if self.collectfn:
            # Applies the specified .collectfn, either as an interator based
            # generator, or as a simple function over key/values
            rgen = applyover( self.collectfn, results.iteritems() )

            # Use the generator expression, and create a new results
            # dict.  We don't simply update the results in place,
            # because the collectfn may choose to alter the keys
            # (eg. discarding invalid keys, adding new keys).
            results = dict( ( k, [ v ] ) for k, v in rgen )

        self.send_command('mapdone', (data[0], results))

    def call_reducefn(self, command, data):
        logging.info("Reducing %s" % str(data[0]))
        results = self.reducefn(data[0], data[1])
        self.send_command('reducedone', (data[0], results))
        
    def process_command(self, command, data=None):
        commands = {
            'mapfn': self.set_mapfn,
            'collectfn': self.set_collectfn,
            'reducefn': self.set_reducefn,
            'map': self.call_mapfn,
            'reduce': self.call_reducefn,
            }

        if command in commands:
            commands[command](command, data)
        else:
            Protocol.process_command(self, command, data)

    def post_auth_init(self):
        if not self.auth:
            self.send_challenge()


class Server(asyncore.dispatcher, object):
    """
    Server -- Distributes Map-Reduce tasks to authenticated clients.
    
    The Map phase allocates a source key/value pair to a client, which
    applies the given Server.mapfn/.collectfn, and returns the
    resultant keys, each with their list of values.  Next, each of the
    Map key/values lists are allocated to a Reduce client, which
    applies the supplied .reducefn.  Finally, the server applies any
    given .finishfn.
    
    If exactly ONE of Server.mapfn or .reducefn is not supplied, the
    Map or Reduce phase is skipped.
    
    The Server.collectfn and .finishfn are optional, and may take
    either a key/value pair and return a new value, or take an
    iterable over key/value pairs, and return key/value pairs.  They
    may be used to post-process Map values (in the Client), or
    post-process Map/Reduce values (in the Server).  

    If the Reduce phase is trivial, it may be preferable to simply use
    the defined Server.reducefn as the .finishfn, leaving .reducefn as
    None.
    
    Creates instances of ServerChannel on-demand, as incoming connect
    requests complete.
    """
    def __init__(self):
        asyncore.dispatcher.__init__(self)
        self.mapfn = None
        self.reducefn = None
        self.collectfn = None
        self.datasource = None
        self.password = None

    def run_server(self, password="", port=DEFAULT_PORT):
        self.password = password
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.bind(("", port))
        self.listen(1)

        # If either the Map or Reduce functions are empty, direct the
        # TaskManager to skip that phase.  Since Server.reducefn and
        # .mapfn are not set at Server.__init__ time, we must defer
        # 'til run_server is invoked, to detect if these are provided.
        if self.reducefn is None:
            self.taskmanager.tasks = TaskManager.MAPONLY
        elif self.mapfn is None:
            self.taskmanager.tasks = TaskManager.REDUCEONLY

        try:
            asyncore.loop()
        except:
            self.close_all()
            raise

        # Successfully completed Map/Reduce.  If .finishfn supplied,
        # support either .finishfn(iterator), or .finishfn(key,value)
        results = self.taskmanager.results
        if self.finishfn:
            # Create a finished result dictionary by applying the
            # supplied Server.finishfn over the Reduce results.
            rgen = applyover( self.finishfn, results.iteritems() )
            results = dict( ( k, v ) for k, v in rgen )
        return results

    def handle_accept(self):
        """
        Accept a new client connection, spawing a channel to handle
        it.  This initiates the authentication procedure, and should
        (eventually) result in the channel asking for tasks from this
        server's taskmanager.  We'll let TaskManager collect and
        manage the pool of available channels.  Ensure we handle
        accept() error cases (see http://bugs.python.org/issue6706)
        """
        try:
            sock, addr = self.accept()
        except TypeError:
            # sometimes accept() might return None (see issue 91)
            return
        except socket.error, err:
            # ECONNABORTED might be thrown on *BSD (see issue 105)
            if err[0] != errno.ECONNABORTED:
                logging.error( traceback.format_exc() )
            return
        else:
            # sometimes addr == None instead of (ip, port) (see issue 104)
            if addr == None:
                return

        logging.debug( "Client connected: %s, %s" % ( repr.repr( sock ), repr.repr( addr )))
        sc = ServerChannel( sock, addr, self )
        sc.password = self.password

    def handle_close(self):
        """
        Close the Server's port.  This will prevent new clients from
        connecting, but won't close all the individual client
        connections serviced by all the ServerChannels.
        """
        self.close()

    def set_datasource(self, ds):
        self._datasource = ds
        self.taskmanager = TaskManager(self._datasource, self)
    
    def get_datasource(self):
        return self._datasource

    datasource = property(get_datasource, set_datasource)


class ServerChannel(Protocol):
    """
    ServerChannel -- Handles Server connection to each Client

    Each channel authenticates the client, and proceeds to obtain
    tasks from its server's TaskManager and send them to the client.
    When (and if) the task completes, its results are reported back to
    the server's TaskManager, and another task requested.

    The default behaviour for using a client is to:
    A) authenticate
    B) Start a task, by
    C)   getting it from the TaskManager (going idle if None)
    D)   sending it to the client
    E) When a 'mapdone' or 'reducedone' is received, go to B
    F) If EOF encountered, close session

    If a channel goes idle, invoke channel.start_new_task() to start
    it up, by force it to go get a new task.
    """
    def __init__(self, sock, addr, server):
        Protocol.__init__(self, sock)
        self.peer = addr
        self.server = server
        self.start_auth()

    def handle_close(self):
        logging.info("Client disconnected")
        self.server.taskmanager.channels.pop( self.peer, None )
        self.close()

    def start_auth(self):
        self.send_challenge()

    def start_new_task(self):
        command, data = self.server.taskmanager.next_task(self)
        if command == None:
            return
        self.send_command(command, data)

    def map_done(self, command, data):
        self.server.taskmanager.map_done(data)
        self.start_new_task()

    def reduce_done(self, command, data):
        self.server.taskmanager.reduce_done(data)
        self.start_new_task()

    def send_command(self, command, data = None):
        self.server.taskmanager.channels[self.peer] = ( command, time.time() )
        Protocol.send_command(self, command, data)

    def process_command(self, command, data=None):
        commands = {
            'mapdone': self.map_done,
            'reducedone': self.reduce_done,
            }

        try:
            pair = self.server.taskmanager.channels[self.peer]
            if pair is None:
                logging.debug( 'Processing %-12s (no command sent!)' % ( command ))
            else:
                last, when = pair
                logging.debug( 'Processing %-12s (%6.3fs since %s)' % ( command, time.time() - when, last ))
        except KeyError:
            logging.debug( 'Processing %16s (channel not known!)' % ( command ))
        self.server.taskmanager.channels[self.peer] = None
        
        if command in commands:
            commands[command](command, data)
        else:
            Protocol.process_command(self, command, data)


    def post_auth_init(self):
        if self.server.mapfn:
            self.send_command('mapfn', self.store_func( self.server.mapfn ))
        if self.server.reducefn:
            self.send_command('reducefn', self.store_func( self.server.reducefn ))
        if self.server.collectfn:
            self.send_command('collectfn', self.store_func( self.server.collectfn ))
        self.start_new_task()
    
class TaskManager:
    """
    Produce a stream of Map/Reduce tasks for all requesting
    ServerChannel channels. 

    Normally, the default TaskManager .tasks is MAPREDUCE, and
    .allocation to CONTINUOUS, meaning that each channel will receive
    a continous stream of all available 'map' tasks, followed by all
    available 'reduce' tasks.
    
    After all available 'map' tasks have been assigned to a client,
    any 'map' tasks not yet reported as complete will be
    (duplicately!) re-assigned to the next Client who asks.  This
    takes care of stalled or failed clients.

    When all 'map' tasks have been reported as completed (any
    duplicate responses are ignored), then the 'reduce' tasks are
    assigned to the following next_task clients.

    Finally, once all Map/Reduce tasks are completed, the clients are
    given the 'disconnect' task.

    """
    # Possible .state
    START	= 0
    MAPPING	= 1
    REDUCING	= 2
    FINISHED	= 3

    # Possible .tasks option
    MAPREDUCE	= 0
    MAPONLY	= 1		# Only perform the Map phase
    REDUCEONLY	= 2		# Only perform the Reduce phase

    # Possible .allocation option
    CONTINUOUS	= 0		# Continuously allocate tasks to every channel
    ONESHOT	= 1		# Only allocate a single Map/Reduce task to each

    # Possible .cycles options
    SINGLEUSE   = 0		# After finishing, close Server and 'disconnect' clients
    PERMANENT   = 1		# Go idle 'til another Map/Reduce starts

    def __init__(self, datasource, server,
                 tasks = None, allocation = None, cycle = None ):
        self.datasource = datasource
        self.server = server
        self.state = TaskManager.START
        self.tasks = tasks or TaskManager.MAPREDUCE
        self.allocation = allocation or TaskManager.CONTINUOUS
        self.cycle = cycle or TaskManager.SINGLEUSE

        # Track what channels were last reported as being up to
        # { peer: (command, timetamp), ... }
        self.channels = {}

    def next_task(self, channel):

        for a,c in self.channels.items():
            logging.debug( "%20s: %s" % ( a, c ))
        if self.state == TaskManager.START:
            self.map_iter = iter(self.datasource)
            self.working_maps = {}
            self.map_results = {}
            self.state = TaskManager.MAPPING
            if self.tasks is TaskManager.REDUCEONLY:
                # If Reduce only, skip the Map phase, passing source
                # key/value pairs straight to the Reduce phase.
                self.reduce_iter = self.map_iter
                self.working_reduces = {}
                self.result = {}
                self.stats = TaskManager.REDUCING

        if self.state == TaskManager.MAPPING:
            try:
                map_key = self.map_iter.next()
                map_item = map_key, self.datasource[map_key]
                self.working_maps[map_item[0]] = map_item[1]

                return ('map', map_item)
            except StopIteration:
                if len(self.working_maps) > 0:
                    key = random.choice(self.working_maps.keys())
                    return ('map', (key, self.working_maps[key]))
                # No more entries left to Map; begin Reduce (or skip)
                self.state = TaskManager.REDUCING
                self.reduce_iter = self.map_results.iteritems()
                self.working_reduces = {}
                self.results = {}
                if self.tasks is TaskManager.MAPONLY:
                    # Skip Reduce phase, passing the key/value pairs
                    # output by Map straight to the result.
                    self.results = self.map_results
                    self.state = TaskManager.FINISHED

        if self.state == TaskManager.REDUCING:
            try:
                reduce_item = self.reduce_iter.next()
                self.working_reduces[reduce_item[0]] = reduce_item[1]
                return ('reduce', reduce_item)
            except StopIteration:
                if len(self.working_reduces) > 0:
                    key = random.choice(self.working_reduces.keys())
                    return ('reduce', (key, self.working_reduces[key]))
                # No more entries left to Reduce; finish
                self.state = TaskManager.FINISHED

        if self.state == TaskManager.FINISHED:
            # Stop accepting new Client connections, and send a
            # 'disconnect' to each client that asks for a new task.
            # 
            self.server.handle_close()
            return ('disconnect', None)
    
    def map_done(self, data):
        # Don't use the results if they've already been counted
        if not data[0] in self.working_maps:
            return
        logging.debug( "Map Done: %s ==> %s" % ( data[0], repr.repr( data[1] )))
        for (key, values) in data[1].iteritems():
            if key not in self.map_results:
                self.map_results[key] = []
            self.map_results[key].extend(values)
        del self.working_maps[data[0]]
                                
    def reduce_done(self, data):
        # Don't use the results if they've already been counted
        if not data[0] in self.working_reduces:
            return
        logging.debug( "Reduce Done: %sfg ==> %s" % ( data[0], repr.repr( data[1] )))
        self.results[data[0]] = data[1]
        del self.working_reduces[data[0]]

def run_client():
    parser = optparse.OptionParser(usage="%prog [options]", version="%%prog %s"%VERSION)
    parser.add_option("-p", "--password", dest="password", default="", help="password")
    parser.add_option("-P", "--port", dest="port", type="int", default=DEFAULT_PORT, help="port")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true")
    parser.add_option("-V", "--loud", dest="loud", action="store_true")

    (options, args) = parser.parse_args()
                      
    if options.verbose:
        logging.basicConfig(level=logging.INFO)
    if options.loud:
        logging.basicConfig(level=logging.DEBUG)

    client = Client()
    client.password = options.password
    client.conn( len(args) > 0 and args[0] or "localhost", options.port)
                      

if __name__ == '__main__':
    run_client()
