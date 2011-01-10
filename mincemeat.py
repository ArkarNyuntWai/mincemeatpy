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
import traceback

VERSION = 0.0


DEFAULT_PORT = 11235

    
def generator(func):
    """
    Takes a simple function with signature "func(k,[v,...])==>v" or
    "func(k,v)==>v", and turns it into an iterator driven generator
    function suitable for use as a Server.collectfn, .reducefn or
    .finishfn.  Always yields (k,v) tuples.
    """
    def wrapper(itr):
        for k, v in itr:
            yield k, func(k, v)
    return wrapper

def applyover(func, itr):
    """
    Takes a function, which is assumed to either take an iterator
    argument and return a generator, or to be a function over a simple
    key/value(s) pair, in which case we return a generator to apply
    the simple function over the given dictionary item iterator.
    (Ensure that the simple func(k,v) version raises TypeError if
    provided with an iterator.)
    
    This allows the user to supply the older style simple functions,
    or newer style generators that have access to the whole result
    dictionary for the .collectfn after Map, the .reducefn, or the
    .finishfn after Reduce.
    """
    try:
        return func(itr)
    except TypeError:
        return generator(func)(itr)

def loop(map=None):
    """
    Processes asyncore based Server or Client events 'til none left.
    On Exception, forcibly cleans up all sockets using the same
    asyncore map.

    If multiple sets of asyncore based objects use separate maps, then
    each separate map needs to be run using a mincemeat.loop in a
    separate thread.  For example, a single Python instance may run
    both a Server, and one or more Clients, or multiple independent
    Server instances listening on different ports.
    """
    try:
        asyncore.loop(map=map)
    except:
        # We're no longer running asyncore.loop, so can't do anything
        # cleanly; just close 'em all...  This should ensure that any
        # socket resources associated with this object get cleaned up.
        logging.error(traceback.format_exc())
        asyncore.close_all(map=map, ignore_all=True)
        raise

class Protocol(asynchat.async_chat):
    """
    Implements the basic protocol used by mincement Client instances
    (back to one server), and ServerChannel instances (spawned by
    Server for each Client connection).  Implements basic
    challenge/response security, and knows how to freeze-dry and
    reconstitute basic functions and simple lexical closures (only
    closures which do not call external functions).

    Derived classes are expected to implement (at least) handle_close,
    to tidy up any connection state on destruction of the
    communications channel.
    """
    def __init__(self, sock=None, map=None):
        """
        A Map/Reduce client Protocol instance.  Optionally, provide a
        'map' dict shared by all asycore based objects to be activated
        using the same asycore.loop.
        """
        asynchat.async_chat.__init__(self, sock=sock, map=map)
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
        if self.auth != "Done":
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
            if self.auth != "Done":
                logging.fatal("Recieved pickled data from unauthed source")
                # sys.exit(1)
                self.handle_close()
                return
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
        logging.info(command)

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
        Pickle up simple, self-contained functions and closures (or
        functions that call modules/methods that exist in both Server
        and Client environments).
        """
        code_blob = marshal.dumps(fun.func_code)
        name = fun.func_name
        dflt = fun.func_defaults
        clos_tupl = None
        if fun.func_closure:
            clos_tupl = tuple(c.cell_contents for c in fun.func_closure)
        return pickle.dumps((code_blob, name, dflt, clos_tupl),
                             pickle.HIGHEST_PROTOCOL)
        
    def load_func(self, blob, globs):
        """
        Load a pickled function.  Attempts to also handle some simple
        closures. See:
        
            http://stackoverflow.com/questions/573569/python-serialize-lexical-closures

        """
        code_blob, name, dflt, clos_tupl = pickle.loads(blob)
        code = marshal.loads(code_blob)
        clos = None
        if clos_tupl:
            ncells = range(len(clos_tupl))
            src = '\n'.join(
                [ "def _f(arg):" ] +
                [ "  _%d = arg[%d] "     % ( n, n ) for n in ncells ] +
                [ "  return lambda:(%s)" % ','.join( "_%d" %n for n in ncells ) ] +
                [ "" ])
            try:
                exec src
            except:
                raise SyntaxError(src)
            clos = _f(clos_tupl).func_closure

        return new.function(code, globs, name, dflt, clos)
        

class Client(Protocol):
    """
    Connect's to a specified server:port, and processes commands
    (authentication is handled by the Protocol superclass).
    """
    def __init__(self, map=None):
        Protocol.__init__(self, map=map)
        self.mapfn = self.reducefn = self.collectfn = None
        self.closed = False

    def finished(self):
        return self.closed is True
        
    def conn(self, interface='', port=DEFAULT_PORT, password=None,
             asynchronous=False):
        """
        Establish connection, and (optionally) synchronously loop 'til
        all file descriptors closed.  Optionally specifies password.
        Note that order is different than Server.run_server, for
        historical reasons.
        
        If no server port exists to bind to, on Windows the
        select.select() call will return an "exceptional" condition on
        the socket; on *nix, a "readable" condition (and a
        handle_connect()), followed by an error on read (errno 11,
        "Resource temporarily unavailable") and a handle_close().  In
        either case, on the loopback interface, this occurs in ~1
        second.

        Since this connection is performed asynchronously, the invoker
        may want to check that .auth is 'Done' after this call, to
        ensure that we successfully connected to and authenticated a
        server...
        """
        if password is not None:
            self.password = password
        logging.info("Connecting to server at %s:%s" % (interface, port))
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((interface, port))

        if asynchronous is False:
            self.process()

    def process(self):
        """
        Run this Client (and anything else sharing its _map), cleaning
        it up on failure.
        """
        logging.debug("Client processing on map at %s: %s",
                      hex(id(self._map)), repr.repr(self._map))
        loop(self._map)

    def handle_connect(self):
        logging.info("Server connected.")
        pass

    def handle_close(self):
        """
        EOF received, or other communications channel failure
        (eg. authentication failure).
        """
        logging.info("Server connection being closed.")
        self.close()
        self.closed = True

    def set_mapfn(self, command, mapfn):
        self.mapfn = self.load_func(mapfn, globals())

    def set_collectfn(self, command, collectfn):
        self.collectfn = self.load_func(collectfn, globals())

    def set_reducefn(self, command, reducefn):
        self.reducefn = self.load_func(reducefn, globals())

    def call_mapfn(self, command, data):
        """
        Map the data.  In the Map phase, the data is always a
        (name,corpus) pair, and the result of the mapfn is always a
        iterable sequence of (key,value) pairs deduced from the corpus
        (eg. a function returning a list of tuples, or a generator
        yielding tuples).  These tuples are used to construct a
        dictionary containing all keys, and a list of all results with
        the same key:
        
            {
              key1: [ value, value, ... ],
              key2: [ value, value, ... ]
              ...
            }

        The (optional) .collectfn takes a list, and returns a simple
        value (just like a 'reducefn', incidentally); it is therefore
        wrapped to produce a ( key, [ value ] ) tuple, as would be
        produced by the normal Map phase.  We use 'applyover' to
        handle either simple functions operating on each individual
        (key,[value,...])  item, or generators which operate over the
        sequence of all items (and hence may employ remembered state).
        """
        logging.info("Mapping %s" % repr.repr( data[0] ))
        results = {}
        for k, v in self.mapfn(data[0], data[1]):
            if k not in results:
                results[k] = []
            results[k].append(v)

        if self.collectfn:
            # Applies the specified .collectfn, either as an interator based
            # generator, or as a simple function over key/values
            rgen = applyover(self.collectfn, results.iteritems())

            # Use the generator expression, and create a new results
            # dict.  We don't simply update the results in place,
            # because the collectfn may choose to alter the keys
            # (eg. discarding invalid keys, adding new keys).
            results = dict((k, [v]) for k, v in rgen)

        self.send_command('mapdone', (data[0], results))

    def call_reducefn(self, command, data):
        """
        Reduce the data.  In the Reduce phase, the data is always a
        (key,[value,...]) item (ie. one of the results returned from
        the Map phase), and the result is always reduced to a single
        (key,value).  Note that the 'reducefn' has the same signature
        as the '.collectfn', above; it may sometimes be useful to
        apply the same function for both the collectfn (to compress
        each Map result), and the reducefn, or even to skip the Reduce
        phase entirely, and run a trivial Reduce phase entirely in the
        server (see finishfn, below).

        We always operate on a single (key,[value,...]) pair.
        However, in order to stay consistent with the policy of
        allowing either a simple function over a pair or a generator
        over an iterator yielding pairs, we'll use applyover to apply
        the function, and ensure only one result is returned.  This
        allows us to use the same function interchangably for
        collectfn, reducefn or finishfn (as appropriate).
        """
        logging.info( "Reducing %s" % repr.repr( data ))
        rgen = applyover(self.reducefn, [data])
        results = list(rgen)
        if len(results) != 1:
            raise IndexError
        self.send_command('reducedone', results[0])
        
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
    Map or Reduce phase is skipped (not both).
    
    The Server.collectfn and .finishfn are optional, and may take
    either a key/value pair and return a new value, or take an
    iterable over key/value pairs, and return key/value pairs.  They
    may be used to post-process Map values (in the Client), or
    post-process Map/Reduce values (in the Server).

    If the Map phase results in very large intermediate data which are
    trivially compressible only AFTER the Map's 'mapfn' function is
    completed over all the data, then a 'collectfn' may be provided to
    the client, to post-process the data for return to the server.  An
    example is a Map function producing very long lists, such as those
    produced by applying the trivial "word count" example to a very
    large corpus of text; it is much more efficient to compress the
    lists produced by the Map by summing them, than to return the raw
    lists.

    If the Reduce phase is trivial, it may be preferable to simply use
    the defined Server.reducefn as the .finishfn, leaving .reducefn as
    None.  This will result in a single Map trip out to the clients,
    with the (trivial) Reduce being run entirely in the server (after
    all clients have completed).  Again, the trivial "word count"
    example is such a case; the act of summing the list of word counts
    for each word from the Map phase on each text corpus is so trivial
    that the communication overhead of shipping the list out to a
    client node is much greaterh than the cost of simply summing the
    list!  
    
    Creates instances of ServerChannel on-demand, as incoming connect
    requests complete.

    Note that this must be a "new-style" class (hence the extraneous
    class dependency on 'object').  This is required to support
    Properties (see datasource = ..., below)
    """

    def __init__(self, map=None):
        """
        A mincemeat Map/Reduce Server.  Specify a 'map' dict, if other
        asyncore based facilities are implemented, and you wish to run
        this Server with a separate asyncore.loop.  Any ServerChannels
        created due to incoming Client request will also share this
        map.  Every Server, Client (or other asyncore object) which
        uses the same asyncore.loop thread must share the same map.
        """
        asyncore.dispatcher.__init__(self, map=map)
        self.mapfn = None
        self.collectfn = None
        self.reducefn = None
        self.finishfn = None
        self.datasource = None
        self.password = None
        self.taskmanager = None
        self.shutdown = False		# Termination indication to clients

    def run_server(self, password="", port=DEFAULT_PORT, interface='',
                   asynchronous=False):
        """
        Runs the Server.  Use this method in the default asynchronous
        == False form, if and only if this the only asyncore based
        application running in this Python instance!  Otherwise, use
        the component methods (ensure that the caller runs
        asyncore.loop in another thread.)

            s = mincemeat.Server()
            s.datasource = ...
            s.mapfn = ...
            s.collectfn = ...
            s.reducefn = ...
            s.finishfn = ...
            
            s.setup(**credentials)
        
            ac = threading.Thread(target=s.process)
            ac.start()
            while not s.finished():
                ac.join(.1)
            ac.join()

            results = s.results()

        Note that using asynchronous = True requires that the caller
        has created at least one asyncore based object (or the
        asyncore.loop() call will return instantly, and the Thread
        will terminate...
        """
        self.conn( password=password, port=port, interface=interface,
                   asynchronous=asynchronous)

        # If we are asynchronous, we have NOT initiated processing;
        # caller must wait 'til we are finished() (or fail) before
        # returning results; there are none to return, yet (use
        # .finished() to discover if they are ready)
        if asynchronous:
            return None

        return self.results()

    def conn(self, password="", port=DEFAULT_PORT, interface='',
              asynchronous=False):
        """
        Establish this Server, allowing Clients to connect to it.
        This will allow exactly one Server bound to a specific port to
        accept incoming Client connections.

        If asynchronous, then we will not initiate processing; it is
        the caller's responsibility to do so; every Client and/or
        Server with a shared map=... require only one loop(obj._map)
        or obj.process() thread.
        """
        self.password = password
        logging.debug("Server port opening.")
        try:
            self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
            if hasattr(socket, 'SO_EXCLUSIVEADDRUSE'):
                # Windows socket re-use semantics differ from *nix (read:
                # are broken).  See http://goo.gl/J89cr
                self.socket.setsockopt( socket.SOL_SOCKET,
                                        socket.SO_EXCLUSIVEADDRUSE, 1)
            self.bind((interface, port))
            self.listen(1)
        except:
            # If anything fails during socket creation, we need to
            # ensure we clean up the partially opened socket;
            # otherwise, it'll leave a busted entry in
            # asyncore.socket_map, which will prevent asyncore.loop()
            # from working correctly.
            logging.error( "Server couldn't bind to %s" % str( ( interface, port ) ))
            #logging.error(traceback.format_exc())
            self.close()
            raise

        # If either the Map or Reduce functions are empty, direct the
        # TaskManager to skip that phase.  Since Server.reducefn and
        # .mapfn are not set at Server.__init__ time, we must defer
        # 'til .setup is invoked, to detect if these are provided.
        if self.reducefn is None:
            self.taskmanager.tasks = TaskManager.MAPONLY
        elif self.mapfn is None:
            self.taskmanager.tasks = TaskManager.REDUCEONLY

        if asynchronous is False:
            self.process()

    def process(self):
        """
        Run this Server (and anything else sharing its _map), cleaning
        it up on failure.
        """
        logging.debug("Server processing on map at %s: %s",
                      hex(id(self._map)), repr.repr(self._map))
        loop(self._map)

    def finished(self):
        """
        Detect if finished.  If no self.taskmanager, self.datasource
        has not been set, hence not finished.
        """
        return self.taskmanager \
            and self.taskmanager.state == TaskManager.FINISHED

    def results(self):
        # Successfully completed Map/Reduce.  If .finishfn supplied,
        # support either .finishfn(iterator), or .finishfn(key,value),
        # apply it -- the resultant values are assumed to be finished
        # results, and are NOT encapsulated as a list.
        results = self.taskmanager.results
        if self.finishfn:
            # Create a finished result dictionary by applying the
            # supplied Server.finishfn over the Reduce results.  
            rgen = applyover(self.finishfn, results.iteritems())
            results = dict(rgen)
        return results

    def handle_accept(self):
        """
        Accept a new client connection, spawing a channel to handle
        it.  This initiates the authentication procedure, and should
        (eventually) result in the channel asking for tasks from this
        server's taskmanager.  We'll use the TaskManager to monitor
        the pool of available channels.  Ensure we handle accept()
        error cases (see http://bugs.python.org/issue6706)
        """
        try:
            sock, addr = self.accept()
        except TypeError:
            # sometimes accept() might return None (see issue 91)
            return
        except socket.error, err:
            # ECONNABORTED might be thrown on *BSD (see issue 105)
            if err[0] != errno.ECONNABORTED:
                logging.error(traceback.format_exc())
            return
        else:
            # sometimes addr == None instead of (ip, port) (see issue 104)
            if addr == None:
                return

        sc = ServerChannel(sock, addr, self)
        sc.password = self.password

    def tidy_close(self):
        """
        We are still running, and wish to perform a clean shutdown;
        the asyncore.loop will continue running 'til all sockets
        report closure.  First, simply close our server port so we
        cease accepting new client connections.  Then, by using
        shutdown (on the client connections), we'll close the outgoing
        half, and wait 'til the EOF trickles through to the client.
        After it closes, we'll receive an EOF, which will trigger
        handle_close within the ServerChannel.  After all are closed,
        asyncore.loop will terminate.  Signal shutdown, so any future
        (eg. not yet authenticated) ServerChannels will know to
        perform a tidy_close.
        """
        if self.shutdown is False:
            logging.debug("Server port closing.")
            self.shutdown = True
            self.close()
            logging.debug("Client connections being tidily closed...")
            if self.taskmanager:
                for chan in self.taskmanager.channels.keys():
                    chan.tidy_close()

    def handle_close(self):
        """
        EOF (or other failure) on our socket.  We have a chance to
        tidy up nicely.  Arrange to send an EOF on all clients by
        using socket.shutdown(SHUT_WR) to close the outbound half file
        descriptor.  This will cause them to finish up their current
        command, send the result, receive EOF, and close nicely.
        """
        self.tidy_close()

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
    B) Process tasks, by
    C)   getting one from the TaskManager (going idle if None)
    D)   sending it to the client
    E) When a 'mapdone' or 'reducedone' is received, go to B
    F) If EOF encountered, close session

    If a channel goes idle, invoke channel.start_new_task() to start
    it up, by force it to go get a new task.
    """
    def __init__(self, sock, addr, server):
        Protocol.__init__(self, sock = sock, map = server._map)
        self.peer = addr
        self.server = server
        self.shutdown = False
        logging.info( "Client %s connected" % ( str( self.peer )))
        self.start_auth()

    def tidy_close(self):
        """
        Indicate completion to client, by closing outgoing half of
        socket.  This will result in an EOF to the client, after the
        current operation is complete, eventually leading to a
        handle_close here.  See http://goo.gl/EtAyN for a confirmation of
        these semantics for Windows.
        """
        if self.shutdown is False:
            self.shutdown = True
            logging.info("Client %s shut down" % str( self.peer ))
            # If the socket has already ceased to exist, don't fail!
            try:    self.socket.shutdown(socket.SHUT_WR)
            except: pass

    def handle_close(self):
        logging.info( "Client %s disconnected" % str( self.peer ))
        self.server.taskmanager.channel_closed(self)
        self.close()

    def start_auth(self):
        self.send_challenge()

    def start_new_task(self):
        if self.server.shutdown:
            self.tidy_close()
            return
        command, data = self.server.taskmanager.next_task(self)
        if command == None:
            logging.info("Client %s idle" % ( self.peer ))
            return
        self.send_command(command, data)

    def map_done(self, command, data):
        self.server.taskmanager.map_done(data)
        self.start_new_task()

    def reduce_done(self, command, data):
        self.server.taskmanager.reduce_done(data)
        self.start_new_task()

    def send_command(self, command, data = None):
        self.server.taskmanager.channel_sending(self, command)
        Protocol.send_command(self, command, data)

    def process_command(self, command, data=None):
        commands = {
            'mapdone': self.map_done,
            'reducedone': self.reduce_done,
            }

        self.server.taskmanager.channel_process(self, command)
        
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
        self.server.taskmanager.channel_opened(self)
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
                 tasks=None, allocation=None, cycle=None):
        self.datasource = datasource
        self.server = server
        self.state = TaskManager.START
        self.tasks = tasks or TaskManager.MAPREDUCE
        self.allocation = allocation or TaskManager.CONTINUOUS
        self.cycle = cycle or TaskManager.SINGLEUSE

        # Track what channels were last reported as being up to
        # { addr: (command, timetamp), ... }
        self.channels = {}

    # 
    # channel_... -- maintain client .channels activity state
    # 
    #     Tracks the command, response and time started for every 
    # request.  If idle, the entry is None.
    # 
    #     .channels = {
    #         ('127.0.0.1, 12345): ('map', 'mapdone', 1234.5678 ),
    #         ('127.0.0.1, 23456): ('map', None, 1235.6789 ),
    #         ...
    #     }
    # 
    def channel_opened(self, chan):
        self.channel_idle(chan)

    def channel_closed(self, chan):
        self.channel_log(chan, "Disconnecting")
        self.channels.pop(chan, None)

    def channel_idle(self, chan):
        self.channel_log(chan, "Idle")
        self.channels[chan] = None

    def channel_sending(self, chan, command):
        self.channels[chan] = (command, None, time.time())
        self.channel_log(chan, "Sending")

    def channel_process(self, chan, response):
        try:
            command, __, started = self.channels[chan]
        except:
            command = None
            started = time.time()
        self.channels[chan] = (command, response, started)
        self.channel_log(chan, "Processing")

    def channel_log(self, chan, what):
        if chan is None:
            # No chan; Just print header
            logging.debug('Client Address        Command Response Time State')
            return
        try:
            triplet = self.channels[chan]
            if triplet is None:
                logging.debug('Client %16.16s:%-5d %8s %8s %6.3fs: %s' % (
                        chan.peer[0], chan.peer[1], 
                        '','', 0.0, what))
            else:
                command, response, when = triplet
                logging.debug('Client %16.16s:%-5d %8s %8s %6.3fs: %s' % (
                        chan.peer[0], chan.peer[1],
                        command, response, time.time() - when, what))
        except KeyError:
            logging.debug('Client %16.16s:%-5d Unknown' % (
                    chan.peer[0], chan.peer[1]))

    def next_task(self, channel):
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
                # A complete iteration of map items is done; either
                # pick a random one of those not yet complete to
                # re-do, or let the client go idle.  
                if self.allocation is self.CONTINUOUS:
                    if len(self.working_maps) > 0:
                        key = random.choice(self.working_maps.keys())
                        return ('map', (key, self.working_maps[key]))
                else:
                    return (None, None)

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
            self.server.tidy_close()
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
        logging.debug( "Reduce Done: %s ==> %s" % ( data[0], repr.repr( data[1] )))
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
