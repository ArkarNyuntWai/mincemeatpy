#!/usr/bin/env python
import mincemeat
import collections
import glob
import logging
import os
import repr
import socket
import errno
import asyncore
import threading
import time
import traceback
import select
import sys

"""
example-sf-repl         -- Client reads/schedules requests; Spawns Server if needed

    To run this test, simply start an instances of this script:

        python example-sf-repl.py

Each client may schedule Map-Reduce transactions to the Server, and
receive the results of those transactions sometime in the future.  Of
course, each Client also receives map and reduce tasks from the
Server.
"""

class file_contents(object):
    def __init__(self, pattern ):
        self.text_files = glob.glob( pattern )

    def __len__(self):
        return len(self.text_files)

    def __iter__(self):
        return iter(self.text_files)

    def __getitem__(self, key):
        f = open(key)
        try:
            return f.read()
        finally:
            f.close()

# Obtain CD ISO from: http://www.gutenberg.org/cdproject/pgsfcd-032007.zip.torrent
datasource = file_contents( '../Gutenberg SF CD/Gutenberg SF/*.txt' )

# 
# Map Functions.
# 
#     Take a name and corpus of data, and map it onto an iterable of
# (key,value) pairs.
# 
def get_lower_split( name, corpus ):
    import string
    logging.debug( "Corpus: %-40s: %d bytes" %( name, len( corpus )))
    for line in corpus.split("\n"):
        for word in line.replace('--',' ').split():
            word = word.lower().strip(string.punctuation+
                                      string.whitespace+
                                      string.digits)
            if "'" in word:
                for suffix in [ "'s", "'ll", "'d", "'ve" ]:
                    if word.endswith( suffix ):
                        word = word[:-len( suffix )]
            if word:
                yield word, 1


def get_lower_simple( k, v ):
    for w in v.split():
        yield w.lower(), 1

# 
# Collect, Reduce, or Finish Functions.
# 
#     Take (key,value) or (key,[value,...]) pairs, or an iterable
# producing such, and return the single value mapped to that key.  The
# functional version returns just the value; the iterable version must
# return the (key,value) pair.
# 
#     If the function is resilient to taking a value that is either an
# iterable OR is a single value, then the same function may be used
# for any of the Collect, Reduce or Finish functions.  Collect and
# Reduce will always be provided with (key,[value,...]) arguments;
# Finish may be provided with (key,[value,...]) OR (key,value).  Try
# isistance(vs,list) or hasattr(vs,'__iter__'), or use functions that
# throw TypeError on non-iterables, and catch the exception.
# 
def sum_values( k, vs ):
    try:
        return sum( vs )                # Will throw unless vs is iterable, summable
    except TypeError:
        return vs

def sum_values_generator( kvi ):
    for k, vs in kvi:
        try:
            yield k, sum( vs )          # Will throw unless vs is iterable, summable
        except TypeError:
            yield k, vs


# 
# Map Phase
# 
#     Each Map client runs a full pass of mapfn over the incoming
# data, followed (optionally) by a pass of collectfn over all values
# for each Map data_key:
# 
# mapfn( source_key, data )
#   --> { map_key1: [ value, ...] ), map_key2: [ value, ...], ... }
# collectfn( map_key1, [ value, value ] )
#   --> data_key1: [ value ]
# 
#     The optional collectfn would be appropriate to (for example)
# reduce the communication payload size (eg. store the map data in
# some global filesystem, and instead return the filesystem path.)
# 
#     Or, if the mapfn is simple (doesn't retain information about the
# data corpus), the collectfn might collapse information about the
# result values.  For example, in the simple "word count" example, the
# mapfn returns lists of the form [ 1, 1, 1, ...., 1 ].  Instead of
# transmitting this, we should use the collect function to sum these
# counters, returning a list with a single value.
# 
#     The .collectfn may take a (key, values) tuple (must be a scalar,
# eg. int, string and an iterable, eg. list), and return a single
# scalar value, which will be returned as a single-entry list.  Or, it
# may take an iterator producing the key, values tuples, and must
# return an (key, values) list of the same types (eg. a scalar key,
# and an iterable value).
# 
mapfn = get_lower_split

# When the map function produces non-optimal results, it may be
# desirable to run a collect phase, to post-process the results before
# returning them to the server.  For example, the trivial map function
# for word counting produces a (very long) list of the form [1, 1,
# ..., 1]; it might be desirable to sum this list before returning.  A
# less contrived example might post-process the entire set of keys
# produced by the map; a generator-style collect function can retain
# state between invocations with each key, and may decide to modify
# (or even skip) keys, or return return new/additional keys.  Try
# setting collectfn to sum_values or sum_values_generator to see the
# differences in the results of the map (dramatically smaller returned
# lists)

#collectfn = None
collectfn = sum_values
#collectfn = sum_values_generator


# 
# Reduce Phase
# 
#     The Reduce phase takes the output of Map:
# 
#          mapped[key] = [ value, value, ... ]
# 
# data, and produces:
# 
#         result[key] = value
# 
# If no Server.reducefn is supplied, then the Reduce phase is skipped,
# and the mapped data is passed directly to the result:
# 
#         result[key] = [ value, value, ... ]
# 
# Therefore, any supplied Server.finishfn() must be able to handle
# either a scalar value (indicating that Reduce has completed), or
# sequence values (indicating that the Reduce phase was skipped.)

# NOTE: In the case where the reduce function is trivial (such as in
# the word counting example), it will take *significantly* longer to
# run this test, than if you specify None for reducefn, and (instead)
# use the finishfn to run the entire reduce phase in the server...
# The results should be identical.  To see the difference, try
# changing reducefn to None, and setting finishfn to sum_values or
# sum_values_generator.

# Skip the Reduce phase; use the Reduce function as Server.finishfn
reducefn = None
#reducefn = sum_values
#reducefn = sum_values_generator

#finishfn = None
finishfn = sum_values
#finishfn = sum_values_generator


# 
# Result Callback
# 
#     Instead of monitoring the Server for completion, an optional
# resultfn callback may be provided, which is invoked by the Server
# immediately with the final results upon completion.
# 

global_results = collections.deque()

def store_results(results):
    global_results.append( results )

def server_results(results):
    # Map-Reduce over 'datasource' complete.  Enumerate results,
    # ordered both lexicographically and by count
    bycount = {}
    for k,v in results.items():
        if v in bycount:
            bycount[v].append(k)
        else:
            bycount[v] = [k]
    
    bycountlist = []
    for k,l in sorted(bycount.items()):
        for w in sorted(l):
            bycountlist.append((k, w))
    
    for k, lt in zip(sorted(results.keys()), bycountlist):
        print "%8d %-40.40s %8d %s" % (results[k], k, lt[0], lt[1])

#resultfn = None
#resultfn = server_results      # Process directly (using asyncore.loop thread)
resultfn = store_results        # Store for processing later



credentials = {
    'password':         'changeme',
    'interface':        'localhost',
    'port':             mincemeat.DEFAULT_PORT,

    'datasource':       None,   # Causes TaskManager to stay idle
    'mapfn':            mapfn,
    'collectfn':        collectfn,
    'reducefn':         reducefn,
    'finishfn':         finishfn,
    'resultfn':         resultfn,
}
    
def logchange( who, previous ):
    current = who.state()
    if current != previous:
        logging.info("%s was %s; now %s" % ( who.name(), previous, current ))
    return current
    
# 
# Deriving from mincemeat.Mincemeat_daemon
# 
#     Pass a 'timeout=#.#' to the constructor, and override the
# timeout method; timeout wil be invoked by the daemon's service
# thread at the specified interval.  To send commands; use
# send_command, NOT send_command_backchannel as required for external
# thread initiated commands!
# 

class Cli(mincemeat.Client_daemon):

    def timeout(self, done=False):
        """
        The Client_daemon's process (asyncore.loop) thread is invoking
        us; use send_command; send_command_backchannel not necessary.

        We just want to ensure our server is alive, and the
        communications channel is open.  If we don't get a response
        within a certain period, there will be... trouble.
        """
        if not done:
            self.mincemeat.ping( allowed=30.0)

class Client_Repl(mincemeat.Client):
    """
    A mincemeat.Client that knows how to process the "transactiondone"
    response, containing the results of a previous "transaction"
    command, sent by the REPL vai the Client, to the Server.
    """
    def __init__(self, *args, **kwargs):
        mincemeat.Client.__init__(self, *args, **kwargs)
        self.pingbeg = time.time()
        self.pongtim = None             # time when pong received
        self.pongtxn = None             # it's transaction

    def ping(self, payload=None, now=None, allowed=None):
        """
        Send a ping command with a tell-tale time as the transaction
        ID.  Allow a certain amount of delay before killing our
        connection to the (crippled) server.

        This method must be invoked (at least) often enough to ensure
        that, under normal circumstances, the inter-ping latency, plus
        any normal Server response delay, plus the round-trip trip
        time does not exceed the allowed time.
        """

        # Check the last pong received.  Defaults to whenever we
        # began.  If we exceed allowed, treat as a protocol failure
        # (same as EOF).  Otherwise, send another ping.
        now = time.time()
        if self.pongtxn is not None:
            delay, delaymsg = self.ping_delay(txn=self.pongtxn,
                                              now=self.pongtim)
            since, sincemsg = self.ping_delay(txn=self.pongtxn,
                                              now=now)
        else:
            delay = 0.0
            delaymsg = "?"
            since = now - self.pingbeg
            sincemsg = "%.3f s (no replies received)" % since

        if since > allowed:
            logging.warning("%s ping latency %s, last %s; Exceeds %.3fs allowed; disconnecting!" % (
                self.name(), delaymsg, sincemsg, allowed))
            self.handle_close()
        else:
            logging.info("%s ping latency %s, last %s < %s" % (
                self.name(), delaymsg, sincemsg, allowed))
            mincemeat.Client.ping(self, now=now)

    def pong(self, command, data, txn):
        """
        Override the default pong, to remember the transaction numbers
        of the ping responses we receive, and the time we received them.
        """
        self.pongtim = time.time()
        self.pongtxn = txn
        mincemeat.Client.pong(self, command, data, txn)

    def unrecognized_command(self, command, data=None, txn=None):
        """
        Store the results; let the main thread detect and print new results.
        """
        if command == "transactiondone":
            store_results( data )
            return True

        return False

class Svr(mincemeat.Server_daemon):

    def timeout(self, done=False):
        """
        The Server_daemon's process (asyncore.loop) thread is invoking
        us; it also runs all the Server's ServerChannel's; hence, we
        use send_command, send_command_backchannel not necessary.
        """
        pass
        logging.info("%s Svr timeout %s" % (
                self.name(), done and "done" or ""))

        self.mincemeat.taskmanager.channel_log(None, "")
        for chan in self.mincemeat.taskmanager.channels.keys():
            self.mincemeat.taskmanager.channel_log(chan, "")

class Server_Repl(mincemeat.Server):
    """
    A mincemeat.Server that knows how to process the "transaction"
    command issued by a Client, coming in via our ServerChannel to
    that client.

    We'll enqueue deque of tasks, where we'll remember the issuing
    ServerChannel, and the (command, data, txn) tuple.  Later, after
    the transaction is complete, we'll send back a "transactiondone".
    
    """

    def unrecognized_command(self, command, data=None, txn=None, chan=None):
        """
        Schedule a future Map/Reduce transaction.  The Client's 'txn'
        will be used when sending back the results in the
        corresponding "transactiondone" response.  

        The local Map/Reduce transaction performed by our TaskManager
        will generate its own 'txn', independent of this one created
        by the Client.
        """
        logging.info("%s received command via %s from peer %s: %s %s" % (
                self.name(), chan.name(), str(chan.addr), 
                '/'.join( [command] + ( txn and [txn] or [])), repr.repr(data)))

        if command == "transaction":
            path = os.path.join( "../Gutenberg SF CD/Gutenberg SF",
                                 str(data))
            logging.info("%s Map/Reduce Transaction: %s" % (
                    self.name, path))
            self.datasource = file_contents( path )

            #self.datasource = transactions.append( (command, data, txn, chan) )
            return True

        return False

    def resultfn(self, results, txn):
        """
        Trap the result, and send them back.
        """
        logging.info("%s computed results: %s" (repr.repr( results )))



def REPL( cli ):
    """
    Await input from client.  Since Windows (unbelievably) cannot
    await async I/O on both a socket and the console, we must use a
    separate thread...  A blank line will terminate input.
    """
    command = 0
    print "Which files?  eg. '*.txt<enter>'"
    while cli.is_alive():
        print "GLOB> ",
        try:    inp = sys.stdin.readline().rstrip()
        except: inp = None
        if inp:
            cli.mincemeat.send_command_backchannel( "transaction", inp,
                                                    txn = str( command ))
        else:
            break
        command += 1

        while cli.is_alive() and not global_results:
            print ".",
            time.sleep( 1 )
        if global_results:
            server_results( global_results.popleft() )
    

def main():
    cli = None
    clista = "(none)"
    svr = None
    svrsta = "(none)"
    rpt = None
    try:
        # If we fail to start a Client, try firing up a Server while
        # continuing trying to fire up the Client.  Since we don't
        # know how long this might take (including authentication), we
        # may need to attempt creating a Client or a Server several
        # times.
        begun = time.clock()
        limit = 5.0                     # Wait for this time, total
        cycle = 0.1                     # Wait about this long per cycle
        while time.clock() < begun + limit:
            if not cli:
                # No client yet? Create one.  May sometimes throw
                # immediately, if non-blocking connect is unusually
                # fast...
                try:
                    cli = Cli(credentials, cls=Client_Repl, timeout=5.0)
                    clista = logchange( cli, clista )
                    cli.start()
                    clista = logchange( cli, clista )
                except Exception, e:
                    logging.warning("Client thread failed: %s\n%s" % (
                            e, traceback.format_exc()))

            time.sleep(cycle)

            if cli:
                # Client exists.  Keep checking state; must reach at
                # least "authenticated"; may (if Server is quick),
                # actually reach "success"!
                clista = logchange( cli, clista )
                if cli.state() in ( "authenticated", "success" ):
                    # Success!  Up and running.
                    break
                if clista.startswith("fail"):
                    logging.warning("Client failed; trying again...")
                    cli.stop(cycle)
                    cli = None

            if not svr:
                # Client didn't come up and/or didn't immediately
                # authenticate.  Create a Server_Repl.
                try:
                    svr = Svr(credentials, cls=Server_Repl, timeout=5.0)
                    svrsta = logchange( svr, svrsta )
                    svr.start()
                    svrsta = logchange( svr, svrsta )
                except Exception, e:
                    # The bind probably failed; Server couldn't
                    # bind,...  Perhaps someone else beat us to it!
                    logging.warning("Server thread failed: %s\n%s" % (
                            e, traceback.format_exc()))

            if svr:
                svrsta = logchange( svr, svrsta )
                if svrsta.startswith("fail"):
                    logging.warning("Server failed; trying again...")
                    svr.stop(cycle)
                    svr = None



        # We've given the the 'ol college try, to start a Client;
        # ensure we have had a Client!  If so, wait 'til it is done.
        # If we (also) ran a Server_thread, it'll use the 'resultfn'
        # callback to deliver the results.

        if not cli or cli.state() != "authenticated":
            raise Exception("Client couldn't authenticate with Server")

        # Got an authenticated Client (and either a Server existed, or
        # we spawned one.  Start the REPL, and process 'til the client
        # thread is done.  Send pings (with no timeout on
        # transmission) every second.  This means that, upon failure
        # to transmit

        rpt = threading.Thread( target = REPL, args=(cli,) )
        rpt.start()

        while cli.is_alive() and rpt.is_alive():
            clista = logchange( cli, clista )
            if svr:
                svrsta = logchange( svr, svrsta )
            time.sleep( 1 )

    except KeyboardInterrupt:
        logging.info("Manual shutdown requested")
    except Exception, e:
        logging.error("Exception encountered: %s\n%s" % (
                e, traceback.format_exc()))
    finally:
        # Exception, Manual shutdown (eg. KeyboardInterrupt), or
        # normal exit; stop any Client and/or Server.  This will cause
        # their asyncore.loop to cease.  Use the default timeouts.
        if svr:
            svr.stop()
        if cli:
            cli.stop()
        if rpt:
            sys.stdin.close()
            rpt.join()

    # Ensure that everything exited cleanly.  The Server should be
    # done, and should have finished() and produced results().
    code = 0
    if cli and cli.state() != "success":
        logging.error("Client thread didn't exit cleanly: %s" % cli.state())
        code = 1
    if svr and svr.state() != "success":
        logging.error("Server thread didn't exit cleanly: %s" % svr.state())
        code = 1
    return code

if __name__ == '__main__':
    logging.basicConfig( level=logging.INFO )
    sys.exit(main())

