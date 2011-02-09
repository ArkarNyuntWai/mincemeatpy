#!/usr/bin/env python
import mincemeat
import glob
import logging
import repr
import socket
import errno
import asyncore
import threading
import time
import traceback
import sys

"""
example-sf-daemon       -- elect a server, become a client; uses daemons

    To run this test, simply start an instances of this script:

        python example-sf-daemon.py

Much like example-sf-masterless.py, but uses the
mincemeat.Server_daemon and Client_daemon to implement threaded async
I/O.  Also demonstrates advanced usage of the default Server.output
deque, for asynchronously accessing Map/Reduce results.
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
datasource = file_contents( '../Gutenberg SF CD/Gutenberg SF/*18*.txt' )

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
#     Each Map client runs a full pass of mapfn over the incoming data, followed
# (optionally) by a pass of collectfn over all values for each Map data_key:
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
def server_results(txn, results, top=None):
    # Map-Reduce over 'datasource' complete.  Enumerate results,
    # ordered both lexicographically and by count
    print "Transaction %s; %s%d results:" % (
        txn, ( top and "top %d of " % top or ""), len(results))
    # Collect lists of all words with each unique count
    bycount = {}
    for wrd,cnt in results.items():
        if cnt in bycount:
            bycount[cnt].append(wrd)
        else:
            bycount[cnt] = [wrd]

    # Create linear list of words sorted by count (limit to top #)
    bycountlist = []
    for cnt in sorted(bycount.keys(), reverse=True):
        for wrd in sorted(bycount[cnt]):
            bycountlist.append((cnt, wrd))
        if top and len(bycountlist) >= top:
            break

    # Print two columns; one sorted lexicographically, one by count
    for wrd, cnt_wrd in zip(sorted([wrd for __,wrd in bycountlist],
                                   reverse=True),
                            reversed(bycountlist)):
        print "%8d %-40.40s %8d %s" % (results[wrd], wrd, cnt_wrd[0], cnt_wrd[1])

resultfn = None                 # None retains default behaviour
#resultfn = server_results       # Process directly (using asyncore.loop thread)

credentials = {
    'password':         'changeme',
    'interface':        'localhost',
    'port':             mincemeat.DEFAULT_PORT,

    'datasource':       datasource,
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
    

# Use the main service thread timeout facility to send commands; NOT
# send_back_channel, as required for externally-initiated commands!

class Cli(mincemeat.Client_daemon):

    def timeout(self, done=False):
        """
        The Client_thread's process (asyncore.loop) thread is invoking
        us; use send_command, NOT send_command_backchannel.
        """
        logging.info("%s timeout %s" % (
                self.name(), done and "done" or ""))
        self.endpoint.send_command("ping", ( "Client Timeout from %s" % (
                                             socket.getfqdn()), 
                                        "%s -- Client loop" % ( 
                                             threading.current_thread().name )))

class Svr(mincemeat.Server_daemon):

    def timeout(self, done=False):
        """
        The Server_thread's process (asyncore.loop) thread is invoking
        us; it also runs all the Server's ServerChannel's; hence, we
        use send_command, NOT send_command_backchannel.
        """
        logging.info("%s timeout %s" % (
                self.name(), done and "done" or ""))
        for chan in self.endpoint.taskmanager.channels.keys():
            chan.send_command("ping", ( "Server Timeout from %s" % (
                                             socket.getfqdn()), 
                                        "%s -- Server loop" % ( 
                                             threading.current_thread().name )))

def main():
    cli = None
    clista = "(none)"
    svr = None
    svrsta = "(none)"
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
                    cli = Cli(credentials, timeout=5.0)
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
                # authenticate.  Create a Server.
                try:
                    svr = Svr(credentials, timeout=5.0)
                    svrsta = logchange( svr, svrsta )
                    svr.start()
                    svrsta = logchange( svr, svrsta )
                except Exception, e:
                    # The bind probably failed; Server couldn't
                    # bind,...  Perhaps someone else beat us to it!
                    logging.warning("Server thread failed: %s" % e)

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
            logging.error("Client couldn't authenticate with Server")
        else:
            # Got an authenticated Client (and either a Server
            # existed, or we spawned one.  Process 'til the client
            # thread is done.  Send pings (with no timeout on
            # transmission) every second.  This means that, upon failure to 
            # transmit
            while cli.is_alive():
                clista = logchange( cli, clista )
                if svr:
                    svrsta = logchange( svr, svrsta )
                begun = time.clock()
                cli.endpoint.send_command_backchannel(
                    "ping", ( "Request from %s" % ( socket.getfqdn()),
                               #'x' * 1 * 1000 * 1000 ))        # a big blob...
                              "%s -- main thread" % (           # a thread name
                                  threading.current_thread().name )))
                time.sleep( 1 )

            # Done! Client has exited.  Log any results available, if
            # we were running Server.  We'll access the
            # mincemeat.Server.output deque directly, and pass the
            # (txn,results) tuple as position args.
            while svr.endpoint.output:
                args = svr.endpoint.output.popleft()
                server_results(top=100, *args)


    except KeyboardInterrupt:
        logging.info("Manual shutdown requested")

    finally:
        # Exception, Manual shutdown (eg. KeyboardInterrupt), or
        # normal exit; stop any Client and/or Server.  This will cause
        # their asyncore.loop to cease.  Use the default timeouts.
        if svr:
            svr.stop()
        if cli:
            cli.stop()

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
