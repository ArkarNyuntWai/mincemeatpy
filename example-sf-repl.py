#!/usr/bin/env python

from __future__ import with_statement

import collections
import glob
import itertools
import logging
import os
import repr
import socket
import errno
import asyncore
import threading
import time
import timeit
import traceback
import select
import sys

import mincemeat

timer = timeit.default_timer # Platform-specific high-precision wall-clock timer

"""
example-sf-repl         -- Client reads/schedules requests; Spawns Server if needed

    To run this test, simply start an instances of this script:

        python example-sf-repl.py

Each client may schedule Map-Reduce transactions to the Server, and
receive the results of those transactions sometime in the future.  Of
course, each Client also receives map and reduce tasks from the
Server.

    The default location of the files referenced for pattern matching is:
in:
        ../Gutenberg SF CD/Gutenberg SF/

Obtain CD ISO from:

        http://www.gutenberg.org/cdproject/pgsfcd-032007.zip.torrent

unzip it, create the following directory, and copy the CD's "Gutenberg
SF" directory into it:

         ../Gutenberg SF CD/

    Or, point 'textbase' (below) to some other directory where you
have some handy ASCII text data files:
"""

textbase = ["..", "Gutenberg SF CD", "Gutenberg SF"]

# 
# Data Source.
# 
#     Provide key-index data for the Map/Reduce.  Typically a dict,
# but any object satisfying the following requirements may be used.
# Mincemeat requires unique keys (it uses them in some internal
# record-keeping), but has no requirements on keys beyond that.  Each
# (key, data) is passed to a Client as a Map task.
# 
#     Requires a dict-like object, where each key must uniquely index
# a datum.  We require that the datasource satisfy the iterator
# protocol (provide __iter__(), which returns an iterable object
# having a next() returning unique keys), and at least the
# __getitem__() method of the sequence protocol (len() is never used
# on a datasource, so __len__() is optional)
# 
class file_contents(object):
    """
    For each filename matching the given pattern, iter(self) returns a
    matching filename.  When indexed with a valid filename, returns
    the file contents.
    """
    def __init__(self, pattern):
        self.text_files = glob.glob( pattern )
    def __len__(self):
        return len(self.text_files)
    def __iter__(self):
        return iter(self.text_files)
    def __getitem__(self, key):
        if key not in self.text_files:
            raise IndexError
        with open(key) as f:
            return f.read()

class file_meta(object):
    """
    For each filename matching the given pattern, iter(self)
    repeatedly returns the command you specified, prefixed with an
    index number "#@".  When indexed with (any key), returns the file
    name.  We use this to produce:

        ("#@command", "some/file/path")

    for the Map phase, when using our augmented get_lower_split:
    """
    def __init__(self, pattern, command):
        self.text_files = glob.glob( pattern )
        self.command = command
    def __len__(self):
        return len(self.text_files)
    def __iter__(self):
        """
        Return a generator producing "##@command", sufficient to index
        each self.text_file.
        """
        return ("%d@%s" % (i, self.command) for i in xrange(len(self)))
    def __getitem__(self, key):
        """
        Index by # portion of "#@..." key.
        """
        off = key.find('@')
        if off < 1:
            raise IndexError
        return self.text_files[int(key[0:off])]

class repeat_command(object):
    """
    Simply repeat the given command as the key; data is always empty.
    We don't implement the full sequence protocol (we don't know our
    length, so provide no __len__()), but that's OK; len() is never
    used.
    
    If the default times=None is used, then this object should be used
    as a datasource only if TaskManager.ONESHOT is specified (send one
    Map task to each Client).
    """
    def __init__(self, command, data="", times=None):
        self.command = command
        self.data = data
        self.times = times
    def __iter__(self):
        """
        Returns a generator expression yielding #@command strings,
        optionally for the specified number of times.
        """
        # Contrary to "equivalent code" in documentation, providing
        # None for times doesn't produce infinite itertools.repeat...
        if self.times is None:
            args = (self.command,)
        else:
            args = (self.command, self.times)
        return ("%d@%s" % (idx, cmd) for idx, cmd in
                enumerate(itertools.repeat(*args)))
    def __getitem__(self, key):
        return self.data

# 
# Map Functions.
# 
#     Take a name and corpus of data, and map it onto an iterable of
# (key,value) pairs.
# 
def get_lower_split( name, corpus ):
    """
    Accepts the name, corpus pair and runs a word count over it. 

    Alternatively, accept a name of the form "##@command", paired with
    some data, and evals the command portion.  Executes the result as
    a function, on the provided data.  If the result is an iterable
    producing (key, value) pairs, yields the results directly.
    Otherwise, processes the results as the word-count corpus.


    """
    import re
    import string

    m = re.match(r"^(\d*)@(.*)$", name)
    if m:
        # We found an '#@...' (possibly with no #); Allow arbitrary
        # anonymous functions to be evaluated; It's OK; Server is
        # authenticated!
        #
        # We expect the code to evaluate to a function taking the
        # corpus, and returning either a new corpus, or an iterable
        # yielding (key, value).  For example, to open and read the
        # file locally (instead of reading it at the Server, and
        # transmitting it:
        # 
        #     123@lambda corpus: open(corpus).read()
        # 
        # To perform a (simpler) word count directly on the named
        # corpus file:
        # 
        #     123@lambda corpus: ((word.lower(), 1) for word in open(corpus).read().split())
        # 
        # Or, to do something else entirely (assuming the code
        # evaluated uses names that can be resolved in the context of
        # the Client). For example, make up keys using enumerate (all
        # Clients must agree on the keys!), and return arbitrary lists
        # of data:
        # 
        #     @lambda corpus: enumerate( [ (socket.getfqdn(), os.name) ] )
        # 
        logging.info( "Evaluating: %-40s: %8d bytes: %s" %(
                name, len( corpus ), repr.repr( corpus )))
        output = eval( m.group(2) )( corpus )
        try:
            for k, v in output:
                yield k, v
            raise StopIteration
        except ValueError:
            # The result was NOT a (key, value) iterator!  Continue to
            # process it as the text corpus.
            corpus = output
            pass

    logging.info( "Processing: %-40s: %8d bytes: %s" %(
                name, len( corpus ), repr.repr( corpus )))
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
    raise StopIteration

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
#   --> {map_key1: [value, ...] ), map_key2: [value, ...], ...}
# collectfn( map_key1, [value, value] )
#   --> data_key1: [value]
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

def sum_values_long( kvi ):
    """
    This collectfn just sums long lists, leaving shorter ones alone.
    It is usable only as a collectfn, but not as a reducefn/finishfn.

    The collectfn may also allows returning lists (unlike
    reducfn/finishfn).  This is because the Map[/Collect] phase always
    takes (arbitrary) name,data as input and results in
    {key:[value,...],...}.  The Reduce[/Finish] phase takes
    key,[value,...] as input and results in {key:value, ...}; no lists
    are allowed in Reduce results.  
    """
    for k, vs in kvi:
        yield k, vs if len(vs) < 10 else [sum(vs)]

#collectfn = None
#collectfn = sum_values
#collectfn = sum_values_generator
collectfn = sum_values_long

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
#     Invoked with the Map/Reduce Transaction ID and its results.
# 
#     Instead of monitoring the Server for completion and manually
# obtaining results via the Server.results() function, an optional
# resultfn callback may be provided, which is invoked by the Server
# immediately with the final results upon completion.
# 
global_results = collections.deque()

def store_results(txn, results):
    global_results.append((txn, results))

def server_results(txn, results, top=None):
    # Map-Reduce over 'datasource' complete.  Enumerate results,
    # ordered both lexicographically and by count
    print "Transaction %s; Word Count; %s%d results:" % (
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

def stats_results(txn, results):
    # Collection of Stats from all Clients complete.
    print "Transaction %s; Stats; %d results:" % (
        txn, len(results))
    for k, vs in results.iteritems():
        for v in vs:
            print "%-16s: %s" % (k, v)
    sys.stdout.flush()

# We'll provide a resultfn in our Server_Repl class...
resultfn = None                 # None retains default behaviour
#resultfn = server_results       # Process directly (using asyncore.loop thread)
#resultfn = store_results        # Store for processing later by another thread

credentials = {
    'password':         'changeme',
    'interface':        'localhost',
    'port':             mincemeat.DEFAULT_PORT,

    'datasource':       None,   # Causes TaskManager to stay idle
    'mapfn':            mapfn,
    'collectfn':        collectfn,
    'reducefn':         reducefn,
    'finishfn':         finishfn,
    'resultfn':         resultfn
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
# thread at the specified interval.  To send commands within timeout; use
# send_command, NOT send_command_backchannel as required for external
# thread initiated commands!
# 

class Client_HB_Repl(mincemeat.Client_HB):
    """
    A mincemeat.Client_HB that knows how to process the
    "transactiondone" response, containing the results of a previous
    "transaction" command, sent by the REPL vai the Client, to the
    Server.
    """
    def unrecognized_command(self, command, data=None, txn=None):
        """
        Store the results; let the main REPL thread detect and print them.
        """
        logging.info("%s received command: %s %s" % (
                self.name(), 
                '/'.join( [command] + ( txn and [txn] or [])),
                repr.repr(data)))

        if command in (
            "transactiondone", 
            "statsdone",
            ):
            store_results(txn, data)
            return True

        return False

class Cli(mincemeat.Client_HB_daemon):
    """
    A Client w/HeartBeats daemon that knows about our REPL commands.
    """
    def __init__(self, credentials, cls=Client_HB_Repl,
                 **kwargs):
        mincemeat.Client_HB_daemon.__init__(self, credentials, cls=cls,
                                            **kwargs)


class Server_HB_Repl(mincemeat.Server_HB):
    """
    A mincemeat.Server_HB that also knows how to process the
    "transaction" and "stats" commands issued by our Client_REPL,
    coming in via our ServerChannel to that client.

    For each incoming command from our Client, we'll allocate a new,
    unique Server Transaction ID, and enqueue a new datasource
    of tasks, where we'll remember the issuing ServerChannel, and the
    (command, data, txn) tuple.  Later, after the transaction is
    complete, we'll send back a "transactiondone" via the same channel
    to the Client.
    """
    def __init__(self, *args, **kwargs):
        mincemeat.Server_HB.__init__(self, *args, **kwargs)
        self.loctxn = 1000
        self.inflight = {}

    def unrecognized_command(self, command, data=None, txn=None, chan=None):
        """
        Schedule a future Map/Reduce transaction.  The Client's 'txn'
        will be used when sending back the results in the
        corresponding "transactiondone" response.  

        The local Map/Reduce transaction performed by our TaskManager
        will generate its own 'txn', independent of this one created
        by the Client.  We'll map our txn back to the client's when
        returning the results.
        """
        # We MUST put in place the "in-flight" mapping of this local
        # txn to the correspoding Client txn and channel, BEFORE we
        # set the data source; if the datasource is empty, processing
        # will be immediate (as set_datasource will jump-start the
        # TaskManager, producing a call to resultfn directly),
        # accessing self.inflight[loctxn])!
        loctxn = str(self.loctxn)
        self.loctxn += 1
        self.inflight[loctxn] = {
            'txn':          txn,
            'chan':         chan,
            'command':      command,
            }

        logging.info("%s txn %s (via %s from peer %s): %s %s" % (
                self.name(), loctxn, chan.name(), str(chan.addr), 
                '/'.join( [command] + ( txn and [txn] or [])),
                repr.repr(data)))

        if command == "transaction":
            glob                = data
            meta                = False
            if glob.startswith('@'):
                glob            = glob[1:]
                meta            = True
            path = os.path.join(*(textbase + [glob]))
            if meta:
                data            = file_meta( path,
                    "lambda corpus: open(corpus).read()" )
            else:
                data            = file_contents( path )

            logging.warning("%s Map/Reduce %s ==> %d files" % (
                self.name(), path, len(data)))
            self.set_datasource(
                datasource      = data,
                txn             = loctxn,
                allocation      = mincemeat.TaskManager.CONTINUOUS,
                cycle           = mincemeat.TaskManager.PERMANENT )
            return True

        if command == "stats":
            logging.warning("%s Stats" % (
                self.name()))
            # This datasource is an unlimited iterator; only usable in ONESHOT
            self.set_datasource(
                datasource      = repeat_command(
                    "lambda corpus: enumerate([(socket.getfqdn(), os.name)])"),
                txn             = loctxn,
                allocation      = mincemeat.TaskManager.ONESHOT,
                cycle           = mincemeat.TaskManager.PERMANENT )
            return True

        # Command not recognized!  Kill inflight entry
        logging.info("%s tnx %s: command %s unrecognized" % (
                self.name(), loctxn,
                '/'.join( [command] + ( txn and [txn] or []))))
        del self.inflight[loctxn]
        return False

    def resultfn(self, txn, results):
        """
        Retrieve the channel and the Client's txn information using
        our Map/Reduce Transaction's txn, and send the results back to
        the Client.
        """
        client = self.inflight.pop(txn, None)
        if client is not None:
            # Got results (they may be empty, but this is a valid result)
            logging.warning("%s Map/Reduce txn %s: results via %s: %s" % (
                    self.name(), txn, client['chan'].name(),
                    repr.repr(results)))
            client['chan'].send_command( client['command'] + "done", data=results,
                                         txn=client['txn'] )
        else:
            logging.error("%s unable to identify txn %s results in %s: %s" % (
                    self.name(), txn, repr.repr(self.inflight),
                    repr.repr(results)))


class Svr(mincemeat.Server_HB_daemon):
    """
    A Server w/HeartBeats daemon that knows about our Client REPL commands.

    Specify our custom mincemeat.Server* class; everything else passes
    through unscathed; we use the default hearbeat timing parameters.
    """
    def __init__(self, credentials, cls=Server_HB_Repl,
                 **kwargs):
        mincemeat.Server_HB_daemon.__init__(self, credentials, cls=cls,
                                            **kwargs)


def REPL( cli ):
    """
    Await input from console, and send it to the Server, via the
    Client.

    Since Windows (unbelievably) cannot await async I/O on both a
    socket and the console, we must use a separate thread...  EOF (^D
    on Posix systems) will terminate input.
    """
    txn = 0
    print "Which files?  eg. '*.txt<enter>'"
    while cli.is_alive():
        sys.stdout.write( "GLOB> " )
        sys.stdout.flush()
        try:
            inp = sys.stdin.readline()
        except:
            inp = None

        if inp:
            inp = inp.rstrip()
            if inp:
                # A filename pattern?  Count some words.
                cmd = "transaction"
            else:
                # A Blank line.  Get some stats.
                cmd = "stats"
            cli.mincemeat.send_command_backchannel(
                cmd, inp, txn=str(txn))
        else:
            # EOF
            break

        # Wait for results to appear, or client to die.
        while cli.is_alive() and not global_results:
            time.sleep(.1)

        if global_results:
            args = global_results.popleft()
            if cmd == "transaction":
                server_results(top=100, *args)
            elif cmd == "stats":
                stats_results(*args)

        txn += 1
    

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
        begun = timer()
        limit = 5.0                     # Wait for this time, total
        cycle = 0.5                     # Wait about this long per cycle
        while timer() < begun + limit:
            if not cli:
                # No client yet? Create one.  May sometimes throw
                # immediately, if non-blocking connect is unusually
                # fast...
                try:
                    cli = Cli(credentials)
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
                    svr = Svr(credentials)
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
        logging.info("%s Manual shutdown requested" % (
                svr and svr.name() or "" ))
    except Exception, e:
        logging.error("%s Exception encountered: %s\n%s" % (
                svr and svr.name() or "",
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
    logging.basicConfig( level=logging.WARNING )
    sys.exit(main())
