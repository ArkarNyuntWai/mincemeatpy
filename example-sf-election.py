#!/usr/bin/env python
import mincemeat
import glob
import logging
import repr
import socket
import asyncore

"""
example-sf-election     -- elects (self) as server, or become a client

    To run this test, simply start multiple instances of this script:

        python example-sf-election.py

The first will become the server; the remainder will become clients.
This is the initial phase of development for systems the dynamically
must decide if there is a server available, and spawn one if a server
is not found.  
    



    Here is the original simple dictionary datasource:

        data = ["Humpty Dumpty sat on a wall",
                "Humpty Dumpty had a great fall",
                "All the King's horses and all the King's men",
                "Couldn't put Humpty together again",
                ]
        # The data source can be any dictionary-like object
        datasource = dict(enumerate(data))

Alternatively, here is a iterable that returns the contents of a set of
files designated by name (or a "glob" pattern match):
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
datasource = file_contents( '../Gutenberg SF CD/Gutenberg SF/*moon*.txt' )

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

collectfn = None
#collectfn = sum_values
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
#reducefn = None
reducefn = sum_values
#reducefn = sum_values_generator

finishfn = None
#finishfn = sum_values
#finishfn = sum_values_generator


# Specify an externally visible server network interface name instead
# of "localhost", if you wish to try this example accross multiple
# hosts.  Note that the empty string '' implies INADDR_ANY for bind, and
# 
addr_info = {
    'password':         'changeme',
    'interface':        'localhost',
    'port':             mincemeat.DEFAULT_PORT
}
    

def server( credentials ):
    """
    Run a Map-Reduce Server, and process a single Map-Reduce 
    """
    s = mincemeat.Server()
    s.datasource = datasource
    s.mapfn = mapfn
    s.collectfn = collectfn
    s.reducefn = reducefn
    s.finishfn = finishfn
    
    results = s.run_server( **credentials )
    
    # Map-Reduce over 'datasource' complete.  Enumerate results,
    # ordered both lexicographically and by count
    bycount = {}
    for k,v in results.items():
        if v in bycount:
            bycount[v].append( k )
        else:
            bycount[v] = [k]
    
    bycountlist = []
    for k,l in sorted(bycount.items()):
        for w in sorted( l ):
            bycountlist.append( (k, w) )
    
    for k, lt in zip( sorted( results.keys() ), bycountlist ):
        print "%8d %-40.40s %8d %s" % ( results[k], k, lt[0], lt[1] )

def client( credentials ):
    logging.debug( "  socket_map at client startup: %s" % (
            repr.repr( asyncore.socket_map )))
    c = mincemeat.Client()
    c.conn( **credentials )
    # Client communications with Server done; either server completed
    # success, or exited without completing our authentication.
    if c.auth != "Done":
        raise Exception( "No server authenticated!" )
    
if __name__ == '__main__':
    logging.basicConfig( level=logging.INFO )
    
    try:
        logging.info( "Trying as client..." )
        client( addr_info )
        # If we get here, we succeeded in connecting and authenticating...
        logging.info( "Client terminating normally" )
    except:
        logging.info( "Client connection failed; Trying as server..." )
        server( addr_info )
