#!/usr/bin/env python
import mincemeat
import glob
import logging
import repr

'''
example-sf-maponly	-- skips reduce phase (run finish func. in server)

    To run this test, first start the example Map-Reduce server; this
will bind to the default port, on all interface:
    
        python example-sf-maponly.py

Then, in another window, run one (or more) clients (defaults to
interface "localhost"):

        python mincemeat.py -p changeme



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
'''

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

def get_lower_split( k, v ):
    import string
    logging.debug( "Corpus: %-40s: %d bytes" %( k, len( v )))
    for line in v.split("\n"):
        for word in line.replace('--',' ').split():
            word = word.lower().strip(string.punctuation+
                                      string.whitespace+
                                      string.digits)
            if word:
                yield word, 1

def get_lower_simple( k, v ):
    for w in v.split():
        yield w.lower(), 1

# Alternatives for specifying a collect, reduce or finish function

def sum_values( k, vs ):
    return sum( vs )

def sum_values_generator( kvi ):
    for k, vs in kvi:
        yield k, sum( vs )

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

# Skip the Reduce phase; use the Reduce function as Server.finishfn
reducefn = None
#finishfn = sum_values_generator
finishfn = sum_values

s = mincemeat.Server()
s.datasource = datasource
s.mapfn = mapfn
s.collectfn = collectfn
s.reducefn = reducefn
s.finishfn = finishfn

logging.basicConfig( level=logging.DEBUG )

results = s.run_server(password="changeme")

bycount = {}
for k,v in results.items():
    #if type( v ) is list:
    #    results[k] = v = sum( v )
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


