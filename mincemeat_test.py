
import asyncore
import collections
import logging
import Queue
import random
import threading
import time

import mincemeat

# We don't want port interference between tests, due to
# kernel-specific i'face:port timeouts; 

testcount = 0
def unique_port(port):
    return port + 99 + testcount

logging.basicConfig(level=logging.ERROR)


data = ["Humpty Dumpty sat on a wall",
        "Humpty Dumpty had a great fall",
        "All the King's horses and all the King's men",
        "Couldn't put Humpty together again",
        ]

def mapfn(k, v):
    for w in v.split():
        yield w, 1

def reducefn(k, vs):
    result = sum(vs)
    return result

credentials = {
    'password':         'changeme',
    'interface':        'localhost',
    'port':             None,   # Invalid; must be updated before use

    'datasource':       None,   # Causes TaskManager to stay idle
    'mapfn':            mapfn,
    'collectfn':        None,
    'reducefn':         reducefn,
    'finishfn':         None,
}

def slow(fun, amt):
    def wrapper(*args, **kwargs):
        import time
        time.sleep(amt)
        fun(*args, **kwargs)
        time.sleep(amt)
    return wrapper


def near( a, b, significance = 1.0e-4 ):
    """
    Returns True iff the difference between the values is within the factor
    'significance' of one of the original values.  Default is to within 4
    decimal places.
    """
    return abs( a - b ) <= significance * max( abs( a ), abs( b ))


counter = 0
def count():
    global counter
    counter += 1


def test_basic():
    """
    Tests basic time and scheduling core functionality.
    """
    global testcount
    testcount += 1

    sch = collections.deque()

    # An arbitrarily small repeat interval will result in one triggering
    # per cycle, and a sensible future next expiry
    now = mincemeat.timer()

    exp, mis, pas = mincemeat.next_future_time(
        expiry=now - .101, repeat=.01, now=now)
    assert near(mis, 10.0)
    assert near(pas, .001)

    sch.append((now, count, 0.001))
    exp = mincemeat.trigger(sch, now=now)
    assert counter == 1
    assert exp > now


def test_bind():
    """
    Tests that socket binding exclusion works.
    """
    global testcount
    testcount += 1

    port = unique_port( mincemeat.DEFAULT_PORT )
    cred = credentials.copy()
    cred.update({"port": port})
    
    s1 = mincemeat.Server_daemon(credentials=cred, timeout=5.)
    state = s1.state()
    assert state == "idle"
    s1.start()
    time.sleep(1)
    state = s1.state()
    assert state == "authenticated"

    try:
        s2 = mincemeat.Server()
        s2.conn(**cred)
        assert False == "Should have thrown Exception in bind()!"
    except Exception, e:
        assert "Only one usage of each socket address" in str(e) \
            or "Address already in use" in str(e)

    # Server wasn't provided with a datasource; it will remain idle, and report
    # failed: incomplete.
    s1.stop()
    state = s1.state()
    assert state == "failed: incomplete"


def test_schedule():
    """
    Tests scheduled events.
    """
    global testcount
    testcount += 1

    # Configure the Server to go idle awaiting Transactions, so we can test
    # scheduling, and response to manually tearing down the Server.
    port = unique_port( mincemeat.DEFAULT_PORT )
    cred = credentials.copy()
    cred.update({
            "port":  port,
            "cycle": mincemeat.TaskManager.PERMANENT
            })
    
    s1 = mincemeat.Server_daemon(credentials=cred)
    s1.start()

    def put(thing, queue):
        logging.info("Putting %s on %s" % (thing, queue))
        queue.put(thing)

    # Wait 1 second, and put something on 'res' Queue
    res = Queue.Queue()
    sch = collections.deque()
    then = mincemeat.timer() + 1.
    sch.append((then, lambda: put(mincemeat.timer(), res), None))

    c1 = mincemeat.Client_daemon(credentials=cred, schedule=sch)
    beg = mincemeat.timer()
    c1.start()
    now = mincemeat.timer()

    try:
        slen = len(sch); assert slen == 1
    
        # Authentication should take a fraction of a second.  This may or may
        # not require the scheduling of a .condition_changed wake-up call,
        # depending on whether or not authentication is complete by this time!
        auth = c1.endpoint.authenticated(timeout=.25)
        logging.info("Took %.6fs to authenticate" % ( now - beg ))
        assert now - beg < .25
        state = c1.state()
        assert auth == True
        assert state == "authenticated"
    
        # OK, we should still have a scheduled .condition_changed wake-up
        # waiting to fire, and our lambda
        for s in sch:
            logging.info("event in %.6f s: %s" % (s[0] - mincemeat.timer(), s))
        slen = len(sch); assert slen in (1,2)
    
        # We should get the scheduled event ~1. second later; 
        try:
            r = res.get(timeout=.5)
        except Exception, e:
            assert type(e) == Queue.Empty
    
        # It should have timed out within a fraction of a second of the
        # intended time, and around 1. second since we started the Client.
        r = res.get(timeout=2)
        assert abs(r-then) < .1
        now = mincemeat.timer()
        assert .9 < now-beg < 1.1
    

        # Test tidy shutdown:
        #  client shutdown   -->
        #  svrchn close      -->
        #  client close
        # A tuple of bools compares like a binary number:
        #   (True, False) > (False, True)
        # 
    
        svrchn = s1.endpoint.taskmanager.channels.keys()[0]
        getstate = lambda number: (c1.endpoint.closed,
                                  svrchn.closed,
                                  c1.endpoint.shutdown, 
                                  number)
        cycle = 0
        last = getstate(cycle); cycle += 1
        assert last == (False, False, False, 0)
        start = now = mincemeat.timer()
    
        svrchn.handle_close = slow(svrchn.handle_close, .05)
    
        logging.info("%s: %s" % (time.ctime(), last))
        while ( now - start < 1. ):
            state = getstate(cycle); cycle += 1
            if state[:-1] != last[:-1]:
                logging.info("%s: %s" % (time.ctime(), state))
            assert state > last
            last = state
    
            if not c1.endpoint.shutdown:
                c1.endpoint.handle_close()
    
            now = mincemeat.timer()
        
        assert state[:-1] == (True, True, True)
    finally:
        c1.stop()
        s1.stop()


def test_example():
    """
    Tests a scaled-up version of example.py.
    
    Starts 1-5 Client threads, and scales up the text corpus a bit, proportional
    to the number of threads we choose.
    """
    global testcount
    testcount += 1

    port = unique_port( mincemeat.DEFAULT_PORT )

    clients = random.randint(1,5)
    scale = clients * 73

    # Since we are running multiple asyncore-based Clients and a
    # Server in separate threads, we need to specify map={} for the
    # Clients, so they all don't use the (default) global asyncore
    # socket map as the Server...
    logging.info("Starting %d clients...", clients)
    for _ in xrange(clients):
        c = mincemeat.Client(map={})
        t = threading.Timer(1.0, c.conn,
                        args=("", port),
                        kwargs={"password": "changeme"})
        t.daemon = True
        t.start()

    s = mincemeat.Server(map={})
    s.datasource = dict(enumerate(data * scale))
    s.mapfn = mapfn
    s.reducefn = reducefn
    
    now = mincemeat.timer()
    results = s.run_server(password="changeme", port=port)
    expected = dict((k, v*scale) for k,v in {
        'All': 1,
        "Couldn't": 1,
        'Dumpty': 2,
        'Humpty': 3,
        "King's": 2,
        'a': 2,
        'again': 1,
        'all': 1,
        'and': 1,
        'fall': 1,
        'great': 1,
        'had': 1,
        'horses': 1,
        'men': 1,
        'on': 1,
        'put': 1,
        'sat': 1,
        'the': 2,
        'together': 1,
        'wall': 1
        }.iteritems())
    assert results == expected


import itertools
class repeat_command(object):
    """
    Simply repeat the given command as the key; data is always the same ("" by
    default).  We don't implement the full sequence protocol (we don't know our
    length, so provide no __len__()), but that's OK; len() is never used.
    
    If the default times=None is used, then this object should be used as a
    datasource only if TaskManager.ONESHOT is specified (send one Map task to
    each Client).
    """
    def __init__(self, command, data="", times=None):
        self.command = command
        self.data = data
        self.times = times

    def __iter__(self):
        """
        Returns a generator expression yielding #@command strings, optionally
        for the specified number of times.
        """
        # Contrary to "equivalent code" in documentation, providing None for
        # times doesn't produce infinite itertools.repeat...
        if self.times is None:
            args = (self.command,)
        else:
            args = (self.command, self.times)
        return ("%d@%s" % (idx, cmd) for idx, cmd in
                enumerate(itertools.repeat(*args)))

    def __getitem__(self, key):
        return self.data

def map_identity( k, v ):
    yield k, 1

def test_oneshot():
    """
    Tests a ONESHOT Server with a pool of Clients.
    
    Starts 1-5 Client threads, and tries to schedule a number of distinct
    TaskManager.ONESHOT Map/Reduce Transactions.

    Transactions are scheduled in 3 ways;
    1) Before Server startup, named "initial-#"
    2) From within the Server's, called "scheduled-#"
    3) From the Client, called "backchannel-#"
    
    As soon as the deque of Transaction datasources drains, the Server quits.
    """
    global testcount
    testcount += 1

    # Since we are running multiple asyncore-based Clients and a
    # Server in separate threads, we need to specify map={} for the
    # Clients, so they all don't use the (default) global asyncore
    # socket map as the Server...

    # First, start up several clients, delayed by a second.  This will
    # result in several Clients starting at roughly the same time; but they
    # won't all be available
    clients = random.randint(7,15)
    logging.info("Starting %d clients...", clients)

    port = unique_port( mincemeat.DEFAULT_PORT )

    class monotonic(object):
        base = 0

        def value(self):
            self.__class__.base += 1
            return self.base

        def transaction(self, name):
            num = self.value()
            return {
                "datasource": repeat_command( command="%s-%d%s" % (
                        name, num,
                        {1:"st", 2:"nd", 3:"rd"}.get(
                            num % 100 // 10 != 1 and num % 10 or 0, "th"))),
                "allocation": mincemeat.TaskManager.ONESHOT,
            }

    which = monotonic()

    class Cli(mincemeat.Client):
        def mapfn( self, k, v ):
            """
            The Map function simply returns the identity value for whatever
            unique key was assigned to the Client.

            On every N'th Map request, it sends a command to the Server!
            """
            logging.info( "%s custom mapfn" % self.name() )
            yield k, 1
            if random.randint( 0, 19 ) == 0:
                logging.info("%s sending whatever command!" % (
                        self.name() ))
                self.send_command( "whatever" )

    class Svr(mincemeat.Server):
        def unrecognized_command( self, command, data, txn, chan ):
            logging.info("%s received command %s from %s" % (
                    self.name(), command, chan.name() ))
            self.set_datasource( **which.transaction( "backchannel" ))
            return True
    cli = []
    for _ in xrange(clients):
        c = Cli(map={})
        cli.append(c)
        t = threading.Timer(1.0, c.conn,
                        args=("", port),
                        kwargs={"password": "changeme"})
        t.daemon = True
        t.start()

    # Our map function simply returns one key, value from each Client: the
    # unique number assigned to the client, mapped to the value 1.
    sd = mincemeat.Server_daemon( cls=Svr,
        credentials={
            "password": "changeme",
            "port":     port,
            })

    # Server's default TaskManager cycle is SINGLEUSE; it will shut down after
    # last queued datasource (which specify a cycle of PERMANENT) is complete.
    # Switch to PERMANENT; we'll switch back to SINGLEUSE when we want the test
    # to end.
    sd.endpoint.taskmanager.defaults["cycle"] = mincemeat.TaskManager.PERMANENT


    # Set up a number of Map/Reduce Transactions.  Each of these will complete
    # with whatever number of clients are authenticated by that time.  This
    # should increase, as we go thru the Transactions...
    transactions = random.randint( 3, 10 )
    logging.info( "Running %d transactions...", transactions )
    for t in xrange(transactions):
        sd.endpoint.set_datasource( **which.transaction( "initial" ))

    # Also, schedule a sub-second repeating event to schedule another datasource
    # -- from inside the Server event loop.  This will test that set_datasource
    # doesn't improperly interfere with processing ongoing Transactions, when
    # enqueueing another datasource.  We should get at least one of these
    # scheduled and processed before the Server goes idle and quits...
    sd_scheduled = ( mincemeat.timer(),
          lambda: sd.endpoint.set_datasource(
                **which.transaction( "scheduled" )),
          0.75 )
    sd.endpoint.schedule.append( sd_scheduled )

    # Run server 'til all datasources are complete, then check Server's .output
    # for multiple results.
    sd.start()
    state = None
    chans = 0
    logging.warning("Test starting...")
    limit = mincemeat.timer() + 60
    stopping = False
    while ( mincemeat.timer() < limit 
            and (not sd.endpoint.finished() 
                 or len(sd.endpoint.taskmanager.channels) > 0 )):

        newchans = len(sd.endpoint.taskmanager.channels)
        if newchans != chans:
            logging.warning("Server has %2s channels", newchans)
            chans = newchans
            
        newstate = sd.endpoint.taskmanager.state
        if newstate != state:
            logging.warning("Server is %s", mincemeat.TaskManager.statename[newstate])
            state = newstate

        time.sleep(.1)
        if which.base >= 50 and not stopping:
            # Shut down the server.  Wait a bit for all the Clients to shut down
            # cleanly, just to avoid a "Broken pipe" as they attempt to shut
            # down their sockets...
            logging.warning("Test completing...")
            sd.endpoint.taskmanager.defaults["cycle"] = mincemeat.TaskManager.SINGLEUSE
            limit = mincemeat.timer() + 10
            stopping = True

    logging.warning("Test done.")
    sd.stop()

    expected = {}
    length = len(sd.endpoint.output)
    assert length >= transactions # At least one "sched-#" processed...

    last = None
    result = sd.endpoint.results()
    while result is not None:
        logging.info("Result: %s" % result)
        if last is not None:
            assert len(result) >= len(last)
        last = result
        result = sd.endpoint.results()
