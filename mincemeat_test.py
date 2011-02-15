
import asyncore
import collections
import logging
import Queue
import random
import threading
import time

import mincemeat


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
    'port':             mincemeat.DEFAULT_PORT,

    'datasource':       None,   # Causes TaskManager to stay idle
    'mapfn':            mapfn,
    'collectfn':        None,
    'reducefn':         reducefn,
    'finishfn':         None,
}

logging.basicConfig(level=logging.ERROR)

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
    s1 = mincemeat.Server_daemon(credentials=credentials, timeout=5.)
    state = s1.state()
    assert state == "idle"
    s1.start()
    time.sleep(1)
    state = s1.state()
    assert state == "authenticated"

    try:
        s2 = mincemeat.Server()
        s2.conn(**credentials)
        assert False == "Should have thrown Exception in bind()!"
    except Exception, e:
        assert "Only one usage of each socket address" in str(e) \
            or "Address already in use" in str(e)

    s1.stop()
    state = s1.state()
    assert state == "success"


def put(thing, queue):
    logging.info("Putting %s on %s" % (thing, queue))
    queue.put(thing)

def test_schedule():
    """
    Tests scheduled events.
    """
    s1 = mincemeat.Server_daemon(credentials=credentials)
    s1.start()

    res = Queue.Queue()
    sch = collections.deque()
 
    # Wait 1 second, and put something on 'res' Queue
    then = mincemeat.timer() + 1.
    sch.append((then, lambda: put(mincemeat.timer(), res), None))
    c1 = mincemeat.Client_daemon(credentials=credentials, schedule=sch)
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
        getstate = lambda count: (c1.endpoint.closed,
                                  svrchn.closed,
                                  c1.endpoint.shutdown, 
                                  count)
        count = 0
        last = getstate(count); count += 1
        assert last == (False, False, False, 0)
        start = now = mincemeat.timer()
    
        svrchn.handle_close = slow(svrchn.handle_close, .05)
    
        logging.info("%s: %s" % (time.ctime(), last))
        while ( now - start < 1. ):
            state = getstate(count); count += 1
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
    count = random.randint(1,5)
    scale = count * 73

    # Since we are running multiple asyncore-based Clients and a
    # Server in separate threads, we need to specify map={} for the
    # Cliens, so they all don't use the (default) global asyncore
    # socket map as the Server...
    logging.info("Starting %d clients...", count)
    for _ in xrange(count):
        c = mincemeat.Client(map={})
        t = threading.Timer(1.0, c.conn,
                        args=("", mincemeat.DEFAULT_PORT),
                        kwargs={"password": "changeme"})
        t.daemon = True
        t.start()

    s = mincemeat.Server(map={})
    s.datasource = dict(enumerate(data * scale))
    s.mapfn = mapfn
    s.reducefn = reducefn
    
    now = mincemeat.timer()
    results = s.run_server(password="changeme")
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

