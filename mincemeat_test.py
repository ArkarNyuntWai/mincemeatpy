
import logging
import time

import mincemeat

import asyncore

def slow(fun, amt):
    def wrapper(*args, **kwargs):
        time.sleep(amt)
        fun(*args, **kwargs)
        time.sleep(amt)
    return wrapper

data = ["Humpty Dumpty sat on a wall",
        "Humpty Dumpty had a great fall",
        "All the King's horses and all the King's men",
        "Couldn't put Humpty together again",
        ]
# The data source can be any dictionary-like object

datasource = dict(enumerate(data))

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

logging.basicConfig(level=logging.INFO)

def test_bind():
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

def test_schedule():

    import collections
    import Queue

    s1 = mincemeat.Server_daemon(credentials=credentials)
    s1.start()

    res = Queue.Queue()
    cs = collections.deque()
    to = mincemeat.timer() + 5.
    cs.append((to, lambda: res.put(mincemeat.timer()), None))
    c1 = mincemeat.Client_daemon(credentials=credentials, schedule=cs)
    c1.start()
    time.sleep(1)
    state=c1.state()
    assert state == "authenticated"

    try:
        r = res.get(timeout=2)
    except Exception, e:
        assert type(e) == Queue.Empty

    r = res.get(timeout=4)
    assert abs(r-to) < .1

    # Test tidy shutdown:
    #  client shutdown   -->
    #  svrchn close      -->
    #  client close
    # A tuple of bools compares like a binary number:
    #   (True, False) > (False, True)

    c1.endpoint.send_command_backchannel("ping")
    time.sleep(.25)

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
