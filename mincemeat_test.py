
import logging
import time

import mincemeat


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


def test_bind():
    logging.basicConfig(level=logging.DEBUG)

    s1 = mincemeat.Server_daemon(credentials=credentials, timeout=5.)
    s1.start()
    time.sleep(1)
    assert s1.state() in ["processing", "authenticated"]

    try:
        s2 = mincemeat.Server()
        s2.conn(**credentials)
        assert False == "Should have thrown Exception in bind()!"
    except Exception, e:
        assert "Only one usage of each socket address" in str(e) \
            or "Address already in use" in str(e)

    s1.stop()

