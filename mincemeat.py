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

from __future__ import with_statement

import asynchat
import asyncore
import cPickle as pickle
import collections
import hashlib
import hmac
import logging
import marshal
import new
import optparse
import os
import random
import repr
import select
import socket
import sys
import threading
import time
import timeit
import traceback
import types

VERSION = 0.1

DEFAULT_PORT = 11235

# Choose the best high-resolution timer for the platform
timer = timeit.default_timer

# ##########################################################################
# 
# threading.*           -- Modernise pre-2.6 threading interface
# 
#     Pre-2.6 threading.* didn't have is_alive, etc...  Set up some
# aliases, so it looks more modern.
if not hasattr(threading, 'current_thread'):
    threading.current_thread = threading.currentThread
if not hasattr(threading.Thread, 'is_alive'):
    threading.Thread.is_alive = threading.Thread.isAlive
if not hasattr(threading.Thread, 'name'):
    threading.Thread.name = property(threading.Thread.getName,
                                     threading.Thread.setName)
if not hasattr(threading._Event, 'is_set'):
    threading._Event.is_set = threading._Event.isSet
if not hasattr(threading._Condition, 'notify_all'):
    threading._Condition.notify_all = threading._Condition.notifyAll


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


# ##########################################################################
# 
# Mincemeat_class       -- Interface required by mincemeat.*_daemon
# Mincemeat_daemon      -- Standard daemon Thread for mincemeat Client/Server
# 
class Mincemeat_class(object):
    """
    The default API for a target class of a Mincemeat_daemon.  No
__init__ (takes no args).

    The normal phases the *_daemon will take us thru is:

     |
 _daemon.__init__  -- User code creates the _daemon 
     | __init__      -- creates our instance specified by cls=...
     | conn          -- invokes to set up networking
     |
 _daemon.start()   -- invokes Thread.run(), which ultimately invokes
   _daemon.process   -- the main _daemon process loop (with optional keywords)
     | starting        -- provides us with all the optional keywords 
   +>|(while more I/O events may occur)
   | | process         -- invoked by _daemon.process w/ minimum computed timeout
   | | timeout         -- invoked after each timeout (when more events pending)
   | |   health          -- we call this in base timeout, below
   +-+ 
     | stopping    -- invoked once after no more events pending
     v


    """
    def starting(self, *args, **kwargs):
        """
        Invoked at the daemon Thread start-up, with all the (optional)
        keyword args.

        Responds to the following keywords:
        
            interval    -- schedules self.ping on the specified repeat interval
        
        """
        logging.info("%s starting: %s, %s" % ( 
                self.name(), repr.repr(args), repr.repr(kwargs)))

        interval = kwargs.get("interval", None)
        if interval:
            logging.info("%s scheduling ping every %s seconds" % (
                    self.name(), interval))
            self.schedule.append(
                (timer() + interval, self.ping, interval))

    def process(self, timeout=None):
        """
        Run this Mincemeat endpoint (and anything else sharing its _map),
        cleaning it up on failure.  (See mincemeat.process or
        mincemeat.Client.process for more details).  Returns when no more events
        are available to process, or the timeout= expires (never times out, if
        None.)

        By the time this is invoked, any optional parameters passed to
        *_daemon.__init__(), and thence to *_daemon.process(), and finally here,
        must have been soaked up by matching positional arguments in derived
        class' process methods, before we get here.  Only timeout=... is
        allowed.
        """
        return process(timeout=timeout, map=self._map, schedule=self.schedule)

    def timeout(self):
        """
        May be invoked by on every timeout, if desired (the supplied
        *_daemon implementations do so).  If overridden, make sure
        to call this (or health checking won't remain hooked up).

        Return a desired numeric timeout, in seconds; it will be used
        to shorten the intended timeout.  None implies no shorter
        timeout desired.  The pattern that overriding methods should
        follow for computing shortened timeouts is:
        """
        shorten = None

        # ... do stuff that might set shorten to a numeric desired timeout

        requested = self.health()
        shorten = minimum_time(shorten, requested)
        
        # Alternatively, if you have other state you need to update if you
        # detect a shorter timeout, use:
        # 
        # if shorten is None or (requested is not None and requested < shorten):
        #     shorten = requested
        #     # ... other things you need to do when requested is used ...

        # ... other stuff; repeat above stanza for each new desired timeout

        return shorten

    def health(self):
        """
        May be used to implement health checks (some of supplied
        *_daemon implementations do so).

        Should check health, and return a desired timeout 'til the
        next desired health check (default None implies no target
        timeout desired).
        """
        pass

    def stopping(self):
        """
        Invoked at the end of processing, after no more events are
        detected by the Mincemeat_daemon.
        """
        pass


class Mincemeat_daemon(threading.Thread):
    """
    A mincemeat Map/Reduce Client or Server daemon thread.  A non-None
    numeric timeout= keyword arg creation will cause timeout() to be
    fired at least every about that many seconds.  By default,
    allocates a private asyncore socket map, so we can process async
    I/O events using a separate thread from any other asyncore derived
    object.

    Note that an overridden Thread.run method does *not* receive the
    args= or kwargs= passed to threading.Thread.__init__; Only a
    target=... method invoked by the default run() does!  This is not
    overly clear in the Python threading.Thread documentation.

    Raises exception on the Server's failure to bind, or a Client's
    failure to connect.

    After start(), processes 'til the processing loop completes, and
    enters "success" state; if an exception terminates processing,
    enters a "failed: ..." state.
    """
    def __init__(self, credentials, cls=None,
                 map=None, schedule=None, shuttout=.5,
                 **kwargs):
        threading.Thread.__init__(self, target=self.process,
                                  kwargs=kwargs)
        self.daemon = True
        self._state = "idle"

        # Instantiate the (NOT optional!) Client or Server class, and invoke its
        # conn with the (also NOT optional) credentials.  We make cls=None to
        # ensure that they are not positional parameters, and force this to
        # crash if not specified.
        if map is None:
            map = {}
        if schedule is None:
            schedule = collections.deque()
        self.endpoint = cls(map=map, schedule=schedule, shuttout=shuttout)
        if not hasattr(self.endpoint, '_map'):
            raise AttributeError("%s requires a cls=... object be derived from asyncore.dispatcher"
                                  % (self.__class__))
        self.endpoint.conn(asynchronous=True, **credentials)

    def name(self):
        return self.endpoint.name()

    def state(self):
        if self._state == "processing":
            if self.endpoint.authenticated():
                self._state = "authenticated"
        return self._state

    def process(self, **kwargs):
        """
        This is the threading.Thread's target= method; the daemon's
        main method.  It is invoked once; when this method returns,
        the daemon Thread is done, and may be join()-ed.  All of the
        keyword arguments are passed to the target mincemeat Server or
        Client.starting(...) method.

        Responds to the following keywords:
        
            timeout     -- the longest interval between invocations of timeout()

        Invokes the process() method in the mincemeat Client or
        Server.  Any extraneous arguments passed to the constructor
        are assumed to be arguments passed to Client or
        Server.process(...).  By default, the only one it understands
        is timeout=.  If specified, Fires the *_daemon.timeout() and
        mincmeat.Server/Client .timeout() methods.  Interprets their
        return values as desired 

        Implements the default processing loop.  If no timeout= was
        specified to __init__, it will simply process with no timeout,
        'til underlying endpoint Client/Server object is finished (its
        asyncore.dispatcher._map is empty).  The timeout() methods
        will never be fired, until the very end (with done=True).

        If timeout() is overriden in derived class, pass a timeout=#
        keyword arg to class constructor to implement a timed
        event-loop.  Then, timeout() will fire at least every timeout=
        seconds; if timeout returns a smaller numeric value, this will
        be used to reduce the next timeout interval.
        """
        logging.info("%s processing (%s)" % (
                self.name(), repr.repr(kwargs)))
        self._state = "processing"

        try:
            self.endpoint.starting(**kwargs)

            # Pull 'timeout' out of the keyword args (None if
            # unspecified) If non-None, we'll kick things off with an
            # immediate timeout, before proceeding with the default.
            # This allows us to specify very large timeout, if we know
            # that we'll primarily be depending on our timeout service
            # routines to "shorten" it.
            timeout = kwargs.get('timeout', None)
            shorten = timeout if timeout is None else 0.0
            while self.endpoint.process(timeout=shorten):
                shorten = timeout
                requested = self.timeout()
                shorten = minimum_time(shorten, requested)
                
            self.endpoint.stopping()
        except Exception, e:
            logging.info("%s failed: %s" % (self.name(), e))
            self._state = "failed: %s" % e
        else:
            if self.endpoint.finished() and self.endpoint.authenticated():
                self._state = "success"
            else:
                self._state = "failed: incomplete"
        logging.info("%s stopping: %s" % (
                self.name(), self.state()))

    def timeout(self):
        """
        Override in derived class (and remember to pass a numeric
        timeout= keyword argument to __init__ on daemon creation), if
        a timed event-loop is desired.  It is invoked at every timeout
        until the event loop detects completion.

        Remember to invoke this base-class method, if you wish to
        chain timeout() invocations into the mincemeat.Client/Server
        (ie. to allow it to implement health checks, etc)!

        May optionally return a numeric value; it will be used to
        specify a shorter desired timeout, in seconds. Returning None
        means that the standard timeout (the timeout= keyword passed
        to __init__) will be applied.
        """
        shorten = None

        requested = self.endpoint.timeout()
        shorten = minimum_time(shorten, requested)

        return shorten

    def stop(self, timeout=5.):
        """
        Stop (and join) the thread (even if incomplete).

        To stop an asyncore.dispatcher based server or client, we'll
        have to make it close its connections, which will cause its
        asyncore.loop to cease processing, and its thread to stop.
        
        If the server doesn't tidily shut down within the timeout,
        we'll take more forceful measures.  This could occur if a
        client is frozen and won't respond to an EOF.
        """
        # It is safe (no-op) to invoke this multiple times
        self.endpoint.handle_close()
        self.join(timeout)
        # Still not dead?  Some client must be frozen.  Try harder.
        if self.is_alive():
            close_all(map=self.endpoint._map, ignore_all=True)
            self.join(0.)


# ##########################################################################
# 
# asynchat:
#   async_chat  -- thread-safe implementation of async_chat, w/2.6 interface
# asyncore:
#   close_all   -- close all sockets in asyncore socket map, w/2.6 interface
#   process     -- asyncore.loop, with synchronous scheduling and timeouts
# 
#     Implements a thread-safe version of the traditional asynchat.async_chat
# and asyncore.loop processing loop supporting arbitrary numbers of
# asyncore.dispatcher and/or asynchat.async_chat based objects sharing a single
# socket map.  Presents the Python 2.6 interface, with 2.5 compatibility.
# 
#      Using mincemeat.process instead of asyncore.loop also implements
# auto-firing scheduled events (one-shot or repeating), while strictly
# respecting the (optional) caller-defined timeout.
# 
def process(timeout=None, map=None, schedule=None):
    """
    Processes asyncore based Server or Client events 'til none left
    (or timeout expires), dispatching scheduled events as we go.  On
    Exception, forcibly cleans up all sockets using the same asyncore
    map.  Return True while more events may be possible, and the
    caller should re-invoke.

    If multiple sets of asyncore based objects use separate maps, then
    each separate map needs to be run using a process (or
    asyncore.loop) in a separate thread.  For example, a single Python
    instance may run both a Server, and one or more Clients, or
    multiple independent Server instances listening on different
    ports.

    Process asyncore events until the specific timeout (if not None)
    expires, then return.  There is no guarantee, however, that we
    will not return slightly after the timeout expires; the overage
    may be as long as the duration of the longest event handler.

    Unfortunately, we need to have knowledge of how asyncore.loop's
    exit condition works; it looks at 'map' (or the global
    asyncore.socket_map), and loops 'til it is empty.  We'll return
    True while there may be more events to process.

    Since we have elected to try to do "tidy" shutdowns of sockets on
    controlled exit events (eg. when the Server or Client is directed
    to terminate due to external events), we need to be able to
    schedule future events.  In addition, there is a general need in
    asynchronous event-driven applications to also support timed
    events.  For example, a timeout on operations that should respond
    within a certain window.

    Therefore, we don't actually allow unbounded timeouts, and we
    always specify count=1, to ensure that we return here after
    processing every I/O event.  Thus, if (during the processing of
    each event) another scheduled event was added, it will influence
    the timeout of the next invocation.  It is recommended that a
    thread-safe container like collections.deque is used, if multiple
    threads may schedule events.
    
    All schedule events are of the form (expiry,callable,repeat),
    where expiry is a timer() value, and callable is any callable
    (normally, a bound method of an asyncore.dispatcher based object),
    and repeat is a number of seconds (or None)
    """
    try:
        if map is None:
            map = asyncore.socket_map   # Internal asyncore knowledge!

        beg = now = timer()
        end = None              # Force one loop; computes end below, if timeout
        while map and (end is None or now < end):
            # Events on 'map' still possible, and either 'timeout' is infinite,
            # or we haven't yet reached it.  Trigger scheduled events, and get
            # earliest expiry remaining in schedule (arbitrary time passes...)
            nxt = trigger(schedule, now=now)
            now = timer()

            # Done firing (and rescheduling) any expired events.  The earliest
            # of the remaining scheduled events (and newly rescheduled events)
            # is in nxt.  Now, further limit 'nxt' by 'timeout'.  Compute 'end'
            # for real, to terminate process loop.  Finally, compute remains:
            # None --> no timeout or other event expiry, 0 --> next scheduled
            # expiry/timeout already passed, or +'ve --> future expiry.
            if timeout is not None:
                end = beg + timeout
                nxt = minimum_time(nxt, end)

            remains = None
            if nxt is not None:
                remains = max(0., nxt - now)

            logging.debug(
                "mincemeat.process socks: %s, sch: %s, dur: %.6f, rem: %.6f of t/o: %.6f" % (
                    len(map),
                    len(schedule) if schedule is not None else None,
                    now - beg,
                    remains if remains is not None else float("inf"),
                    timeout if timeout is not None else float("inf")))

            asyncore.loop(timeout=remains, map=map, count=1)
            now = timer()

    except Exception, e:
        # We're no longer running asyncore.loop, so can't do anything cleanly;
        # just close 'em all...  This should ensure that any socket resources
        # associated with this object get cleaned up.  In order to ensure that
        # the original error context gets raised, even if the asyncore.close_all
        # fails, we must use re-raise inside a try-finally.
        try:
            raise
        finally:
            close_all(map=map, ignore_all=True)

    # True iff map isn't empty: there may be more asynchronous I/O events
    return bool(map)


def function_name(func):
    """
    Return a more human readable name for an (optionally bound) function.
    """
    return '.'.join( ([] if not hasattr(func, "im_class") 
                      else [func.im_class.__name__])
                     + [func.__name__])


def minimum_time(least, *rest):
    """
    Computes the minimum of an arbitrary number of wall-clock times (in seconds
    since the Epoch) or durations (in seconds), where None implies no time or
    duration.  In other words, any time (including 0.) is greater than None.
    """
    for t in rest:
        if least is None or (t is not None and t < least):
            least = t

    return least


def next_future_time(expiry, repeat, now=None):
    """
    Computes the next (future) expiry of a repeating event, relative to 'now'.
    Returns that expiry time, the number of repeats missed, and the amount
    we've already passed into the current repeat interval (-'ve if not yet
    expired!).

    Returns the number of repeat intervals already passed since expiry, and the
    portion of the next interval already passed (usually ignored -- it embodies
    the error of the system timer()).
    
    Since we don't save the very original scheduled expiry, we ensure it
    will creep over time by an amount proportional to the limit of the
    precision of a Python float (ie. not much) -- so we always base exp
    from itself, NOT now (which may have a much lower precision, on the
    order of 1ms!)
    """
    if now is None:
        now = timer()
    if now >= expiry:
        missed, passed = divmod(now - expiry, repeat)
        expiry += missed * repeat + repeat
    else:
        missed = 0.
        passed = now - expiry

    return expiry, missed, passed


def trigger(schedule, now=None):
    """
    Dispatch (and re-schedule) expired scheduled events.  Returns the next
    scheduled event's expiry (or None).  Modifies the passed schedule.
    """
    if not schedule:
        return None
    if now is None:
        now = timer()
    nxt = None
    for exp, fun, rpt in sorted(schedule):
        if now >= exp:
            # Scheduled event expired; Fire fun()!  Arbitrary time passes...
            logging.debug("Firing scheduled event %s (repeat: %s)" % (
                    function_name(fun), rpt))
            try:
                fun()
            except Exception, e:
                # A scheduled event failed; this isn't
                # considered fatal, but may be of interest...
                logging.warning("Failed scheduled event %s: %s\n%s" % (
                        fun, e, traceback.format_exc()))
            # Arbitrary time has passed (exception or not)
            now = timer()
        else:
            # Scheduled event is in future; done.
            nxt = minimum_time(nxt, exp)
            break

        # This event was at or before now (and was fired); remove it and loop to
        # get next one (if any).
        schedule.remove((exp, fun, rpt))
        if rpt:
            # Repeat is not None/zero.  Careful re-schedule the event.  If we've
            # already missed one (or more) timeouts, log this fact, and
            # re-schedule for the next future multiple of repeat interval.
            exp, mis, pas = next_future_time(exp, rpt, now=now)
            nxt = minimum_time(nxt, exp)
            if int(mis):
                logging.info("Missed %d firings (and %.3f s of next)"
                             " of scheduled event %s" % (
                        int(mis), pas, function_name(fun)))
            schedule.append((exp, fun, rpt))

    return nxt


def close_all(map=None, ignore_all=False):
    """
    Safely close all sockets, forcing loop (using the same map) to
    exit.  Handles pre-2.6 asyncore.close_all args.
    """
    try:
        if map is None:
            map = asyncore.socket_map
        logging.warning("Forcibly closing remaining %d sockets: %s" % (
                len(map), repr.repr(map)))
        asyncore.close_all(map=map, ignore_all=True)
    except TypeError:
        # Support pre-2.6 asyncore; no ignore_all...
        try: 
            asyncore.close_all(map=map)
        except Exception, e:
            logging.warning("Pre-2.6 asyncore; socket map cleanup incomplete: %s" % (e))
    

class async_chat(asynchat.async_chat):
    """
    Support multithreaded access to pushing data for transmission on
    the async_chat FIFO.  If any other (non-asyncore.loop) thread
    pushes data, it should probably invoke flush_backchannel.
    Otherwise, the data may not be (completely) sent, until the
    asyncore.loop awakens from its select/poll (due to other socket
    events, or timeout), and notices that this socket has data to
    send.

    Therefore, we have a 'sending' lock that we use to synchronize
    with the asyncore.loop thread; asyncore.loop holds the thread any
    time it is dealing with putting things ONTO the async_chat outgoing
    FIFO (and calling initiate_send); we hold it while our thread is
    putting things onto the FIFO (and calling initiate_send).

    Also supports pre-2.6 asynchat, which didn't directly support a non-global
    asyncore socket map.
    """
    def __init__(self, sock, map=None):
        # Preserve pre-2.6 compatibility by avoiding map, iff None.
        # Also first keyword name changed, so pass as simple arg...
        if map is None:
            asynchat.async_chat.__init__(self, sock)
        else:
            # However, if we must specify a custom map...
            try:
                asynchat.async_chat.__init__(self, sock, map=map)
            except TypeError, e:
                # ... before 2.6, async_chat wasn't updated to pass a
                # custom socket map thru to asyncore.dispatcher...
                # So, we have to intercept the global socket_map,
                # __init__ it, and always fix it up after.
                logging.debug("Pre-2.6 asynchat; intercepting socket map: %s" % e)
                try:
                    save_asm = asyncore.socket_map
                    asyncore.socket_map = map
                    asynchat.async_chat.__init__(self, sock)
                finally:
                    asyncore.socket_map = save_asm

        self.send_lock = threading.Lock()
        self.sending = False            # Is asyncore.loop sending data?
        self.reading = False            # Is asyncore.loop reading data?
        self.shutdown = False           # Have we already shut down?
        self.closed = False

    def handle_read(self):
        """
        We want to be aware of when the event handling loop is reading
        data.  Any handle_close triggered during this time indicates
        an EOF, and should result in an immediate close.
        
        Note that we do *not* support multi-threaded reading of
        incoming data!
        """
        self.reading = True
        asynchat.async_chat.handle_read(self)
        self.reading = False

    def writable(self):
        """
        Serialize any async_chat handlers that we could fire and
        result in access to the async_chat output FIFO.  Any derived
        class that overrides these should be careful to invoke these,
        and itself be thread-safe.
        """
        with self.send_lock:
            self.sending = asynchat.async_chat.writable(self)
        return self.sending

    def handle_write(self):
        with self.send_lock:
            asynchat.async_chat.handle_write(self)

    def push(self, data):
        with self.send_lock:
            asynchat.async_chat.push(self, data)

    def push_with_producer(self, producer):
        with self.send_lock:
            asynchat.async_chat.push_with_producer(self, producer)

    def flush_backchannel(self, now=None, timeout=None):
        """
        Usher out pending async_chat FIFO data, 'til the asyncore.loop
        thread acknowledges that it knows about it.

        An optional timeout may be supplied; None implies no timeout
        (wait until either no data remains, or asyncore.loop
        acknowledges), or 0 (no delay; only invoke handle_write once
        to try to send some data, and then return immediately)
        """
        if now is None:
            now = timer()

        while not self.sending:
            # asyncore.loop's select doesn't think we need to write,
            # but we need to ensure that our data is sent.  Exceptions
            # during attempting to send will flow back to the caller.
            # 
            # This is a non-locked check; could there be a race
            # condition here?  Could we see self.sending as True (from
            # some previous check), but the aysncore.loop is about to
            # invoke writable(), and discover a False?  Our caller
            # must have just pushed some data, either before OR after
            # the asyncore.loop's called writable (because they are
            # all serialized on the same lock).  So, if self.sending
            # is False, the check was from before the push; if after
            # (and it's False), then the FIFO was actually empty (data
            # already sent).  So, no race.
            with self.send_lock:
                if asynchat.async_chat.writable(self):
                    # asyncore.loop doesn't (yet) know we need to
                    # write, and we know that we need to...
                    asynchat.async_chat.handle_write(self)
                    writable = asynchat.async_chat.writable(self)
                    if self.sending or not writable:
                        # asyncore.loop knows now, or we're empty; done.
                        break
                else:
                    # asyncore.loop emptied us; we're done.
                    break

            # We checked, tried to send, and checked again; we still have data
            # to send, and asyncore.loop's select still doesn't know about it!
            # We are out of the lock now, so the asyncore.loop thread may now
            # awaken (if it has detected some other activity), and then take
            # note that there is data to send.  If we have time, and our fileno
            # is still valid, wait...
            fileno = self._fileno
            elapsed = timer() - now
            if fileno is None or ( timeout is not None
                                   and elapsed >= timeout ):
                break
            
            # We're not empty, and asyncore.loop doesn't yet know.
            # Any non-None and non-zero timeout has not yet elapsed.
            # If timeout is None or 0, it is used as-is; otherwise,
            # the remaining duration is computed.  Wait a bit.
            try:
                r, w, e = select.select([], [fileno], [fileno],
                                        ( timeout
                                          and ( timeout - elapsed )
                                          or timeout ))
                if e:
                    self.handle_expt()
            except select.error, e:
                if e.arg[0] != errno.EINTR:
                    # Anything that wakes us up harshly will also wake up the
                    # main loop's select; Done (except ignore well-behaved
                    # interrupted system calls).
                    break

    def close(self):
        """
        Keep track of when we finally actually closed the socket.
        """
        self.closed = True
        asynchat.async_chat.close(self)

    def tidy_close(self):
        """
        Indicate completion to the peer, by closing outgoing half of socket,
        returning True iff we cleanly shutdown the outgoing half of the socket.
        We only do this once, and only if we aren't reading (indicating an EOF),
        closed, or in an error condition (or have a bad socket or fd!)
        
        This will result in an EOF flowing through to the client, after the
        current operation is complete, eventually leading to a handle_close
        there, and finally an EOF and a handle_close here; the kernel TCP/IP
        layer will cleanly release the resources.  See http://goo.gl/EtAyN for a
        confirmation of these semantics for Windows.

        This may, of course, not actually flow through to the client (no route,
        client hung, ...)  Therefore, the caller needs to ensure that we wake up
        after some time, and forcibly close our end of the connection if this
        doesn't work!  If we haven't been given provision to do this, we need to
        just close the socket now.
        """
        if self.reading is False:
            if self.closed is False:
                if self.shutdown is False:
                    self.shutdown = True
                    try:
                         if 0 == self.socket.getsockopt(socket.SOL_SOCKET,
                                                        socket.SO_ERROR):
                             # No reading (EOF), and socket not in error.
                             # There's a good chance that we might be able to
                             # cleanly shutdown the outgoing half, and flow an
                             # EOF through
                             logging.info("%s shutdown to peer  %s" % (
                                     self.name(), str(self.addr)))
                             self.socket.shutdown(socket.SHUT_WR)
                             return True
                    except Exception, e:
                        logging.info("%s shut down failed: %s" % (
                                self.name(), e ))
                else:
                    logging.debug("%s shut down already!" % self.name())
            else:
                logging.debug("%s closed already!" % self.name())
        else:
            logging.debug("%s shut down at EOF!" % self.name())

        return False


# ##########################################################################
# 
# Protocol              -- Core mincemeat communication protocol
# Client                -- A mincmeat Client registers with a Server, rx. tasks
# Server                -- A mincmeat Server awaits Clients, issues tasks
# ServerChannel         -- A Server's channel to one Client
# TaskManager           -- Breaks Map/Reduce Transactions into tasks
# 
#     Building blocks used to construct a Map/Reduce engine.
# 
class Protocol(async_chat):
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

    The primary state machine is driven by incoming protocol data on
    the socket.  Commands are receieve and parsed, which triggers
    invocation of methods and local processing, which sends response
    commands and data.  Normally, between receiving incoming command
    and data (and sending back the response command and data), the
    socket is idle.  Local threads are not allowed to invoke
    send_command, as this is not thread-safe and could interleave data
    if a response command is being sent at the same time a local
    thread invokes send_command.  

    In these intervals, a secondary "back channel" is available, via
    calls to send_command_backchannel(command,data).  These
    transmissions of (command, data) are scheduled (thread-safely),
    and are guaranteed to be transmitted *between* normal outgoing
    command responses (due to serializing the underlying
    async_chat.push.)

    If we simply enqueued the data for future transmission, and the
    asyncore loop wasn't *already* waiting for writability on the
    socket (due to existing outgoing data in the FIFO), it could be an
    indeterminate amount of time before the asyncore loop awoke from
    select/poll.  We must have a thread-safe means to send the new
    data (and invoke async_chat.handle_write and ultimately,
    async_chat.initiate_send and socket.send), from another thread,
    while asyncore.loop is asleep (or even when awake, iff it is doing
    something not output FIFO related).  That is where
    flush_backchannel() comes in...
    """
    def __init__(self, sock=None, map=None, schedule=None, shuttout=None):
        """
        A Map/Reduce client Protocol instance.  Optionally, provide a socket map
        shared by all asyncore based objects to be activated using the same
        asyncore.loop.  A schedule is necessary; a shuttout is recommended, or
        tidy closes won't be used.  However, for backwards compatibility, we'll
        default to hard closes.
        """
        async_chat.__init__(self, sock, map=map)
        if schedule is None:
            schedule = collections.deque()
        self.schedule = schedule
        self.shuttout = shuttout

        self.set_terminator("\n")
        self.buffer = []
        self.auth = None
        self.mid_command = False
        self.what = "Proto."
        self.peer = True                # Name shows >peer i'face by default
        self.locl = None                # Just like self.addr, 'til known
        # Since we cannot predict that the condition_await predicate won't
        # itself check condition, we'll use an re-entrant lock
        self.condition = threading.Condition(threading.RLock())

    def condition_changed(self):
        """
        Some aspect of Protocol state has changed; awaken all threads that might
        be blocked awaiting the change.
        """
        with self.condition:
            self.condition.notify_all()

    def condition_await(self, predicate, timeout=0.):
        """
        Await the predicate returning True, or timeout.  Returns the final
        predicate truth value.

        Avoids the threading.Condition overhead by only invoking wait if
        necessary; The predicate is a one-way flag, so this is safe.

        WARNING

        Must only be used by a Thread *other* than the asyncore.loop
        (mincemeat.process) thread, if a non-zero timeout is specified.  Due to
        deficiencies in the pre-3.2 threading implementation, we must use
        scheduled synchronous timeouts implemented by the loop thread (using
        select), in order to avoid a polling loop in threading.Condition.wait!

        If a non-zero timeout is specified, we'll block 'til we are awakened by
        our scheduled timeout.  We can only miss our scheduled timeout if the
        process() method stays blocked (no socket activity at all), and hence
        the list of scheduled activities is not re-evaluated before our timeout
        is scheduled.

        Therefore, the timeout should be considered a "suggestion"; we'll be
        awakened either before or at the scheduled timeout, or when the socket
        is closed (or some unrelated timeout occurs).

        BACKGROUND
        
        Providing a non-zero timeout to threading.Condition.wait probably
        doesn't mean what you think it means...  Before Python 3.2, these
        timeouts are actually implemented as a fairly tight polling loop in
        threading.Condition.wait; use sparingly.  However, http://goo.gl/Oj7GF
        sparked an idea:

        Since we already have another thread handy (the process/ asyncore.loop
        thread), and we can schedule it to perform synchronous scheduled events,
        we'll use that to awaken all the threads awaiting this condition; then,
        each of them can just go back to sleep if their time isn't up ('cause
        they have their own wake-up call scheduled.)

        We must ensure that a thread can never miss its wake-up call, during the
        moments between processing someone else's wake-up call, and going back
        to sleep.  From http://goo.gl/tHjcm, Tricks of the Trade:

          # In A:                             In B:
          #
          # B_done = condition()              ... do work ...
          # B_done.acquire()                  B_done.acquire(); B_done.release()
          # spawn B                           B_done.signal()
          # ... some time later ...           ... and B exits ...
          # B_done.wait()
          #
          # Because B_done was in the acquire'd state at the time B was spawned,
          # B's attempt to acquire B_done can't succeed until A has done its
          # B_done.wait() (which releases B_done).  So B's B_done.signal() is
          # guaranteed to be seen by the .wait().  Without the lock trick, B
          # may signal before A .waits, and then A would wait forever.

        We acquire the Condition's lock before scheduling the future
        Condition.notify_all, and we hold it (except during Condition.wait)
        during the entire period 'til our timeout expires.  If we awaken (due to
        some other thread's wake-up call), and detect we are not done, even if
        OUR wake-up call triggers, it will block 'til we re-enter Condition.wait
        -- at which time, our notify_all will proceed and awaken us.
        
        Unused wake-up calls may remain in the schedule and fire harmlessly.  If
        Protocol.close is invoked, we'll also wake up (early) and probably
        return False.
        """
        if predicate():
            return True
        elif not (timeout is None
                  or timeout > 0.):
            return False

        # The predicate is not satisfied, and a timeout is desired...
        start = now = timer()
        with self.condition:
            # It is now safe to schedule our .notify_all; we cannot miss it,
            # even if the asyncore.loop thread immediately fires it.  None
            # implies no wake-up call!  We'll wait forever (or 'til closed())
            expiry = None if timeout is None else now + timeout
            if expiry is not None:
                self.schedule.append(
                    (expiry, self.condition_changed, None))
            while (not predicate()
                   and (expiry is None
                        or now < expiry)):
                # We could not re-enter this loop unless now - start >= timeout.
                # Could we possibly miss our wake-up call, by having
                # mincemeat.process trigger it ever-so-slightly early, so our
                # timer() is not satisfied, and we end up waiting here forever?
                # mincemeat.process tests whether timer() >= expiry and fires,
                # we test for now < expiry to continue.  So, unless now() is not
                # monotonic (returns non-increasing values, for increasing
                # wall-clock time), we are safe.
                self.condition.wait()
                now = timer()

        return predicate()

    def name(self, what = None, peer = None):
        """
        Some human-readable name for this Protocol endpoint, including
        the either the local i'face:port, or the destination i'face:port:
        
            name@locl:port
            name>addr:port

        Normally, if a protocol endpoint is the accepting end, it will
        have a locl i'face:port at a well-known (and boring) address,
        and we'll generally want to show the peer's (ephemeral)
        address.  So, default to show peer, unless we find we're on
        the connect path (below).  Regardless, show the address we
        have (if any).
        
        Only updates self.what, .dest if non-empty what or dest is
        provided, and also updates empty self.locl if connected.
        """
        if what is not None:
            self.what = what
        if peer is not None:
            self.peer = peer
        if self.peer and self.addr or not self.locl:
            char = '>'
            addr = self.addr and self.addr or (None, None)
        else:
            char = '@'
            addr = self.locl and self.locl or (None, None)

        return self.what + char + str(addr[0]) + ':' + str(addr[1])

    def update_addresses(self):
        try:
            addr = self.socket.getpeername()
            if self.addr is None:
                self.addr = addr
            elif self.addr != addr:
                logging.debug("%s resolved peer address %s to %s" % (
                        self.name(), str(self.addr), str(addr)))
                self.addr = addr
        except Exception, e:
            logging.info("%s couldn't update peer address: %s" % (
                    self.name(), e))
            pass

        try:
            locl = self.socket.getsockname()
            if self.locl is None or self.locl != locl:
                logging.debug("%s resolved locl address %s to %s" % (
                        self.name(), str(self.locl), str(locl)))
                self.locl = locl
        except Exception, e:
            logging.info("%s couldn't update locl address: %s" % (
                    self.name(), e))
            pass

    def finished(self, timeout=0.):
        """
        We are finished if the socket is either partially shutdown or completely
        closed.  Importantly, this will not report True during the initial
        phases of socket connection (ie. before self.connected becomes True).

        WARNING
        
        Must never be invoked with a non-zero timeout, by the asyncore.loop
        (mincemeat.process) thread which is running this Protocol!
        """
        return self.condition_await(lambda: self.shutdown or self.closed,
                                    timeout=timeout)

    def authenticated(self, timeout=0.):
        """
        Return whether the Protocol endpoint is authenticated.  If non-zero
        timeout specified, block 'til auth'ed, or timeout expiry (or we discover
        we're finished!)  A timeout will expire early if we discover that the
        connection is finished.

        Since authentication involves plenty of socket I/O activity, there is no
        chance of our scheduled timeout being missed completely, unless the
        socket just goes silent; then, we may not be awakened 'til the socket is
        closed (or some other unrelated scheduled timeout occurs).  Therefore,
        if very tight timing is required on detecting authentication, you'll
        need to consider triggering I/O, or something using the very Condition
        .wait(timeout=...) interface we're working here to avoid...

        WARNING
        
        Must never be invoked with a non-zero timeout, by the asyncore.loop
        (mincemeat.process) thread which is running this Protocol!
        """
        self.condition_await(lambda: self.auth == "Done" or self.finished(),
                             timeout=timeout)

        return self.auth == "Done"

    def send_command(self, command, data=None, txn=None):
        """
        Allows the asyncore.loop thread OR an external thread to compose and
        send a command, pushing it onto the async_chat FIFO for transmission.
        This is thread-safe (due to mincemeat.async_chat), even though
        asynchat.async_chat.push... breaks the transmission into output buffer
        sized blocks in push, and appends them to a FIFO; simultaneous calls to
        push could interleave data, but are locked (see mincemeat.async_chat).
        It ultimately initiates an attempt to asyncore.dispatcher.send some
        data.

        When any service is complete, the asyncore.loop service thread will
        prepare to wait in select/poll, and will call writable(), which will
        return True because there is data awaiting transmission in the
        dispatcher's FIFO, so the loop service thread will awaken (later) and
        send data when there is outgoing buffer available on the socket.

        This interface may also be invoked by external threads, via
        send_command_backchannel (below).  In that case, the asyncore.loop
        thread (blocking in its own select/poll) will NOT know that output is
        now ready for sending; until it awakens, the external thread must ensure
        that the data gets sent, using flush_backchannel.

        The command may or may not contain an optional transaction; we append
        it, if supplied.  If an (optional) data segment is supplied, its length
        follows the mandatory ':'

            <command>[/<transaction>]:[length]\n
            [data\n]
        """
        if txn:
            if "/" in command:
                raise SyntaxError("Command already contains transaction id!")
            command += "/" + str(txn)
        if not ":" in command:
            command += ":"
        if data is not None:    # An empty container is different than None!
            pdata = pickle.dumps(data)
            command += str(len(pdata))
            logging.debug("%s -->%s(%s)" % (self.name(), command, repr.repr(data)))
            self.push(command + "\n" + pdata)
        else:
            logging.debug("%s -->%s" % (self.name(), command))
            self.push(command + "\n")

    def send_command_backchannel(self, command, data=None, txn=None, timeout=None):
        """
        Allows another thread (NOT the asyncore.loop) to compose and
        send a command.  If the asyncore.loop thread knows that there
        is already data awaiting transmission, we are done; the
        asyncore.loop will take it from here.

        However, if the asyncore.loop is blocked (indeterminately!) on
        its select/pool awaiting events on other sockets, it may
        *never* wake up to send this data!  We must usher it out, 'til
        we know that the asyncore.loop discovered our socket's
        writability...

        So, loop here (None ==> no timeout, 0. ==> no waiting),
        attempting to send the data, 'til either we're done (no longer
        writable()), or asyncore.loop discovers this fact!

        WARNING

        Must only be used by a Thread *other* than the asyncore.loop
        (mincemeat.process) thread.
        """
        now = timer()
        self.send_command(command, data, txn)
        self.flush_backchannel(now=now, timeout=timeout)

    # ----------------------------------------------------------------------
    # overridden async_chat methods
    # 
    #     The methods collect incoming data and implement the
    # protocol.  They are invoked by the asyncore.loop thread due to
    # I/O events detected on this socket, and invoke the appropriate
    # callbacks as commands are received.
    # 
    def log_info(self, message, type):
        """
        The asyncore.dispatcher.log_info type='name' categorization
        maps directly onto the logging.name; redirect.
        """
        getattr(logging, type)("%s %s" % (self.name(), message))

    def collect_incoming_data(self, data):
        self.buffer.append(data)

    def found_terminator(self):
        """
        Collect an incoming command.  Allowed commands are of the
        following forms:
        
        Un-authenticated commands:

            <command>:<abitrary data>\n

        Authenticated commands:
            challenge:<arbitrary data>\n
            <command>[/<transaction>]:\n
            <command>[/<transaction>]:length\n<length bytes of data>

          Transaction IDs may be appended to a command, and are passed
          unmollested in the 'txn=' keyword arg process_command.
        """
        merged = ''.join(self.buffer)
        if not self.authenticated():
            logging.debug("%s<-- %s (unauthenticated!)" % (self.name(), merged))
            command, data = merged.split(":", 1)
            self.process_unauthed_command(command, data)
        elif not self.mid_command:
            logging.debug("%s<-- %s" % (self.name(), merged))
            command, length = merged.split(":", 1)
            if command == "challenge":
                # length is actually arbitrary data, in this case
                self.process_command(command, length)
            elif length:
                # Continue reading the command's data segment
                self.set_terminator(int(length))
                self.mid_command = command
            else:
                # Command with no data; still may have txn ID
                txn = command.find('/')
                if txn >= 0:
                    self.process_command(command[0:txn], txn=command[txn+1:])
                else:
                    self.process_command(command)
        else:
            # Read the data segment from the previous command
            if not self.authenticated():
                logging.fatal("Recieved pickled data from unauthed source")
                # sys.exit(1)
                self.handle_close()
                return
            data = pickle.loads(merged)
            self.set_terminator("\n")
            command = self.mid_command
            self.mid_command = None
            txn = command.find('/')
            if txn >= 0:
                self.process_command(command[0:txn], data, txn=command[txn+1:])
            else:
                self.process_command(command, data)
        self.buffer = []

    def connect(self, addr):
        """
        Initiate a connect, setting self.addr to a (tentative)
        address, in case if the underlying asyncore.dispatcher.connect
        doesn't; on some platforms (guess...), this is unreliable.

        The accept and connect paths are the two general ways a socket
        establishes endpoint addresses; on the connect path, we don't
        know the actual .addr (peer addr) and .locl (local ephemeral
        addr), 'til the connect completes.  Since we are on the
        connect path, default to showing the local i'face in our name
        (once we're connected, and actually know our ephemeral
        port...)
        """
        if self.addr is None:
            self.addr = addr
        self.name(peer=False)
        logging.info("%s connecting" % (self.name()))
        async_chat.connect(self, addr)

    def handle_connect(self):
        """
        A connect appears to have completed!  We probably need to
        detect the local interface address...  However, certain
        connect failures may show up at this point.

        After this returns, self.connected will be true, 'til self.close() is
        invoked.  Thus, the connection state variables during the normal
        lifespan of the Protocol object are:

   .connected  .shutdown  .closed
    True        False      False  Initial (open sock provided to __init__)
    False       False      False  Initial (no sock to __init__)
    True        False      False  After .handle_connect completes
           ...
    True        True       False  If .handle_close uses .tidy_close
    False       True       True    and, after subsequent .close
    True        False      True   If .handle_close uses .close directly

        """
        self.update_addresses()
        logging.debug("%s connection established %s->%s" % (
                self.name(),
                str(self.locl), str(self.addr)))

    def close(self):
        """
        Keep track of when we finally closed the socket.  Wake up any thread
        awaiting on authenticated; they'll need to do something else...
        """
        self.condition_changed()
        async_chat.close(self)

    def handle_close(self):
        """
        Handle events indicating we should close; EOF on read,
        exceptional conditions indicating an error condition,
        unhandled exception from some other handler invoked from the
        asyncore.loop. 

        Ideally, we would like to separate this into two streams;
        those that indicate that the socket is immediately closable
        (incoming EOF, fatal error condition on socket), and those
        that indicate that the socket is still usable, and we wish to
        shut down tidily (some unhandled error condition.)

        Unfortunately, there is no way to reliably do this.
        Therefore, proceed on the assumption we are still viable the
        first time thru, and attempt to perform a shutdown on the
        socket.  On any subsequent call (or an exception), we'll do a
        hard close; and we'll schedule a handle_close for the future,
        to force cleanup, should the shutdown not flow an EOF through.
        """
        if self.shuttout and self.tidy_close():
            # We performed a tidy close, and can schedule a real close, in case
            # it doesn't flow through...
            self.schedule.append(
                (timer() + self.shuttout, self.handle_close, None))
        elif not self.closed:
            # Already did a shutdown, no connection timeouts, or no schedule to
            # handle them, or an exception while attempting to shut down socket
            # and schedule a future cleanup.  Just close.
            logging.info("%s closed connection %s" % (
                    self.name(), str(self.addr)))
            self.close()

    def handle_expt(self):
        """
        Exceptional conditions on sockets may mean out-of-band data,
        or errors.  If an error is detected, asyncore.dispatcher will
        fire handle_close instead.  Since we don't know how to handle
        other types of exceptional events, we'll treat this as a
        protocol failure, and trigger close.
        """
        self.handle_close()

    # ----------------------------------------------------------------------
    # Un-authenticated commands
    # 
    #     The un-authenticated portion of the protocol, used to
    # implement challenge-response authentication.  Note that
    # respond_to_challenge may actually be either an authenticated OR
    # an un-authenticated commadn (depending on who demanded auth
    # first).
    # 
    def send_challenge(self):
        logging.debug("%s -- send_challenge" % self.name())
        self.auth = os.urandom(20).encode("hex")
        self.send_command(":".join(["challenge", self.auth]))

    def respond_to_challenge(self, command, data, txn=None):
        logging.debug("%s -- respond_to_challenge" % self.name())
        mac = hmac.new(self.password, data, hashlib.sha1)
        self.send_command(":".join(["auth", mac.digest().encode("hex")]))
        self.post_auth_init()

    def verify_auth(self, command, data):
        logging.debug("%s -- verify_auth" % self.name())
        mac = hmac.new(self.password, self.auth, hashlib.sha1)
        if data == mac.digest().encode("hex"):
            self.auth = "Done"
            logging.info("%s Authenticated other end" % self.name())
            self.condition_changed()
        else:
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

    # ----------------------------------------------------------------------
    # Authenticated commands
    # 
    #      After both ends of the channel have authenticated eachother, they may
    # then use these commands.
    # 
    def ping(self, message=None, payload=None, now=None,
             backchannel=False):
        """
        Send a standard "ping", carrying the given payload, and a txn containing
        the current time.time().

        If invoked from a Thread other than the asyncore.loop process thread,
        use backchannel=...  The value passed will be used as the timeout (None,
        or a numeric timeout in seconds)
        """
        if now is None:
            now = time.time()
        if message is None:
            message = "from %s" % (socket.getfqdn())
        if payload is None:
            payload = self.name()

        if backchannel is False:
            self.send_command("ping", (message, payload),
                              txn="%.6f" % now)
        else:
            self.send_command_backchannel("ping", (message, payload),
                                          txn="%.6f" % now, timeout=backchannel)

    def ping_delay(self, txn, now=None):
        """
        Compute the round-trip delay time of a ping, from its 'txn',
        and the current time 'now'.  Returns a tuple with numeric and
        string display versions.
        """
        if now is None:
            now = time.time()
        delay = 0                       # Defaults to (0, "?"), if -'ve, or
        delaymsg = "?"                  #  if any exception prevents conversion
        try:
            delay = now - float(txn)
            if 0.0 <= delay < 0.01:
                delaymsg = "%.3f ms" % (delay * 1000)
            elif 0.0 <= delay:
                delaymsg = "%.3f s" % (delay)
        except:
            pass

        return (delay, delaymsg)

    def ping_payload(self, command, data, txn):
        """
        Decodes a standard "ping" (or "pong"), logs it, and return its
        payload.
        """
        try:
            message, payload = data
        except TypeError:
            message = data
            payload = None

        (delay, delaymsg) = self.ping_delay(txn)

        logging.info("%s %s %s(%s): txn=%s time=%s" % (
            self.name(), command, message, repr.repr(payload),
            txn, delaymsg))

        return payload

    def respond_to_ping(self, command, data, txn):
        """
        Ping handler.  By default, expectes a (message, payload) and
        just returns a new message with the same payload and txn.
        """
        message = "from %s" % socket.getfqdn()
        payload = self.ping_payload(command, data, txn)
        self.send_command("pong", (message, payload), txn=txn)

    def pong(self, command, data, txn):
        """
        Pong handler (response to a Ping).  Just logs it, and ignores
        the payload.
        """
        self.ping_payload(command, data, txn)

    def process_command(self, command, data=None, txn=None):
        """
        Process a command, if possible; returns True iff handled.
        """
        commands = {
            'ping': self.respond_to_ping,
            'pong': self.pong,
            'challenge': self.respond_to_challenge,
            'disconnect': lambda x, y: self.handle_close(),
            }

        if command in commands:
            commands[command](command, data, txn)
            return True

        if self.unrecognized_command(command, data, txn):
            return True

        self.failed_command(command, data, txn)
        return False

    def unrecognized_command(self, command, data=None, txn=None):
        """
        Override in derived classes, to do something with incoming
        commands unknown to Protocol or derived classes.  Default
        behavior is to deem such commands a fatal protocol failure,
        and kill the connection.

        Return True iff the command is handled.
        """
        return False

    def failed_command(self, command, data=None, txn=None):
        """
        Default behavior is to deem such commands a fatal protocol
        failure, and kill the connection.

        Return True iff the command is handled.
        """
        logging.critical("Unknown command received: %s (txn: %s)" % (
                command, txn))
        self.handle_close()        

    # ----------------------------------------------------------------------
    # function marshalling utilities
    # 
    #     After authentication, the protocol allows for functions and
    # simple closures to be marshalled and transmitted.  These methods
    # are used to dump and reconstitute methods.
    # 
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
                ["def _f(arg):"] +
                ["  _%d = arg[%d] "     % (n, n) for n in ncells] +
                ["  return lambda:(%s)" % ','.join("_%d" %n for n in ncells)] +
                [""])
            try:
                exec src
            except:
                raise SyntaxError(src)
            clos = _f(clos_tupl).func_closure

        return new.function(code, globs, name, dflt, clos)


class Client(Protocol, Mincemeat_class):
    """
    Implements a mincemeat Map/Reduce Client.  Connect's to a
    specified server:port, and processes commands (authentication is
    handled by the Protocol superclass).

    Also implements the Mincemeat_class interface, so it may be
    used as a target of Mincemeat_daeamon.
    """
    def __init__(self, map=None, schedule=None, shuttout=None):
        """
        Instantiate a Map/Reduce Client connection to a server.
        """
        Protocol.__init__(self, map=map, schedule=schedule, shuttout=shuttout)
        for attr in ['mapfn', 'collectfn', 'reducefn']:
            if not hasattr(self, attr):
                setattr(self, attr, None)
        self.name("Client")

    # ----------------------------------------------------------------------
    # Methods required by Mincemeat_daemon
    # 
    def conn(self, interface="", port=DEFAULT_PORT, password=None,
             asynchronous=False, **kwargs):
        """
        Establish Client connection, and (optionally) synchronously loop 'til
        all file descriptors closed.  Optionally specifies password.  Note that
        order is different than Server.run_server, for historical reasons.
        
        We iggnore additional unrecognized keyward args, so that identical
        packages of configuration may be used for both a Client.conn and a
        Server.conn call.
        
        If no server port exists to bind to, on Windows the select.select()
        call will return an "exceptional" condition on the socket; on *nix, a
        "readable" condition (and a handle_connect()), followed by an error on
        read (errno 11, "Resource temporarily unavailable") and a
        handle_close().  In either case, on the loopback interface, this occurs
        in ~1 second.

        Since this connection is performed asynchronously, the invoker may want
        to check that the client is authenticated (perhaps with a timeout)
        after this call, to ensure that we successfully connected to and
        authenticated a server...
        
        Since the default kernel socket behavior for interface == "" is
        inconsistent, we'll choose "localhost".
        """
        for k, v in kwargs.iteritems():
            logging.info("%s conn ignores %s = %s" % (
                    self.name(), k, v))
        if password is not None:
            self.password = password
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((interface or "localhost", port))
        if asynchronous is False:
            self.process()

    # ----------------------------------------------------------------------
    # Protocol command processing
    # 
    def set_mapfn(self, command, mapfn, txn):
        self.mapfn = self.load_func(mapfn, globals())

    def set_collectfn(self, command, collectfn, txn):
        self.collectfn = self.load_func(collectfn, globals())

    def set_reducefn(self, command, reducefn, txn):
        self.reducefn = self.load_func(reducefn, globals())

    def call_mapfn(self, command, data, txn):
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
        wrapped to produce a (key, [value]) tuple, as would be
        produced by the normal Map phase.  We use 'applyover' to
        handle either simple functions operating on each individual
        (key,[value,...])  item, or generators which operate over the
        sequence of all items (and hence may employ remembered state).
        """
        logging.info("%s Mapping %s" % (self.name(), repr.repr(data[0])))
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
            # (eg. discarding invalid keys, adding new keys).  Allow
            # collectfn to yield (k, [v]) or simple (k, v), but always
            # return keys mapped to lists.
            results = dict((k, v if isinstance(v, list) else [v]) for k, v in rgen)

        self.send_command('mapdone', (data[0], results), txn)

    def call_reducefn(self, command, data, txn=None):
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
        logging.info("%s Reducing %s" % (self.name(), repr.repr(data)))
        rgen = applyover(self.reducefn, [data])
        results = list(rgen)
        if len(results) != 1:
            raise IndexError
        self.send_command('reducedone', results[0], txn)
        
    def process_command(self, command, data=None, txn=None):
        commands = {
            'mapfn': self.set_mapfn,
            'collectfn': self.set_collectfn,
            'reducefn': self.set_reducefn,
            'map': self.call_mapfn,
            'reduce': self.call_reducefn,
            }

        if command in commands:
            commands[command](command, data, txn)
            return True

        return Protocol.process_command(self, command, data, txn)

    def post_auth_init(self):
        """
        After Client has been authenticated by the Server, we will
        initiate a challenge for authentication of the Server.
        """
        logging.debug("%s -- post_auth_init" % self.name())
        if not self.auth:
            self.send_challenge()


class Server(asyncore.dispatcher, Mincemeat_class):
    """
    Server -- Distributes Map/Reduce tasks to authenticated clients.
    
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

    Note that this must be a "new-style" class (forced by inheritance
    from Mincemeat_class).  This is required to support Properties
    (see datasource = ..., below)
    """

    def __init__(self, map=None, schedule=None, shuttout=None):
        """
        A mincemeat Map/Reduce Server.  Specify a 'map' dict, if other asyncore
        based facilities are implemented, and you wish to run this Server with a
        separate asyncore.loop.  Any ServerChannels created due to incoming
        Client request will also share this map.  Every Server, Client (or other
        asyncore object) which uses the same asyncore.loop thread must share the
        same map.

        The .schedule and .shuttout are (eventually) passed to each newly minted
        ServerChannel, to allow it to perform a tidy_close (if shuttout is None,
        they will simply close).  We can handle a map=None (the default, global
        asyncore.dispatcher map will be used); If shuttout is None, then
        ServerChannels will be taken down harshly at Server.handle_close,
        because ServerChannel.handle_close will simply invoke .close!

        For historical consistency, the TaskManager is set up with the default
        cycle, etc., which is TaskManager.SINGLEUSE by default -- shut down
        after datasource is empty.  After creation, if you change a Server 's'
        TaskManager defaults:
        
            s.taskmanager.defaults["cycle"] = TaskManager.PERMANENT

        and it will go IDLE awaiting the next datasource, instead of quitting.

        A number of configurations may be specified *either* in the derived
        Server class' attributes, *or* in the package of Server.conn's
        'credentials':

            *fn    -- all of the Map and Reduce phase functions
            cycle  -- the default TaskManager.cycle setting (None --> default)

        We ensure that these are all initialized to None, if not defined
        in a derived class.
        """
        if map is None:
            # Preserve pre-2.6 compatibility by avoiding map, iff None
            asyncore.dispatcher.__init__(self)
        else:
            asyncore.dispatcher.__init__(self, map=map)

        if schedule is None:
            schedule = collections.deque()
        self.schedule = schedule
        self.shuttout = shuttout

        self.taskmanager = TaskManager(self)
        self.password = None

        for attr in ['mapfn', 'collectfn', 'reducefn', 'finishfn', 'cycle']:
            if not hasattr(self, attr):
                setattr(self, attr, None)

        self.output = collections.deque()

    def resultfn(self, txn, results):
        """
        Take responsibilty for some Map/Reduce results.  By default, just
        collect up the results in the self.output deque.

        This may be overridden in a derived class:

            class My_Server(mincemeat.Server):
                def resultfn(txn, results):
                    ...

        or (as with the other configurable Server methods like .mapfn),
        redefined by a assignment:

            s = mincemeat.Server()
            s.resultfn = my_resultfn

        or by passing extra keyword args to 'conn':

            s.conn(..., resultfn=my_resultfn)

        NOTE: If you DO override this to handle results directly, you
        probably should look at your use of finished() and results(),
        to see if they make sense.  We use finished() to determine the
        successful completion of a Server or Client Mincemeat_daemon,
        and this usage will still work, because finished() returns
        True if the Server's TaskManager achieves FINISHED state (the
        final Map/Reduce Transaction was done).
        """
        self.output.append((txn, results))

    # ----------------------------------------------------------------------
    # Methods required by Mincemeat_daemon
    # 
    def finished(self):
        """
        Detect if finished.  If running in cycle == PERMANENT mode, this will
        never report "finished".
        """
        return self.taskmanager.state in [
            TaskManager.FINISHED,
            ]

    def results(self):
        """
        Successfully completed Map/Reduce.  Return results (discards
        the 'txn'; override or redefine resultfn if you need it).

        If you've overridden resultfn to handle results differently,
        this will probably just always return None.
        """
        if not bool(self.output):
            # No output produced.  Return nothing.
            return None
        txn, results = self.output.popleft()
        return results

    def authenticated(self):
        """
        Server has no authenticated phase; only its ServerChannel(s)
        do, and they don't report for duty 'til they are done
        authenticating.  Therefore, we always deem a Server to be
        authenticated.  This is one of the factors used by the
        Mincemeat_daemon in determining a successful completion state.
        """
        return True

    def name(self):
        """
        Our name is simply the bind address, which is saved (oddly) in
        addr; this means that asyncore.dispatcher treats self.addr as
        a remote peer interface address (when used with connect, and
        after creating a dispatcher following accept), but as a local
        interface address when used with bind/lishten.  It will be
        None 'til bind is invoked.
        """
        addr = self.addr and self.addr or (None,None)
        return "Server" + '@' + str(addr[0]) + ':' + str(addr[1])

    def conn(self, password="", port=DEFAULT_PORT, interface="",
             asynchronous=False, **kwargs):
        """
        Establish this Server, allowing Clients to connect to it.  This will
        allow exactly one Server bound to a specific port to accept incoming
        Client connections.

        Any additional keyword args are assumed to be (optional) values for
        Server attributes (eg. .datasource, .mapfn, .cycle, ...)  Any values
        passed are only used if they are non-None; in other words, a None value
        will not override a valid default value (such as for resultfn, where
        the default behavior saves the results for later access!

        If asynchronous, then we will not initiate processing; it is the
        caller's responsibility to do so; every Client and/or Server with a
        shared map=... require only one asyncore.loop(map=obj._map) or
        obj.process() thread.

        The default behaviour of bind when interface == '' is pretty
        consistently to bind to all available interfaces.

        We'll set self.addr early (it is also set in asyncore.bind), for
        logging purposes.
        """
        addr = (interface, port)
        self.addr = addr

        for k, v in kwargs.iteritems():
            if v is not None and hasattr(self,k):
                if getattr(self,k) is None:
                    logging.info("%s conn setting %s = %s" % (self.name(), k, v))
                else:
                    logging.info("%s conn setting %s = %s (was %s)" % (
                            self.name(), k, v, getattr(self, k)))
                setattr(self, k, v)
            else:
                logging.debug("%s conn ignores %s = %s" % (self.name(), k, v))
        self.password = password
        
        logging.debug("%s listening port binding..." % self.name())
        try:
            self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
            if hasattr(socket, 'SO_EXCLUSIVEADDRUSE'):
                # Windows socket re-use semantics differ from *nix (read:
                # are broken).  See http://goo.gl/J89cr
                self.socket.setsockopt(socket.SOL_SOCKET,
                                        socket.SO_EXCLUSIVEADDRUSE, 1)
            else:
                # Posix platform.  See http://goo.gl/LBqSS,
                # http://goo.gl/lzD8Q.  We want to set SO_REUSEADDR,
                # to allow multiple servers to bind() (even if the
                # address endpoint has sockets still in the TIME_WAIT
                # state.)  This will NOT allow multiple parties to
                # simultaneously bind() and listen() for incoming
                # TCP/IP connections, if the address is in the Unicast
                # range (Multicast addresses would allow it (sometimes
                # also requiring SO_REUSEPORT), but are not typically
                # used for TCP/IP protocols.)
                self.set_reuse_addr()
            self.bind(addr)
            self.listen(5)
        except Exception, e:
            # If anything fails during socket creation, we need to
            # ensure we clean up the partially opened socket;
            # otherwise, it'll leave a busted entry in
            # asyncore.socket_map, which will prevent asyncore.loop()
            # from working correctly.
            logging.warning("%s couldn't bind to %s: %s" % (
                    self.name(), str((interface, port)),
                    e ))
            self.close()
            raise

        # If either the Map or Reduce functions are empty, direct the
        # TaskManager to skip that phase.  Since Server.reducefn and
        # .mapfn are not set at Server.__init__ time, we must defer
        # 'til .conn() is invoked, to detect if these are provided.
        # 
        # After this point, any Client that connects will be issued
        # whatever Server.mapfn, .collectfn and .reducefn are
        # currently defined, so this setting cannot really be changed.
        if self.reducefn is None:
            self.taskmanager.tasks = TaskManager.MAPONLY
        elif self.mapfn is None:
            self.taskmanager.tasks = TaskManager.REDUCEONLY

        # The 'credentials' package, or the a Server derived class may change
        # this default.
        if self.cycle is not None:
            self.taskmanager.defaults["cycle"] = self.cycle

        if asynchronous is False:
            self.process()

    # ----------------------------------------------------------------------
    # overridden asyncore.dispatcher methods
    # 
    def log_info(self, message, type='info'):
        getattr(logging, type)("%s %s" % (self.name(), message))

    def handle_accept(self):
        """
        Accept a new client connection, spawing a channel to handle
        it.  This initiates the authentication procedure, and should
        (eventually) result in the channel asking for tasks from this
        server's taskmanager.  We'll use the TaskManager to monitor
        the pool of available channels.  Ensure we handle accept()
        error cases (see http://bugs.python.org/issue6706)
        
        We simply ignore incomplete connection attempts; logging would
        present an increased DOS attack opportunity.  It may make
        sense to minimize or eliminate logging 'til authentication of
        client is complete, if used in an internet-facing application.
        """
        try:
            sock, addr = self.accept()
        except TypeError:
            # sometimes accept() might return None (see issue 91)
            # In Python 2.7+, asyncore.dispatcher.accept is improved,
            # but may still return just None.
            return
        except socket.error, e:
            # ECONNABORTED might be thrown on *BSD (see issue 105). 
            # In Python 2.7+, this is handled (returns None).
            if e[0] != errno.ECONNABORTED:
                logging.warning("Unhandled socket error on accept: %s\n%s" % (
                    e, traceback.format_exc()))
            return
        else:
            # sometimes addr == None instead of (ip, port) (see issue 104)
            if addr == None:
                return

        # Got a valid socket and address; spawn a ServerChannel to handle.
        sc = ServerChannel(sock, addr, self)
        sc.password = self.password

    def handle_close(self):
        """
        EOF (or other failure) on our socket (or just detected completion, or
        Mincemeat_daemon.stop has been invoked).  We have a chance to tidy up
        nicely.

        Arrange to send an EOF on all current clients by using
        socket.shutdown(SHUT_WR) to close the outbound half of all ServerChannel
        file descriptors.  This will cause them to finish up their current
        command, send the result, receive EOF, and close nicely.  The Protocol
        will handle establishing deferred closes, if EOF doesn't flow through.

        For channels that are not yet set up, we need to ensure that they get
        shut down when they report for their first TaskManager.next_task
        """
        logging.debug("%s listening port closing." % self.name())
        try:
            self.close()
        except Exception, e:
            logging.warning("%s closing main port failed: %s" % (
                    self.name(), e))
        for chan in self.taskmanager.channels.keys():
            try:
                chan.handle_close()
            except Exception, e:
                logging.warning("%s closing %s failed: %s" % (
                        self.name(), chan.name(), e))

    def handle_expt(self):
        """
        Consider an exceptional condition on our main socket a
        protocol failure.
        """
        self.handle_close()

    # ----------------------------------------------------------------------
    # mincemeatpy traditional interface
    # 
    #     We remain backward-compatibile with the original
    # mincemeat.py.  We've split out the connection related
    # functionality into the conn() method required by
    # Mincemeat_daemon, but retain the original API.
    # 
    def run_server(self, password="", port=DEFAULT_PORT, interface="",
                   asynchronous=False):
        """
        Runs the Server, by default synchronously, returning the
        results when done.

        Use this method in the default asynchronous == False form, if
        and only if this the only asyncore based object running in
        this Python instance (or you wish to run all other
        asyncore-based services with out asyncore.loop thread)!  This
        is because the default constructor uses the global asyncore
        socket map, shared with all other asyncore objects.

        Otherwise, use the component methods (ensure that the caller
        runs asyncore.loop in another thread.)

            s = mincemeat.Server()
            s.datasource = ...
            s.mapfn = ...
            s.collectfn = ...
            s.reducefn = ...
            s.finishfn = ...
            
            s.conn(**credentials)
        
            ac = threading.Thread(target=s.process)
            ac.start()
            while not s.finished():
                ac.join(.1)
            ac.join()

            results = s.results()

        See the example*.py files for samples of various usages.
        """
        self.conn(password=password, port=port, interface=interface,
                  asynchronous=asynchronous)

        # If we are asynchronous, we have NOT initiated processing;
        # This will return None, if not finished(), or the results if
        # we are were not asynchronous.
        return self.results()

    def tickle(self):
        """
        Tickle an IDLE Server awake.

        Must be invoked from within the Server's asyncore.loop thread.
        """
        if ( self.taskmanager.deque
             and self.taskmanager.state == TaskManager.IDLE
             and self.taskmanager.channels ):
            self.taskmanager.jump_start(count=1)

    def set_datasource(self, datasource=None, **kwargs):
        """
        Sets the TaskManager datasource, which is expected to be a dict-like
        object, implementing at least the iterator protocol (.__iter__()
        returning something with .__iter__() and .next()), and the sequence
        protocol (at least the .__getitem__() method; .__len__() is not used).
        
        Set (or enqueue) a Map/Reduce Transaction, starting the TaskManager if
        it is idle.  The first positional parameter is presumed to be datasource
        (eg. when invoked via the self.datasource property); any other keyword
        args are presumed to be configuration parameters for the TaskManager.

        This method (when called directly) allows specifying other TaskManager
        parameters, such as cycle=TaskManager.PERMANENT (the default is
        TaskManager.SINGLEUSE, for historical compatibility -- The TaskManager
        will shut down the Server after the Map/Reduce Transaction is complete).

        We update the kwargs with the datasource (which is required to be the
        first positional parameter), and put the the bundle on deque.

        The temptation might be to jump-start the TaskManager's idle channels.
        However, this method may easily be invoked by threads other than the
        asyncore thread!  So, how do we ensure we awaken if a non-I/O thread
        sets a datasource, and the TaskManager is completely idle, and there is
        no I/O activity?  How can we ensure that at least one idle ServerChannel
        awakens and invokes start_new_task?

        The truth is, we cannot deduce whether we can safely do this: we don't
        know what thread may be running the asyncore.loop for the Server (and
        all its ServerChannels).  Historically, invoking mincemeatpy's
        Server.set_datasource never started processing; it only prepared the
        TaskManager for the future arrival of new ServerChannels, which
        (ultimately) would invoke .start_new_task at the end of their
        Authentication process, from .post_auth_init.

        Therefore, it must be left to the implementation to ensure that that
        asyncore.loop thread will check, and if the TaskManager is IDLE and
        there are now datasources in TaskManager.deque, that we jump_start a
        ServerChannel.  TaskManager.next_task will then (safely) fire up all
        remaining idle channels as it transitions from the IDLE to START state.

        The simplest method is to ensure that Server.set_datasource is invoked
        from within the asyncore.loop thread (eg. from within a *_command or
        other callback), and that we invoke Server.tickle.

        If an external thread does invoke set_datasource, then the application
        truly requires an asynchronous means to awaken the asyncore.loop thread
        stuck in select.  Unfortunately, this is pretty trivial on Posix
        platforms (using os.pipe), but a significant problem for Windows:

            http://svn.zope.org/zc.ngi/trunk/src/zc/ngi/async.py?rev=69400&view=markup

        So, we'll do our best: we'll schedule an immediate self.tickle, which
        will be run as soon as the next I/O event occurs on any Server or
        ServerChannel file descriptor.  This will be 100% certain to ignite the
        Transaction, when set_datasource is called from within an asyncore.loop
        callback, and will ignite processes as soon as possible when called from
        any other context.
        """
        kwargs.update({'datasource': datasource})
        self.taskmanager.deque.append(kwargs)
        logging.info("%s setting datasource (now %d): %s" % (
                self.name(), len(self.taskmanager.deque), repr.repr(kwargs)))
        self.schedule.append( (0.0, self.tickle, None) )

    def get_datasource(self):
        """
        Returns the TaskManager's currently active datasource.  This
        may change asynchronously, as the TaskManager completes
        Map/Reduce transactions, and pulls new data sources off the
        deque.
        """
        return self.taskmanager.datasource

    datasource = property(get_datasource, set_datasource)

    # ----------------------------------------------------------------------
    # Protocol command processing (hooks invoked by our ServerChannels)
    # 
    def unrecognized_command(self, command, data=None, txn=None, chan=None):
        """
        Process a command, if possible, from the specified
        ServerChannel; returns True iff handled.  We will get all
        unrecognized commands received by our ServerChannels.  If we
        don't recognize them, we just return False to elicit the
        default behavior (protocol failure; close channel.)
        """
        return False

    def ping(self, message=None, payload=None, now=None,
             backchannel=False):
        """
        A Server has multiple ServerChannels to its Clients.  Ping them all.
        """
        for chan in self.taskmanager.channels.keys():
            chan.ping(message=message, payload=chan.name(), now=now,
                      backchannel=backchannel)

    def pong(self, chan, command, data, txn):
        """
        Invoked on every pong received by any of our ServerChannels.
        Override to do something interesting with the Client pong
        responses (eg. heartbeat health monitoring.)
        """
        pass


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
    C)   getting one from the TaskManager.start_new_task (going idle if None)
    D)   sending it to the client
    E) When a 'mapdone' or 'reducedone' is received, go to B
    F) If EOF encountered, close session

    If a channel goes idle, invoke channel.start_new_task() to start
    it up, by force it to go get a new task.

    Normally, Map/Reduce Task commands flow from the Server's
    TaskManager by the process above.  However, arbitrary commands may
    flow in the opposite direction: from the Client, to the
    ServerChannel (handled by overridden process_command or
    unrecognized command methods).  Similarly, other commands may be
    sent (from the Server, for example) via our send_command and
    send_command_backchannel methods.
    """
    def __init__(self, sock, addr, server):
        # We need to use the same asyncore _map as the server.
        Protocol.__init__(self, sock=sock, map=server._map,
                          schedule=server.schedule, shuttout=server.shuttout)
        self.server = server
        self.name("SvrChn")
        self.start_auth()

    def handle_close(self):
        """
        We need to help the Server's TaskManager keep track of client
        activity state.
        """
        self.server.taskmanager.channel_closed(self)
        Protocol.handle_close(self)

    def start_auth(self):
        logging.debug("%s -- start_auth" % self.name())
        self.send_challenge()


    # ----------------------------------------------------------------------
    # Protocol command processing (hooks invoked by our ServerChannels)
    # 
    # We use the ...taskmanager.channel_...() methods to maintain channel state
    # in the taskmanager, ONLY for Map/Reduce task commands we issue and the
    # corresponding responses we receive.  Any other commands sent/received via
    # the ServerChannel do not affect the taskmanager state.  This allows the
    # taskmanager to use its .channels state to determine how the ServerChannel
    # needs to be prodded, as a Map/Reduce Transaction proceeds, or channels
    # appear/disappear.
    # 
    # When the TaskManager decides that it is done, ServerChannels reporting for
    # duty will be issued a Protocol 'disconnect' command, which will cause them
    # to perform a handle_close.  After that is done (once), we'll detect a
    # .shutdown or .closed, and do nothing (taskmanager state already changed).
    # 
    # We don't want to try to deduce whether or not this channel should be
    # closing, here -- we leave it up to the higher-level TaskManager, Server,
    # etc., to decide these things.  Otherwise, we'll get into trouble with
    # multiple parties trying to maintaining TaskManager's state (eg. via
    # jump_start.)
    # 
    # If we've been shutdown or closed, either before or after during the call
    # to next_task, we can just do nothing (the channel state will have been
    # maintained). 
    def start_new_task(self):
        if self.shutdown or self.closed:
            return
        command, data, txn = self.server.taskmanager.next_task(self)
        if command == None:
            self.server.taskmanager.channel_idle(self)
            return
        if self.shutdown or self.closed:
            return
        self.server.taskmanager.channel_sending(self, command, data, txn)
        self.send_command(command, data, txn)

    def map_done(self, command, data, txn):
        self.server.taskmanager.channel_process(self, command, data, txn)
        self.server.taskmanager.map_done(data, txn)
        self.start_new_task()

    def reduce_done(self, command, data, txn):
        self.server.taskmanager.channel_process(self, command, data, txn)
        self.server.taskmanager.reduce_done(data, txn)
        self.start_new_task()

    def process_command(self, command, data=None, txn=None):
        commands = {
            'mapdone': self.map_done,
            'reducedone': self.reduce_done,
            }

        if command in commands:
            commands[command](command, data, txn)
            return True

        return Protocol.process_command(self, command, data, txn)

    def pong(self, command, data, txn):
        """
        Override Protocol.pong to report in with our Server, to
        support implementation of custom ServerChannel health
        monitoring.
        """
        self.server.pong(self, command, data, txn)
        Protocol.pong(self, command, data, txn)

    def unrecognized_command(self, command, data=None, txn=None):
        """
        Direct all other unknown incoming commands to the Server.  If
        it doesn't like 'em, we're toast; consider it a protocol
        failure (by invoking original base class method).
        """
        if self.server.unrecognized_command(command, data, txn, chan=self):
            return True

        return Protocol.unrecognized_command(self, command, data, txn)

    def post_auth_init(self):
        """
        After we have authenticated the Client, we expect it to
        challenge the Server for authentication.  Once we have
        responded (Protocol.respond_to_challenge has been invoked, and
        the challenge response has been sent), it will invoke
        post_auth_init, taking us here.

        We assume that the Client is going to like our response, so we
        can proceed to transmit configuration to the client.
        """
        logging.debug("%s -- post_auth_init" % self.name())
        if self.server.mapfn:
            self.send_command('mapfn', self.store_func(self.server.mapfn))
        if self.server.reducefn:
            self.send_command('reducefn', self.store_func(self.server.reducefn))
        if self.server.collectfn:
            self.send_command('collectfn', self.store_func(self.server.collectfn))
        self.server.taskmanager.channel_opened(self)
        self.start_new_task()


class TaskManager(object):
    """
    Produce a stream of Map/Reduce tasks for all requesting
    ServerChannel channels.  

    Normally, the default TaskManager .tasks is MAPREDUCE, and
    .allocation to CONTINUOUS, meaning that each channel will receive
    a continous stream of all available 'map' tasks, followed by all
    available 'reduce' tasks.
    
    After all available 'map' tasks have been assigned to a client,
    any 'map' tasks not yet reported as complete will be
    (duplicately!) re-assigned to the next client channel that asks,
    limited by .retransmit percentage.  This takes care of stalled or
    failed clients.

    When all 'map' tasks have been reported as completed (any
    duplicate responses are ignored), then the 'reduce' tasks are
    assigned to the subsequent next_task-calling channels.

    Finally, once all Map/Reduce tasks are completed, the resultfn is
    fired, and the clients are given the 'disconnect' task and the
    Server is shut down, if .cycle is SINGLEUSE; if PERMANENT, the
    TaskManager goes back to IDLE state, awaiting a new transaction.
    """
    # Possible .state
    IDLE        = 0             # Awaiting a datasource
    START       = 1             # Ready to start; prep iterator, decide phase
    MAPPING     = 2             # Performing Map phase of task
    REDUCING    = 3             # Performing Reduce phase of task
    FINISHING   = 4             # Performing Finish phase, preparing results
    FINISHED    = 5             # Final results available; loop or quit

    statename = {
        IDLE:           "Idle",
        START:          "Starting",
        MAPPING:        "Mapping",
        REDUCING:       "Reducing",
        FINISHING:      "Finishing",
        FINISHED:       "Finished",
        }
    statecommand = {
        MAPPING:        "map",
        REDUCING:       "reduce",
        }
    
    # Possible .tasks option
    MAPREDUCE   = 0
    MAPONLY     = 1             # Only perform the Map phase
    REDUCEONLY  = 2             # Only perform the Reduce phase
    tasksname = {
        MAPREDUCE:      "Map/Reduce",
        MAPONLY:        "Skip Reduce",
        REDUCEONLY:     "Skip Map",
        }

    # Possible .allocation option
    CONTINUOUS  = 0             # Continuously allocate tasks to every channel
    ONESHOT     = 1             # Only allocate a single Map/Reduce task to each

    # Possible .cycle options
    SINGLEUSE   = 0             # After finishing, close Server and 'disconnect' clients
    PERMANENT   = 1             # Go idle 'til another Map/Reduce transaction starts

    def __init__(self, server,
                 tasks=None, allocation=None, cycle=None):

        # The datasources, etc. configurations on deque ;) Contains dicts
        # configuring:
        # 
        #   .datasource -- A dict-like name/corpus source
        #   .txn        -- A transaction ID to use (None default)
        #   .allocation -- ONESHOT or CONTINUOUS (default)
        #   .cycle      -- PERMANENT or SINGLEUSE (default)
        #   .retransmit -- % retransmission factor (when extra channels available)
        # 
        # Any entries not configured (or containing None) are set to the value
        # in self.defaults, remembered from the args provided.

        self.deque = collections.deque()        # {'attr': value, ...}, ...
        self.defaults = {
            "datasource":       None,           # If None, TaskManager will idle
            "txn":              None,           # Currently running Transaction
            "allocation":       allocation or self.CONTINUOUS,
            "cycle":            cycle      or self.SINGLEUSE,
            "retransmit":       5,              # Retransmit re-work to 5% of channels
            }

        self.server = server
        self.state = self.IDLE
        self.tasks = tasks or self.MAPREDUCE

        # Track what channels exist, and were last reported as being up to
        self.channels = {}

        # Finally, initialize remaining attrs to the defaults.  Change the
        # .default["attr"] to permanently alter the default behaviour after
        # TaskManager creation, but ideally before the first invocation of
        # next_task.
        for attr, value in self.defaults.iteritems():
            setattr(self, attr, value)

    # 
    # channel_... -- maintain client .channels activity state
    # 
    #     Tracks the command, response and time started for every
    # request.  If idle, the entry is None (hit it with a
    # .start_new_task() to get it going again)
    # 
    #     .channels = {
    #         ('127.0.0.1, 12345): (txn, 'map', 'mapdone', 1234.5678 ),
    #         ('127.0.0.1, 23456): (txn, 'map', None, 1235.6789 ),
    #         ...
    #     }
    # 
    #     Used by .next_task() to track available channel state,
    # especially for ONESHOT .allocation mode.  When a channel is
    # closed (a completely asynchronous event), we must give
    # .next_task() an opportunity to awaken and change state, because
    # there may be no other non-idle channels with events outstanding
    # to ever wake it up...  
    # 
    def channel_opened(self, chan):
        self.channel_log(chan, "Opened")
        self.channels[chan] = None

    def channel_closed(self, chan):
        """
        A channel has been closed; perhaps not for the first time (due
        to tidy shutdown).  If this is the first report, remove it
        from the active channels, and jump-start the TaskManager state
        machinery, so that things don't go completely idle, if this
        was the last channel w/activity being waited for...
        """
        if chan in self.channels:
            self.channel_log(chan, "Closed")
            self.channels.pop(chan, None)
            self.jump_start(count=1)

    def channel_idle(self, chan):
        self.channel_log(chan, "Idle")
        self.channels[chan] = None

    def channel_sending(self, chan, command, data, txn):
        if self.channels.get(chan, None):
            self.channel_log(chan, "Done", how="debug")
        self.channels[chan] = (txn, command, None, time.time())
        self.channel_log(chan, "Sending", 
                         detail=repr.repr(data) if data is not None else "")

    def channel_process(self, chan, response, data, txn):
        """
        Update what we think a channel is processing.  Even though responses may
        be arbitrarily delayed, we should not have "moved on" to another txn
        until we got a response from the last one...  However, we can't
        guarantee that some implementation hasn't implemented some custom
        timeout code that throws a channel back into the pool...  

        So, if the channel entry doesn't exist, we'll still log, but be careful
        to NOT update the channels entry; we don't want to be resurrecting
        channels that have already been closed.

        If we find a mismatched txn, we'll update the channels entry and log it;
        the payload will be rejected in TaskManager.*_done due to the
        mismatching txn.
        """
        what = "Processing"
        try:
            cmdtxn, command, __, started = self.channels[chan]
            if txn != cmdtxn:
                what = "Mismatched"
            self.channels[chan] = (txn, command, response, started)
        except:
            what = "Spontaneous"
            
        self.channel_log(chan, what,
                         detail=repr.repr(data) if data is not None else "")

    def channel_log(self, chan, what, detail=None, how="info"):
        if chan is None:
            # No chan; Just print header
            getattr(logging, how)("%-25s Time    State      Txn  Response  Command" % (
                    "Channel"))
            return
        tpl = self.channels.get(chan, None)
        if tpl is None:
            getattr(logging, how)('%-25s %6.3fs %-10s %-4s<%-10s>%-10s%s' % (
                    chan.name(), 0.0, what,
                    '', '', '', ''))
        else:
            txn, command, response, when = tpl
            getattr(logging, how)('%-25s %6.3fs %-10s %-4s<%-10s>%-10s%s' % (
                    chan.name(), time.time() - when, what,
                    txn, response and response or "", command,
                    detail and detail or ""))
                                  
    def jump_start(self, count=None, avoid=None):
        """
        Any ServerChannels that have gone idle must be manually awakened,
        whenever there may be a new Transaction tasks to process.  Since this
        invokes methods that modify self.channels (the dict we're iterating over
        the items of), iterate over a copy (probably OK in Python 2.x, but
        dict.items() is destined to return an iterator in the future...)

        Optionally limit the number jump-started to count.  Also, avoid the
        specified channel.
        """
        for chan, activity in list(self.channels.items()):
            if chan is avoid:
                continue
            if activity is None:
                logging.debug("%s jump-starting %s" % (
                        self.server.name(), chan.name()))
                chan.start_new_task()
                if count:
                    count -= 1
                    if 0 == count:
                        break

    def next_task(self, channel):
        """
        Invoked by ServerChannels after they complete their current
        Task (or after being awakened), to obtain a new one.  Also, by
        channel_closed (to wake us up and perhaps transition states,
        if it was the last channel with pending activity!)

        When TaskManager is IDLE, the first one in will check if new
        Transactions are available on deque, and get things going.
        """
        if self.state == self.IDLE:
            # If nothing (or None) is left on the deque, set us back
            # to defaults.  This will put us back into idle mode.  It
            # will also cause any outstanding Map/Reduce tasks for the
            # present transaction to fall on deaf ears, when they
            # arrive ('cause we're about to set our self.txn to the
            # next one on deque, or None)
            try:
                transaction = self.deque.popleft()
            except:
                transaction = None
            if transaction is None:
                transaction = {}

            for attr in self.defaults.keys():
                value = transaction.pop(attr, self.defaults[attr])
                logging.debug("%s TaskManager.%-10s = %s %s" % (
                    self.server.name(), attr, value,
                    value == self.defaults[attr] and "(default)" or ""))
                setattr(self, attr, value)
            if transaction:
                logging.warning("%s unrecognized TaskManager configuration: %s " % (
                    self.server.name(), repr.repr(transaction)))

            # Tidy up residual state from prior Map/Reduce runs (may be large).
            self.items = None
            self.been = None
            self.done = None
            self.work = None
            self.results = None

            if channel is not None and self.datasource is not None:
                # Got a datasource; may be empty (we want to return empty
                # results), but isn't None; Fire it up.  
                self.state = self.START
            elif self.cycle == self.SINGLEUSE:
                # In the default mode, once the datasource is complete, we're done.
                self.state = self.FINISHED
            else:
                # PERMANENT, but no datasource.  Issue any clients an idle task.
                return (None, None, None)

        if self.state == self.START:
            # Start of Map/Reduce.  Generate a stream of key, value
            # pairs, from any object that implements only iterator
            # protocol, and the __getattr__ method of the sequence
            # protocol.
            self.items = ((k, self.datasource[k]) for k in iter(self.datasource))
            self.been = set()
            self.done = set()
            self.work = {}
            self.results = {}
            self.state = self.MAPPING
            if self.tasks is self.REDUCEONLY:
                # If Reduce only, skip the Map phase, passing datasource
                # key/value pairs straight to the Reduce phase.
                self.state = self.REDUCING
            # Jump start all the other channels, falling thru to get a task for
            # this channel, too.
            self.jump_start(avoid=channel)
            logging.debug("%s %s (%s)" % (
                    self.server.name(), self.statename[self.state],
                    self.tasksname[self.tasks]))

        if self.state in (self.MAPPING, self.REDUCING):
            # For ONESHOT allocation, we remember in-progress Map/Reduce tasks
            # by channel, too; as soon as each live channel has .been here, and
            # .done that task, we are done (any remaining tasks on the map_iter
            # are ignored; it may indeed be a non-terminating iterator in this
            # mode)!  Any new live channels showing up during the process will
            # get a chance to get work.
            #
            # We need to be careful to be sensitive to a channel disappearing;
            # if all other channels are done their assigned task and are idle,
            # and a channel dies and is closed, we could hang; we need to
            # jump-start one of the other channels (if any) and fire its
            # start_new_task to get the thread back in there to advance our
            # TaskManager state, or (if the dead channel is the one one) we must
            # advance to the next state, with NO (ie. empty) Map results...
            # This is done via channel_closed and jump_start.
            try:
                if self.allocation == self.ONESHOT:
                    # ONESHOT.  Assign one task to each channel as it shows up.
                    # We never assign re-work in this .allocation mode; just
                    # idle incoming channels 'til they've all completed (or
                    # died.)
                    if channel in self.been:
                        # OK, we've already seen this channel. Go check
                        # if we're Done...
                        raise StopIteration

                    # This channel hasn't .been yet; remember we gave it work.
                    self.been.add(channel)
                else:
                    # CONTINUOUS.  Assign tasks to all available channels 'til
                    # we run out of work (map_iter empty).  Then, retransmit
                    # work only when we are about to fall below our .retransmit
                    # factor of non-idle channels.
                    pass

                # This channel needs a Map/Reduce task item.  Get one (if
                # available); will raise StopIteration continually after .items
                # runs out of tasks, for every channel attempting to get work.
                item = self.items.next()
                self.work[item[0]] = item[1]
                logging.debug("%s %s %s work" % (
                        self.server.name(), self.statename[self.state],
                        channel.name()))
                return (self.statecommand[self.state], item, self.txn)

            except StopIteration:
                # A complete CONTINUOUS iteration of Map/Reduce task .items is
                # done, or we've given this channel ONESHOT of work (or ran
                # out); Since we can allow clients to go idle, we must remember
                # to wake them up when we transition to the Reduce phase!  Check
                # if we've completed the phase.
                if self.allocation is self.ONESHOT:
                    # ONESHOT.  Last one here, shut out the lights...  We come
                    # thru here for every channel that shows up after it's .been
                    # for work; remeber it's .done.  We may have dead (closed)
                    # channels in .been and/or .done; also, any fresh live
                    # channels will go thru (above) and get work.  If there are
                    # no longer any live channels in the set difference between
                    # the .been and .done, we are finished.
                    self.done.add(channel)
                    busy = self.been.difference(self.done) \
                                    .intersection(self.channels.keys())
                    if busy:
                        # OK, this channel has completed one shot of work, but
                        # we are awaiting others; just idle it; the thread will
                        # come back when another channel finishes, or dies.
                        logging.debug("%s %s %s idling (ONESHOT) (%d busy)" % (
                                self.server.name(), self.statename[self.state],
                                channel.name(), len(busy)))
                        return (None, None, None)

                    # We've all .been there, .done that; Done!
                    logging.debug("%s %s %s complete (ONESHOT)" % (
                            self.server.name(), self.statename[self.state], 
                            channel.name()))
                else:
                    # CONTINUOUS.  Pick a random item of those .working (not yet
                    # complete) to re-do, or let the client go idle.
                    if len(self.work) > 0:
                        # Channels still working...  Do we want/need to schedule
                        # some re-work?  Say we have N channels, and a %
                        # .retransmit; always round up (in other words, if any
                        # work outstanding, and anything other than 0%
                        # .retransmit, we'll issue at least one channel of
                        # rework):
                        # 
                        #      N    %               # Chans. Rework:
                        #     --   --               __
                        #     21 *  5 + 99 / 100
                        #              204 / 100 == 2
                        #     13 *  5 + 99 / 100
                        #              164 / 100 == 1
                        #      1 *  5 + 99 / 100
                        #              104 / 100 == 1
                        if (len(self.work)
                             <= ((len(self.channels) * self.retransmit + 99)
                                / 100)):
                            key = random.choice(self.work.keys())
                            logging.debug("%s Mapping %s rework" % (
                                    self.server.name(), channel.name()))
                            return (self.statecommand[self.state],
                                    (key, self.work[key]), self.txn)
                        # Busy, but no more rework required.  Idle it.
                        logging.debug("%s Mapping %s idling (CONTINUOUS)" % (
                                self.server.name(), channel.name()))
                        return (None, None, None)
                    # All .working Done!

                if self.state == self.MAPPING:
                    # MAPPING done; begin Reduce (or straight to FINISHING)
                    if self.tasks is self.MAPONLY:
                        # Skip Reduce phase, passing the key/value pairs
                        # output by the Map straight to the result.
                        self.state = self.FINISHING
                        logging.debug("%s Finishing (skipping Reduce)" % (
                                self.server.name()))
                    else:
                        # Mapping done; start Reducing.  Restart all idle
                        # channels (this is ultimately a recursive call;
                        # however, we've already advanced .state to REDUCING, so
                        # we'll be the only thread of control that ends up
                        # here...
                        logging.debug("%s Reducing" % (self.server.name()))
                        self.state = self.REDUCING
                        self.items = self.results.iteritems()
                        self.work = {}
                        self.results = {}
                        self.jump_start(avoid=channel)
                        # Fall through and Loop to REDUCING state.  We just jump
                        # started all the other channels; we'll be falling thru
                        # and getting a task for this channel, too.
                else:
                    # REDUCING done
                    self.state = self.FINISHING

        if self.state == self.FINISHING:
            # Map/Reduce done.  If .finishfn supplied, support
            # either .finishfn(iterator), or .finishfn(key,values),
            # apply it -- the resultant values are assumed to be
            # finished results, and are NOT encapsulated as a list.
            if self.server.finishfn:
                # Create a finished result dictionary by applying the
                # supplied Server.finishfn over the Reduce results.  
                self.results \
                    = dict(applyover(self.server.finishfn,
                                     self.results.iteritems()))

            # All done; results ready.  Invoke (optionally overridden or
            # redefined) .resultfn with results.
            logging.info("%s Results: %s" % (
                    self.server.name(), repr.repr(self.results)))
            self.server.resultfn(self.txn, self.results)
            self.state = self.FINISHED

        if self.state == self.FINISHED:
            # If not PERMANENT, stop accepting new Client connections, and send
            # a 'disconnect' to each client that asks for a new task.
            if self.cycle == self.PERMANENT:
                self.state = self.IDLE
                # Fall through and Loop to IDLE state
            else:
                # SINGLEUSE.  Done.  Kill server, disconnect any channels coming
                # for tasks.  The default implementation of Server.handle_close
                # also closes all the ServerChannels, but send a redundant
                # 'disconnect', too, in case some derived class changes that...
                self.server.handle_close()
                return ('disconnect', None, self.txn)

        # When making some state transitions (those involving cycling back to a
        # lower-numbered state), we'll need to loop to determine the work unit
        # to assign to this channel.
        logging.debug("%s %s %s looping..." % (
                self.server.name(), self.statename[self.state], channel.name()))
        return self.next_task(channel)

    def map_done(self, data, txn):
        """
        Absorb Map task responses.
        
        Don't use the results if they've already been counted, or if
        they don't belong to this transaction (eg. the Transaction is
        done, self.txn is None, and some (duplicate) stragglers have
        yet to arrive)!

        Since clients may exist beyond the scope of one transaction,
        and may be blocked or delayed nondeterministically, we can't
        assume that every Map or Reduce response was associated with
        this Transaction.
        """
        if txn != self.txn:
            logging.debug("%s Map result Task txn mismatch: %s != %s; Ignoring." % (
                    self.server.name(), txn, self.txn ))
            return
        if self.state != self.MAPPING or data[0] not in self.work:
            logging.debug("%s Map result Task duplicate/late; Ignorning: %s" % (
                    self.server.name(), repr.repr(data[0])))
            return
        logging.debug("Map Done: %s ==> %s" % (
                repr.repr(data[0]), repr.repr(data[1])))
        for (key, values) in data[1].iteritems():
            if key not in self.results:
                self.results[key] = []
            self.results[key].extend(values)
        del self.work[data[0]]
                                
    def reduce_done(self, data, txn):
        """
        Absorb Reduce task responses.
        """
        if txn != self.txn:
            logging.info("%s Reduce result Task txn mismatch: %s != %s; Ignoring" % (
                    self.server.name(), txn, self.txn ))
            return
        if self.state != self.REDUCING or data[0] not in self.work:
            logging.debug("%s Reduce result Task duplicate/late; Ignoring: %s" % (
                    self.server.name(), repr.repr(data[0])))
            return
        logging.debug("Reduce Done: %s ==> %s" % (
                repr.repr(data[0]), repr.repr(data[1])))
        self.results[data[0]] = data[1]
        del self.work[data[0]]


# ##########################################################################
# 
# Client_daemon         -- Standard Client daemon Thread
# Server_daemon         -- Standard Server daemon Thread
# Server_HB             -- Server with all Client health heartbeat monitoring
# Server_HB_daemon      -- Server_HB daemon Thread
# Client_HB             -- Client with Server health heartbeat monitoring
# Client_HB_daemon      -- Client_HB daemon Thread
# 
#     Implements various levels of Client and Server functionality,
# optionally as an independent asynchronous I/O processing daemon
# Thread.
# 
class Client_daemon(Mincemeat_daemon):
    """
    Start a mincemeat.Client (by default), as a daemon Thread.

    Defaults to use its own asyncore socket map; set map=<dict> to use
    a common map (and one thread) to active multiple objects derived
    from asyncore.dispatcher or asynchat.async_chat.

    Any additional keyword arguments (no positional args allowed!) are
    passed (ultimately, via kwargs), to Mincemeat_daemon.process;
    override .process to trap additional configuration data you wish
    to pass via __init__ to a derived mincemeat.*_daemon.
    """
    def __init__(self, credentials, cls=Client,
                 **kwargs):
        Mincemeat_daemon.__init__(self, credentials, cls=cls,
                                  **kwargs)


class Server_daemon(Mincemeat_daemon):
    """
    Start a mincemeat.Server (by default), as a daemon Thread.

    We allow any optional keyword arguments to flow through __init__
    in **kwargs, to be passed (by threading.Thread's target=) to our
    process method.  We suggest that you handle any supply custom
    keyword arguments to your derived mincmeat.*_daemon classes this
    way.

    This allows us to uniformly handle arguments to custom derived
    components, where we might mix and match, components; eg. using a
    standard Server_daemon with a custom-derived Server; the standard
    Server_daemon wouldn't know what keyword parameters to strip off
    of its __init__() and pass to the custom derived Server cls, and
    which to forward along for threading.Thread to pass to the target=
    method...

    In summary: override process in *_daemon, and trap custom keyword
    parameters there.  Use keywords only; positional parameters are not
    supported -- only the (required) credentials argument is positional, and
    must be provided, in first position, in every overridden
    Mincemeat_daemon.__init__.
    """
    def __init__(self, credentials, cls=Server,
                 **kwargs):
        Mincemeat_daemon.__init__(self, credentials, cls=cls,
                                  **kwargs)


class Server_HB(Server):
    """
    A mincemeat.Server that monitors the Server's Client channel heartbeats.
    """
    def __init__(self, *args, **kwargs):
        Server.__init__(self, *args, **kwargs)
        self.pongseen = {}

    def starting(self, *args, **kwargs):
        """
        Responds to the following keywords:
        
            "allowed"   -- must see responses to self.ping within allowed interval

        """
        self.allowed = kwargs.get('allowed', None)
        if self.allowed is not None:
            logging.info("%s allowing %.3f s between pongs" % (
                    self.name(), self.allowed))
        Server.starting(self, *args, **kwargs)

    def health(self):
        """
        Override mincemeat.Server.health to check that we've received a
        pong from all Clients, and kill any ServerChannels to
        (apparenly) dead Clients.  Retain the default behavior, which
        is to send a ping to all Clients via their ServerChannel.

        We'll traverse all the Server's known channels; the first time
        we encounter a channel, we'll default to believing we've just
        seen a pong from it's Client (given it the full allowed
        timeout to comply).

        As soon as the last pong seen time exceeds allowed seconds
        ago, we'll kill the channel.  We don't monitor the turnaround
        time, just the wall-clock time since the last pong was seen.
        """
        now = time.time()
        shorten = None
        if self.allowed is not None:
            for chan in self.taskmanager.channels.keys():
                last = self.pongseen.setdefault(chan, now)
                since = now - last
                if since > self.allowed:
                    logging.warning("%s ping timeout  from %s; unhealthy Client; disconnecting!" % (
                            self.name(), chan.name()))
                    chan.handle_close()
                else:
                    logging.debug(   "%s ping response from %s, %.3f s ago" % (
                            self.name(), chan.name(), now - last))
                    requested = self.allowed - since
                    if shorten is None or (requested is not None and requested < shorten):
                        shorten = requested
                        shortest = chan

        if shorten is not None:
            logging.debug("%s needs to see a ping response from %s within %.3f s" % (
                            self.name(), shortest.name(), shorten ))

        # We must return at most self.allowed from now, even if we
        # don't yet have any channels (hence no shortest)...
        return shorten if shorten is not None else self.allowed

    def pong(self, chan, command, data, txn):
        """
        Override mincemeat.Server.pong to just remember when we last
        saw a pong from this channel.
        """
        self.pongseen[chan] = time.time()
        Server.pong(self, chan, command, data, txn)


class Server_HB_daemon(Server_daemon):
    """
    Start a Server with Client channel Heartbeat checks.  After 2
    heartbeat cycles (7 seconds) are missed, the client channel is
    closed (15 seconds).

    The cls must be derived from Server_HB, as we assume ping/pong
    have been instrumented to collect timing information.
    """
    def __init__(self, credentials, cls=Server_HB,
                 interval=7.0, allowed=15.0, timeout=999.0,
                 **kwargs):
        Server_daemon.__init__(self, credentials, cls=cls,
                               interval=interval, allowed=allowed, timeout=timeout,
                               **kwargs)

class Client_HB(Client):
    """
    A mincemeat.Client that knows how heartbeat its Server.
    """
    def __init__(self, *args, **kwargs):
        Client.__init__(self, *args, **kwargs)
        self.pingbeg = time.time()
        self.pongtim = None             # time when pong received
        self.pongtxn = None             # it's transaction

    def starting(self, *args, **kwargs):
        """
        Responds to the following keywords:
        
            "allowed"   -- must see responses to self.ping within allowed interval

        """
        self.allowed = kwargs.get('allowed', None)
        if self.allowed is not None:
            logging.info("%s allowing %.3f s between pongs" % (
                    self.name(), self.allowed))
        Client.starting(self, *args, **kwargs)

    def health(self):
        """
        Allow only a certain amount of delay in ping responses before
        killing our connection to the (crippled) server.

        This method must be invoked (at least) often enough to ensure
        that, under normal circumstances, the inter-ping latency, plus
        any normal Server response delay, plus the round-trip trip
        time does not exceed the allowed time.
        """
        # Check the last pong received.  Defaults to whenever we
        # began.  If we exceed allowed, treat as a protocol failure
        # (same as EOF).  Otherwise, send another ping.
        shorten = None
        now = time.time()
        if self.pongtxn is not None:
            # We note that a pong has been received.  Compute the
            # actual ping latency, and also how long since it was
            # seen.
            delay, delaymsg = self.ping_delay(txn=self.pongtxn,
                                              now=self.pongtim)
            since, sincemsg = self.ping_delay(txn=self.pongtxn,
                                              now=now)
        else:
            delay = 0.0
            delaymsg = "?"
            since = now - self.pingbeg
            sincemsg = "%.3f s (no replies received)" % since

        if self.allowed is not None and since > self.allowed:
            logging.warning("%s ping latency %s, last %s; Exceeds %.3f s allowed; Unhealthy Server; disconnecting!" % (
                self.name(), delaymsg, sincemsg, self.allowed))
            self.handle_close()
        else:
            logging.debug("%s ping latency %s, last %s <= %s" % (
                self.name(), delaymsg, sincemsg, self.allowed))
            shorten = self.allowed - since

        if shorten is not None:
            logging.debug("%s needs to see a pong from Server within %.3f s" % (
                            self.name(), shorten ))

        # May be None, if we just .handle_close()ed ourself...
        return shorten

    def pong(self, command, data, txn):
        """
        Override the default pong, to remember the transaction numbers
        of the ping responses we receive, and the time we received them.
        """
        self.pongtim = time.time()
        self.pongtxn = txn
        Client.pong(self, command, data, txn)


class Client_HB_daemon(Client_daemon):
    """
    Start a Client with Server Heartbeats.  After 2 heartbeat intervals
    (27 seconds) go without response, the client channel is closed (60
    seconds).
    """
    def __init__(self, credentials, cls=Client_HB,
                 interval=27.0, allowed=60.0, timeout=999.0,
                 **kwargs):
        Client_daemon.__init__(self, credentials, cls=cls,
                               interval=interval, allowed=allowed, timeout=timeout,
                               **kwargs)


# ##########################################################################
# 
# run_client            -- Implements a simple, standard Map/Reduce client
# 
#     When run as:
# 
#         python mincemeat.py -p "changeme" [-P port] [hostname]
# 
# we will attempt to register on the given port and interface (default is
# localhost:11235), and implement a simple Map/Reduce client, and process
# requests.  The complete Map/Reduce client implementation is assumed to be
# delivered from the Server in the mapfn, collectfn and reducefn supplied via
# the Protocol.
# 
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
    client.conn(len(args) > 0 and args[0] or "", options.port)

if __name__ == '__main__':
    run_client()
