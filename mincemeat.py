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

import asynchat
import asyncore
import cPickle as pickle
import collections
import hashlib
import hmac
import logging
import marshal
import optparse
import os
import random
import repr
import select
import socket
import sys
import types
import new
import threading
import time
import timeit
import traceback

VERSION = 0.0


DEFAULT_PORT = 11235

# Choose the best high-resolution timer for the platform
timer = timeit.default_timer
    
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

def process(timeout=None, map=None, schedule=None):
    """
    Processes asyncore based Server or Client events 'til none left,
    dispatching scheduled events as we go.  On Exception, forcibly
    cleans up all sockets using the same asyncore map.

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
    threads may schedule events.  All events are of the form
    (expiry,callable), where expiry is a timer() value, and callable
    is any callable (normally, a bound method of an
    asyncore.dispatcher based object)
    """
    try:
        if map is None:
            map = asyncore.socket_map		# Internal asyncore knowledg!

        beg = now = timer()
        dur = 0.0
        while map and ( timeout is None or dur <= timeout ):
            # Events on 'map' still possible, and either 'timeout' is
            # infinite, or we haven't yet exceeded it.  NOTE: We
            # *always* want to do at least one cycle, even if timeout
            # == 0, hence the <= instead of <.
            exp = None
            if schedule:
                # Dispatch expired scheduled events.  Any scheduled
                # events remaining after 'map' is empty (all sockets
                # closed) will never be fired!
                for exp,fun in sorted(schedule):
                    try:
                        if now >= exp:
                            logging.info("%s expired; Firing %r" % (exp, fun))
                            fun()		# Timer expired; fire fun()!
                        else:
                            break		# Timer in future; done.
                    except Exception, e:
                        # A scheduled event failed; this isn't
                        # considered fatal, but may be of interest...
                        logging.warning("Failed scheduled event %s: %s" % (
                                fun, e))
                
                    # This event was at or before now (and was fired);
                    # remove it and get next one (if any).  We'll
                    # always refresh 'now' after every fun() firing,
                    # b/c arbitrary time has elapsed.
                    schedule.remove( (exp,fun) )
                    now = timer()
                    exp = None
            
            # Done firing any expired events.  The expiry 'exp' of the
            # one that fired the break (if any) will limit expiry
            # timeout; 'now' is recently refreshed.  Deduce time that
            # 'remains'; None means no timeout.
            dur = now - beg
            rem = []
            if exp is not None:           	# A (finite) event awaits...
                rem.append(exp - now)
            if timeout is not None:             # A (finite) timeout defined...
                rem.append(timeout - dur)
            # Convert durations list to scalar minimum, None for inf.
            if rem:
                rem = min(rem)
            else:
                rem = None
            asyncore.loop(timeout=rem, map=map, count=1)
            now = timer()
            dur = now - beg

    except:
        # We're no longer running asyncore.loop, so can't do anything
        # cleanly; just close 'em all...  This should ensure that any
        # socket resources associated with this object get cleaned up.
        # In order to ensure that the original error context gets
        # raised, even if the asyncore.close_all fails, we must use
        # re-raise inside a try-finally.
        try:
            raise
        finally:
            close_all(map=map, ignore_all=True)

    # True iff map isn't empty
    return bool(map)


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
            logging.warning( "Pre-2.6 asyncore; socket map cleanup incomplete: %s" % e )
    


class threaded_async_chat(asynchat.async_chat):
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

    Also supports pre-2.6 asynchat, which didn't support a non-global
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
                    asyncat.async_chat.__init__(self, sock)
                finally:
                    asyncore.socket_map = save_asm

        self.send_lock = threading.Lock()
        self.sending = False		# Is asyncore.loop sending data?
        self.reading = False		# Is asyncore.loop reading data?
        self.shutdown = False		# Have we already shut down?

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
            # condition here, where we could see self.sending as
            # True (from some previous check), but the aysncore.loop
            # is about to invoke writable(), and discover a False?
            # Our caller must have just pushed some data, either
            # before OR after the asyncore.loop's called writable
            # (because they all are serialized on the same lock).  So,
            # if self.sending is False, the check was from before
            # the push; if after (and it's False), then the FIFO was
            # actually empty (data already sent).  So, no race.
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

            # We checked, tried to send, and checked again; we still
            # have data to send, and asyncore.loop's select still
            # doesn't know about it!  We are out of the lock now, so
            # the asyncore.loop thread may now awaken (if it has
            # detected some other activity), and then take note that
            # there is data to send.  If we have time, wait...
            elapsed = timer() - now
            if timeout is not None and elapsed >= timeout:
                break
            
            # We're not empty, and asyncore.loop doesn't yet know.
            # Any non-None and non-zero timeout has not yet elapsed.
            # If timeout is None or 0, it is used as-is; otherwise,
            # the remaining duration is computed.  Wait a bit.
            try:
                r, w, e = select.select([], [self._fileno], [self._fileno],
                                        timeout and timeout - elapsed or timeout)
                if e:
                    self.handle_expt()
            except select.error, err:
                # Anything that wakes us up harshly will wake up the
                # main loop's select.  Done (except ignore
                # well-behaved interrupted system calls).
                if err.arg[0] != errno.EINTR:
                    break

    def tidy_close(self):
        """
        Indicate completion to the peer, by closing outgoing half of
        socket, returning True iff we cleanly shutdown the outgoing
        half of the socket.  We only do this once, and only if we
        aren't reading (indicating an EOF), or in an error condition!
        
        This will result in an EOF flowing through to the
        client, after the current operation is complete, eventually
        leading to a handle_close there, and finally an EOF and a
        handle_close here; the kernel TCP/IP layer will cleanly
        release the resources.  See http://goo.gl/EtAyN for a
        confirmation of these semantics for Windows.

        This may, of course, not actually flow through to the client
        (no route, client hung, ...)  Therefore, the caller needs to
        ensure that we wake up after some time, and forcibly close our
        end of the connection if this doesn't work!  If we haven't
        been given provision to do this, we need to just close the
        socket now.
        """
        if self.shutdown is False:
            self.shutdown = True
            if self.reading is False:
                try:
                     if 0 == self.socket.getsockopt(socket.SOL_SOCKET,
                                                    socket.SO_ERROR):
                         # No reading (EOF), and socket not in error.
                         # There's a good chance that we might be able
                         # to cleanly shutdown the outgoing half, and
                         # flow an EOF through
                         logging.info("%s shut down to peer %s" % (
                                 self.name(), str(self.addr)))
                         self.socket.shutdown(socket.SHUT_WR)
                         return True
                except Exception, e:
                    logging.warning("%s shut down failed: %s" % (
                            self.name(), e ))
            else:
                logging.info("%s shut down at EOF!" % self.name())
        else:
            logging.info("%s shut down already!" % self.name())

        return False


class Protocol(threaded_async_chat):
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
        A Map/Reduce client Protocol instance.  Optionally, provide a
        socket map shared by all asyncore based objects to be activated
        using the same asyncore.loop.
        """
        threaded_async_chat.__init__(self, sock, map=map)
        self.schedule = schedule
        self.shuttout = shuttout

        self.set_terminator("\n")
        self.buffer = []
        self.auth = None
        self.mid_command = False
        self.what = "Proto."
        self.peer = True		# Name shows >peer i'face by default
        self.locl = None		# Just like self.addr, 'til known
        self.update_addresses()

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
                logging.info("%s resolved peer address %s to %s" % (
                        self.name(), str(self.addr), str(addr)))
                self.addr = addr
        except Exception, e:
            logging.info( "Couldn't update peer address: %s" % e )
            pass

        try:
            locl = self.socket.getsockname()
            if self.locl is None or self.locl != locl:
                logging.info("%s resolved locl address %s to %s" % (
                        self.name(), str(self.locl), str(locl)))
                self.locl = locl
        except Exception, e:
            logging.info( "Couldn't update locl address: %s" % e )
            pass

    def connect(self, addr):
        """
        Initiate a connect, setting self.addr to a (tentative)
        address, in case if the underlying asyncore.dispatcher.connect
        doesn't; on some platforms (guess...), this is unreliable.

        The accept and connect paths are the two general ways a socket
        establishes endpoint addresses; on the connect path, we don't
        know the actual .addr (peer addr) and .locl (local ephemeral
        addr), 'til the connect completes.  Since we are on the
        connect path, default to showing the local i'face in our name.
        """
        if self.addr is None:
            self.addr = addr
        self.name(peer=False)
        logging.info( "%s connecting to %s" % ( self.name(), addr ))
        threaded_async_chat.connect(self, addr)

    def handle_connect(self):
        """
        A connect has completed!  We probably need to detect the local
        interface address...
        """
        self.update_addresses()
        logging.info("%s connection established %s->%s" % (
                self.name(),
                str(self.locl), str(self.addr)))

    def authenticated(self):
        return self.auth == "Done"

    def send_command(self, command, data=None, txn=None):
        """
        Allows the asyncore.loop thread OR an external thread to
        compose and send a command, pushing it onto the async_chat
        FIFO for transmission.  This is thread-safe (due to
        threaded_async_chat), even though async_chat.push... breaks
        the transmission into output buffer sized blocks in push, and
        appends them to a FIFO; simultaneous calls to push could
        interleave data, but are locked (see threaded_async_chat).  It
        ultimately initiates an attempt to asyncore.dispatcher.send
        some data.

        When any service is complete, the asyncore.loop service thread
        will prepare to wait in select/poll, and will call writable(),
        which will return True because there is data awaiting
        transmission in the dispatcher's FIFO, so the loop service
        thread will awaken (later) and send data when there is
        outgoing buffer available on the socket.

        This interface may also be invoked by external threads, via
        send_command_backchannel (below).  In that case, the
        asyncore.loop thread (blocking in its own select/poll) will
        NOT know that output is now ready for sending; until it
        awakens, the external thread must ensure that the data gets
        sent, using flush_backchannel.

        The command may or may not contain an optional transaction; we
        append it, if supplied.  If an (optional) data segment is
        supplied, its length follows the mandatory ':'

            <command>[/<transaction>]:[length]\n
            [data\n]
        """
        if txn:
            if "/" in command:
                raise SyntaxError("Command already contains transaction id!")
            command += "/" + str(txn)
        if not ":" in command:
            command += ":"
        if data:
            pdata = pickle.dumps(data)
            command += str(len(pdata))
            logging.debug( "%s -->%s( %s )" % (self.name(), command, repr.repr(data)))
            self.push(command + "\n" + pdata)
        else:
            logging.debug( "%s -->%s" % (self.name(), command))
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
        getattr(logging,type)("%s %s" % (self.name(), message))

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
        if self.shuttout 			\
             and self.schedule is not None	 \
             and self.tidy_close():
            # We performed a tidy close!  Schedule a real close, in
            # case it doesn't flow through...
            self.schedule.append( (timer() + self.shuttout, 
                                   self.handle_close) )
        else:
            # Already did a shutdown, no connection timeouts, or no
            # schedule to handle them, or an exception while
            # attempting to shut down socket and schedule a future
            # cleanup.  Just close.
            logging.info("%s closing connection to peer %s" % (
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


    # -------------------------------------------------------------------------
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
        
    # -------------------------------------------------------------------------
    # Authenticated commands
    # 
    #      After both ends of the channel have authenticated eachother, they may
    # then use these commands.
    # 
    def respond_to_ping(self, command, data, txn):
        """
        Ping.  By default, expectes a (message, payload) and just
        returns a new message with the same payload.
        """
        try:
            message, payload = data
        except TypeError:
            message = data
            payload = None
        logging.info("%s %s:%s" % (self.name(), command, repr.repr((message, payload))))
        message = "Reply from %s" % socket.getfqdn()
        
        self.send_command("pong", (message, payload), txn)

    def pong(self, command, data, txn):
        """
        Pong (response to Ping).
        """
        try:
            message, payload = data
        except TypeError:
            message = data
            payload = None
        logging.info("%s %s:%s" % (self.name(), command, repr.repr((message, payload))))

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

        if self.unrecognized_command( command, data, txn):
            return True

        self.failed_command( command, data, txn)
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

    # -------------------------------------------------------------------------
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
                [ "def _f(arg):" ] +
                [ "  _%d = arg[%d] "     % ( n, n ) for n in ncells ] +
                [ "  return lambda:(%s)" % ','.join( "_%d" %n for n in ncells ) ] +
                [ "" ])
            try:
                exec src
            except:
                raise SyntaxError(src)
            clos = _f(clos_tupl).func_closure

        return new.function(code, globs, name, dflt, clos)
        

class Client(Protocol):
    """
    Implements a mincemeat Map/Reduce Client.  Connect's to a
    specified server:port, and processes commands (authentication is
    handled by the Protocol superclass).
    """
    def __init__(self, map=None, schedule=None, shuttout=None):
        """
        Instantiate a Map/Reduce Client connection to a server.
        """
        Protocol.__init__(self, map=map, schedule=schedule, shuttout=shuttout)
        self.mapfn = self.reducefn = self.collectfn = None
        self.name("Client")

    def finished(self):
        return self.shutdown is True

    def conn(self, interface="", port=DEFAULT_PORT, password=None,
             asynchronous=False, **kwargs):
        """
        Establish Client connection, and (optionally) synchronously
        loop 'til all file descriptors closed.  Optionally specifies
        password.  Note that order is different than
        Server.run_server, for historical reasons.  Ignores additional
        unrecognized keyward args, so that identical packages of
        configuration may be used for both a Client.conn and a
        Server.conn call.
        
        If no server port exists to bind to, on Windows the
        select.select() call will return an "exceptional" condition on
        the socket; on *nix, a "readable" condition (and a
        handle_connect()), followed by an error on read (errno 11,
        "Resource temporarily unavailable") and a handle_close().  In
        either case, on the loopback interface, this occurs in ~1
        second.

        Since this connection is performed asynchronously, the invoker
        may want to check that .auth is 'Done' after this call, to
        ensure that we successfully connected to and authenticated a
        server...
        
        Since the default kernel socket behavior for interface == ""
        is inconsistent, we'll choose "localhost".
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
        
    def process(self, timeout=None):
        """
        Run this Client (and anything else sharing its _map), cleaning
        it up on failure.  The loop function may raise an exception,
        but it will forcibly clean up all the map sockets on its way
        out, so we don't have to worry about that (if it exits
        cleanly, all map sockets were already cleaned up).
        
        The meaning of 'timeout' here differs from asyncore.loop; if
        None, only returns when complete (map becomes empty).  If a
        positive numeric value is provided, it returns (soon) after
        that timeout duration has passed.  
        
        Returns Tree iff there may yet be more events to process (and
        caller should re-invoke.)
        """
        return process(timeout=timeout, map=self._map, schedule=self.schedule)

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
        wrapped to produce a ( key, [ value ] ) tuple, as would be
        produced by the normal Map phase.  We use 'applyover' to
        handle either simple functions operating on each individual
        (key,[value,...])  item, or generators which operate over the
        sequence of all items (and hence may employ remembered state).

        Note that if command contains a 
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
            # (eg. discarding invalid keys, adding new keys).
            results = dict((k, [v]) for k, v in rgen)

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
        logging.info( "%s Reducing %s" % (self.name(), repr.repr(data)))
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


class Server(asyncore.dispatcher, object):
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

    Note that this must be a "new-style" class (hence the extraneous
    class dependency on 'object').  This is required to support
    Properties (see datasource = ..., below)
    """

    def __init__(self, map=None, schedule=None, shuttout=None):
        """
        A mincemeat Map/Reduce Server.  Specify a 'map' dict, if other
        asyncore based facilities are implemented, and you wish to run
        this Server with a separate asyncore.loop.  Any ServerChannels
        created due to incoming Client request will also share this
        map.  Every Server, Client (or other asyncore object) which
        uses the same asyncore.loop thread must share the same map.
        """
        if map is None:
            # Preserve pre-2.6 compatibility by avoiding map, iff None
            asyncore.dispatcher.__init__(self)
        else:
            asyncore.dispatcher.__init__(self, map=map)

        self.schedule = schedule
        self.shuttout = shuttout	# If None, no shutdown timeout (just close)

        self.taskmanager = None
        self.password = None
        self.shutdown = False		# Termination indication to clients

        self.mapfn = None
        self.collectfn = None
        self.reducefn = None
        self.finishfn = None
        self.resultfn = None


    # -------------------------------------------------------------------------
    # Methods required by Mincemeat_daemon

    def authenticated(self):
        """
        Server has no authenticated phase; only its ServerChannels and Clients
        """
        return False

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

    def log_info(self, message, type):
        getattr(logging,type)(message)
        
    def run_server(self, password="", port=DEFAULT_PORT, interface="",
                   asynchronous=False):
        """
        Runs the Server.  Use this method in the default asynchronous
        == False form, if and only if this the only asyncore based
        application running in this Python instance!  Otherwise, use
        the component methods (ensure that the caller runs
        asyncore.loop in another thread.)

            s = mincemeat.Server()
            s.datasource = ...
            s.mapfn = ...
            s.collectfn = ...
            s.reducefn = ...
            s.finishfn = ...
            
            s.setup(**credentials)
        
            ac = threading.Thread(target=s.process)
            ac.start()
            while not s.finished():
                ac.join(.1)
            ac.join()

            results = s.results()

        Note that using asynchronous = True requires that the caller
        has created at least one asyncore based object (or the
        asyncore.loop() call will return instantly, and the Thread
        will terminate...
        """
        self.conn(password=password, port=port, interface=interface,
                   asynchronous=asynchronous)

        # If we are asynchronous, we have NOT initiated processing;
        # This will return None, if not finished(), or the results if
        # we are were not asynchronous.
        return self.results()

    def conn(self, password="", port=DEFAULT_PORT, interface="",
             asynchronous=False, **kwargs):
        """
        Establish this Server, allowing Clients to connect to it.
        This will allow exactly one Server bound to a specific port to
        accept incoming Client connections.  Any additional keyword
        args are assumed to be (optional) values for Server attributes
        (eg. .datasource, .mapfn, ...)

        If asynchronous, then we will not initiate processing; it is
        the caller's responsibility to do so; every Client and/or
        Server with a shared map=... require only one
        asyncore.loop(map=obj._map) or obj.process() thread.

        The default behaviour of bind when interface == '' is pretty
        consistently to bind to all available interfaces.
        """
        for k, v in kwargs.iteritems():
            if hasattr(self,k):
                if getattr(self,k) is None:
                    logging.info("%s conn setting %s = %s" % (self.name(), k, v))
                else:
                    logging.info("%s conn setting %s = %s (was %s)" % (
                            self.name(), k, v, getattr(self, k)))
                setattr(self, k, v)
            else:
                logging.debug("%s conn ignores %s = %s" % (self.name(), k, v))
        self.password = password
        logging.debug("Server port opening.")
        try:
            self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
            if hasattr(socket, 'SO_EXCLUSIVEADDRUSE'):
                # Windows socket re-use semantics differ from *nix (read:
                # are broken).  See http://goo.gl/J89cr
                self.socket.setsockopt( socket.SOL_SOCKET,
                                        socket.SO_EXCLUSIVEADDRUSE, 1)
            self.bind((interface, port))
            self.listen(1)
        except:
            # If anything fails during socket creation, we need to
            # ensure we clean up the partially opened socket;
            # otherwise, it'll leave a busted entry in
            # asyncore.socket_map, which will prevent asyncore.loop()
            # from working correctly.
            logging.error( "Server couldn't bind to %s" % str( ( interface, port ) ))
            #logging.error(traceback.format_exc())
            self.close()
            raise

        # If either the Map or Reduce functions are empty, direct the
        # TaskManager to skip that phase.  Since Server.reducefn and
        # .mapfn are not set at Server.__init__ time, we must defer
        # 'til .setup is invoked, to detect if these are provided.
        if self.reducefn is None:
            self.taskmanager.tasks = TaskManager.MAPONLY
        elif self.mapfn is None:
            self.taskmanager.tasks = TaskManager.REDUCEONLY

        if asynchronous is False:
            self.process()

    def process(self, timeout=None):
        """
        Run this Server (and anything else sharing its _map), cleaning
        it up on failure.  (See mincemeat.process or
        mincemeat.Client.process for more details)
        """
        return process(timeout=timeout, map=self._map, schedule=self.schedule)

    def finished(self):
        """
        Detect if finished.  If no self.taskmanager, self.datasource
        has not been set, hence not finished.
        """
        return self.taskmanager \
            and self.taskmanager.state == TaskManager.FINISHED

    def results(self):
        # Successfully completed Map/Reduce.  Return results.
        if not self.finished():
            return None
        return self.taskmanager.results

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
        except socket.error, err:
            # ECONNABORTED might be thrown on *BSD (see issue 105). 
            # In Python 2.7+, this is handled (returns None).
            if err[0] != errno.ECONNABORTED:
                logging.error(traceback.format_exc())
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
        EOF (or other failure) on our socket.  We have a chance to
        tidy up nicely.  Arrange to send an EOF on all clients by
        using socket.shutdown(SHUT_WR) to close the outbound half of
        all file descriptors.  This will cause them to finish up their
        current command, send the result, receive EOF, and close
        nicely.  The Protocol will handle establishing deferred
        closes, if EOF doesn't flow through.
        """
        try:
            self.close()
        except Exception, e:
            logging.warning("%s closing main port failed: %s" % (
                    self.name(), e))
        if self.taskmanager:
            for chan in self.taskmanager.channels.keys():
                try:
                    chan.handle_close()
                except Exception, e:
                    logging.warning("%s closing %s failed: %s" % ( 
                            self.name(), chan.name(), e ))

    def handle_expt(self):
        """
        Consider an exceptional condition on our main socket a
        protocol failure.
        """
        self.handle_close()

    def set_datasource(self, ds):
        """
        Setting a new datasource replaces the TaskManager.  This must
        be done only when no Map/Reduce tasks are outstanding, because
        the incoming responses will be supplied to the Server's
        current TaskManager.  Unfortunately, it it is almost
        impossible to ensure; communications to a client may be
        nondeterministically delayed, so ancient responses may arrive
        at any time.
        """
        # Create a TaskManager, or give an idle one the datasource
        if self.taskmanager is None:
            self.taskmanager = TaskManager(ds, self)
        else:
            if self.taskmanager.state != TaskManager.IDLE:
                raise Exception("TaskManager Busy")
            self.taskmanager.datasource = ds

        # If a non-None datasource provided, START the IDLE
        # TaskManager, and jump-start any idling channels.
        if ds is not None:
            if self.taskmanager.state is TaskManager.IDLE:
                self.taskmanager.state = TaskManager.START
                for chan, activity in self.taskmanager.channels.iteritems():
                    if activity is None:
                        chan.start_new_task()
    
    def get_datasource(self):
        return self.taskmanager and self.taskmanager.datasource or None

    datasource = property(get_datasource, set_datasource)

    def unrecognized_command(self, command, data=None, txn=None, chan=None):
        """
        Process a command, if possible, from the specified
        ServerChannel; returns True iff handled.  We will get all
        unrecognized commands received by our ServerChannels.  If we
        don't recognize them, we just return False to elicit the
        default behavior (protocol failure; close channel.)
        """
        return False



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

    # We use these ...taskmanager.channel_...() methods to maintain
    # channel state in the taskmanager, ONLY for Map/Reduce task
    # commands we issue and the corresponding responses we receive.
    # Any other commands sent/received via the ServerChannel do not
    # affect the taskmanager state.  This allows the taskmanager to
    # use its channel_... state to determine how the ServerChannel
    # needs to be prodded, as a Map/Reduce Transaction proceeds.
    def start_new_task(self):
        if self.server.shutdown:
            self.tidy_close()
            return
        command, data, txn = self.server.taskmanager.next_task(self)
        if command == None:
            self.server.taskmanager.channel_idle(self)
            logging.info("%s idle" % self.name())
            return
        self.server.taskmanager.channel_sending(self, command, txn)
        self.send_command(command, data, txn)

    def map_done(self, command, data, txn):
        self.server.taskmanager.channel_process(self, command, txn)
        self.server.taskmanager.map_done(data, txn)
        self.start_new_task()

    def reduce_done(self, command, data, txn):
        self.server.taskmanager.channel_process(self, command, txn)
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

    def unrecognized_command(self, command, data=None, txn=None):
        """
        Direct all other unknown incoming commands to the Server.  If
        it doesn't like 'em, we're toast; consider it a protocol
        failure (by invoking original base class method).
        """
        if self.server.unrecognized_command( command, data, txn, chan=self):
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
            self.send_command('mapfn', self.store_func( self.server.mapfn ))
        if self.server.reducefn:
            self.send_command('reducefn', self.store_func( self.server.reducefn ))
        if self.server.collectfn:
            self.send_command('collectfn', self.store_func( self.server.collectfn ))
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
    (duplicately!) re-assigned to the next client channel that asks.
    This takes care of stalled or failed clients.

    When all 'map' tasks have been reported as completed (any
    duplicate responses are ignored), then the 'reduce' tasks are
    assigned to the subsequent next_task-calling channels.

    Finally, once all Map/Reduce tasks are completed, the clients are
    given the 'disconnect' task.

    """
    # Possible .state
    IDLE	= 0		# Awaiting a datasource
    START	= 1		# Ready to start; prep iterator, decide phase
    MAPPING	= 2		# Performing Map phase of task
    REDUCING	= 3		# Performing Reduce phase of task
    FINISHING	= 4		# Performing Finish phase, preparing results
    FINISHED	= 5		# Final results available; loop or quit

    # Possible .tasks option
    MAPREDUCE	= 0
    MAPONLY	= 1		# Only perform the Map phase
    REDUCEONLY	= 2		# Only perform the Reduce phase

    # Possible .allocation option
    CONTINUOUS	= 0		# Continuously allocate tasks to every channel
    ONESHOT	= 1		# Only allocate a single Map/Reduce task to each

    # Possible .cycle options
    SINGLEUSE   = 0		# After finishing, close Server and 'disconnect' clients
    PERMANENT   = 1		# Go idle 'til another Map/Reduce transaction starts

    txn = "0"
    def __init__(self, datasource, server,
                 tasks=None, allocation=None, cycle=None):
        self.datasource = datasource
        self.server = server
        self.state = TaskManager.IDLE
        self.tasks = tasks or TaskManager.MAPREDUCE
        self.allocation = allocation or TaskManager.CONTINUOUS
        self.cycle = cycle or TaskManager.SINGLEUSE

        # Track what channels exist, and were last reported as being
        # up to { addr: (command, timetamp), ... }
        self.channels = {}

        # Each time we create another TaskManager (or an existing Task
        # Manager moves onto a new Map/Reduce Transaction), we'll
        # advance this.  This will enforce a new Transaction number
        # for every possible transaction, during the possible lifespan
        # of the python interpreter's connections to any Client
        self.txn = str(int(self.txn) + 1)


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
    def channel_opened(self, chan):
        self.channel_idle(chan)

    def channel_closed(self, chan):
        self.channel_log(chan, "Disconnect")
        self.channels.pop(chan, None)

    def channel_idle(self, chan):
        self.channel_log(chan, "Idle")
        self.channels[chan] = None

    def channel_sending(self, chan, command, txn):
        if self.channels.get(chan, None):
            self.channel_log(chan, "Done")
        self.channels[chan] = (txn, command, None, time.time())
        self.channel_log(chan, "Sending")

    def channel_process(self, chan, response, txn):
        what = "Processing"
        try:
            cmdtxn, command, __, started = self.channels[chan]
        except:
            cmdtxn = None
            command = None
            started = time.time()
            what = "Spontaneous"
            
        if txn != cmdtxn:
            return
        self.channels[chan] = (cmdtxn, command, response, started)
        self.channel_log(chan, "Processing")

    def channel_log(self, chan, what, how = "info"):
        if chan is None:
            # No chan; Just print header
            getattr(logging, how)("%-25s Time    State      Txn  Response  Command" % (
                    "Channel"))
            return
        tpl = self.channels.get(chan, None)
        if tpl is None:
            getattr(logging, how)('%-25s %6.3fs %-10s %-4s<%-10s>%s' % (
                    chan.name(), 0.0, what,
                    '', '',''))
        else:
            txn, command, response, when = tpl
            getattr(logging, how)('%-25s %6.3fs %-10s %-4s<%-10s>%s' % (
                    chan.name(), time.time() - when, what,
                    txn, response and response or "", command ))

    def next_task(self, channel):
        if self.state == TaskManager.IDLE:
            # Initial state (no datasource, or not yet appropriate
            # to start.  Issue any clients an idle task.
            return (None, None, None)

        if self.state == TaskManager.START:
            self.map_iter = iter(self.datasource)
            self.working_maps = {}
            self.map_results = {}
            self.state = TaskManager.MAPPING
            if self.tasks is TaskManager.REDUCEONLY:
                # If Reduce only, skip the Map phase, passing source
                # key/value pairs straight to the Reduce phase.
                self.reduce_iter = self.map_iter
                self.working_reduces = {}
                self.result = {}
                self.stats = TaskManager.REDUCING
                logging.debug('Server Reducing (skipping Map)')
            else:
                logging.debug('Server Mapping')

        if self.state == TaskManager.MAPPING:
            try:
                map_key = self.map_iter.next()
                map_item = map_key, self.datasource[map_key]
                self.working_maps[map_item[0]] = map_item[1]
                return ('map', map_item, self.txn)
            except StopIteration:
                # A complete iteration of map items is done; either
                # pick a random one of those not yet complete to
                # re-do, or let the client go idle.  
                if self.allocation is self.CONTINUOUS:
                    if len(self.working_maps) > 0:
                        key = random.choice(self.working_maps.keys())
                        return ('map', (key, self.working_maps[key]), self.txn)
                else:
                    return (None, None, None)

                # No more entries left to Map; begin Reduce (or skip)
                self.state = TaskManager.REDUCING
                self.reduce_iter = self.map_results.iteritems()
                self.working_reduces = {}
                self.results = {}
                if self.tasks is TaskManager.MAPONLY:
                    # Skip Reduce phase, passing the key/value pairs
                    # output by Map straight to the result.
                    self.results = self.map_results
                    self.state = TaskManager.FINISHING
                    logging.debug('Server Finishing (skipping Reduce)')
                else:
                    logging.debug('Server Reducing')

        if self.state == TaskManager.REDUCING:
            try:
                reduce_item = self.reduce_iter.next()
                self.working_reduces[reduce_item[0]] = reduce_item[1]
                return ('reduce', reduce_item, self.txn)
            except StopIteration:
                if len(self.working_reduces) > 0:
                    key = random.choice(self.working_reduces.keys())
                    return ('reduce', (key, self.working_reduces[key]), self.txn)
                # No more entries left to Reduce; finish (self.results now valid)
                self.state = TaskManager.FINISHING

        if self.state == TaskManager.FINISHING:
            # Map/Reduce Task done.` If .finishfn supplied, support
            # either .finishfn(iterator), or .finishfn(key,value),
            # apply it -- the resultant values are assumed to be
            # finished results, and are NOT encapsulated as a list.
            if self.server.finishfn:
                # Create a finished result dictionary by applying the
                # supplied Server.finishfn over the Reduce results.  
                self.results \
                    = dict( applyover(self.server.finishfn,
                                      self.results.iteritems()))
            self.state = TaskManager.FINISHED

        if self.state == TaskManager.FINISHED:
            # All done; results ready.  Invoke optional .resultfn with
            # results.  Stop accepting new Client connections, and
            # send a 'disconnect' to each client that asks for a new
            # task.  TODO: For PERMANENT TaskManagers, we do NOT want
            # to close; in fact, we can remove this (and server
            # argument), and let the server itself decide when it
            # should shut down.
            if self.server.resultfn:
                self.server.resultfn( self.results )
            self.server.handle_close()
            return ('disconnect', None, self.txn)
    
    def map_done(self, data, txn):
        # Don't use the results if they've already been counted, or if
        # they don't belong to this transaction!  Since clients may
        # exist beyond the scope of one transaction, and may be
        # blocked ordelayed nondeterministically, we can't assume that
        # every Map or Reduce response was associated with this
        # Transaction.
        if txn != self.txn:
            logging.error("%s Map result Transaction mismatch: %s != %s" % (
                    self.server.name(), txn, self.txn ))
            return
        if not data[0] in self.working_maps:
            return
        logging.debug("Map Done: %s ==> %s" % (data[0], repr.repr(data[1])))
        for (key, values) in data[1].iteritems():
            if key not in self.map_results:
                self.map_results[key] = []
            self.map_results[key].extend(values)
        del self.working_maps[data[0]]
                                
    def reduce_done(self, data, txn):
        # Don't use the results if they've already been counted
        if txn != self.txn:
            logging.error("%s Reduce result Transaction mismatch: %s != %s" % (
                    self.server.name(), txn, self.txn ))
            return
        if not data[0] in self.working_reduces:
            return
        logging.debug("Reduce Done: %s ==> %s" % (data[0], repr.repr(data[1])))
        self.results[data[0]] = data[1]
        del self.working_reduces[data[0]]


class Mincemeat_daemon(threading.Thread):
    """
    A mincemeat Map/Reduce Client or Server daemon thread.  A non-None
    numeric 'timeout=' keyword arg creation will cause timeout() to be
    fired about every that many seconds.

    Note that an overridden Thread.run method does *not* receive *args
    or **kwargs; Only a target=... method does...

    Raises exception on the Server's failure to bind, or a Client's
    failure to connect.

    After start(), processes 'til the processing loop completes, and
    enters "success" state; if an exception terminates processing,
    enters a "failure: ..." state.
    """
    def __init__(self, credentials=None, cls=None,
                 map=None, schedule=None, shuttout=.5,
                 *args, **kwargs):
        threading.Thread.__init__(self, target=self.process,
                                  args=args, kwargs=kwargs)
        self.daemon = True
        if not hasattr(self, 'is_alive'):
            # Pre-2.6 threading.Thread didn't have is_alive...
            self.is_alive = self.isAlive

        self._state = "idle"

        # Instantiate the (NOT optional!) Client or Server class, and
        # invoke its conn with the (also NOT optional) credentials.
        # We make them none to ensure that they are not positional
        # parameters, and force this to crash if not specified.
        if map is None:
            map = {}
        if schedule is None:
            schedule = collections.deque()
        self.mincemeat = cls(map=map, schedule=schedule, shuttout=shuttout)

        self.mincemeat.conn(asynchronous=True, **credentials)


    def name(self):
        return self.mincemeat.name()

    def state(self):
        if self._state == "processing":
            if self.mincemeat.authenticated():
                self._state = "authenticated"
        return self._state

    def process(self, *args, **kwargs):
        """
        Invoke the event processing on the mincemeat Client or Server.
        Any extraneous arguments passed to the constructor are assumed
        to be arguments to .process(...)

        Default processing loop.  With no args, will simply process
        with no timeout, 'til underlying mincemeat object is finished
        (its asyncore.dispatcher._map is empty).

        If timeout() is overriden in derived class, pass a
        'timeout=#.#' keyword arg to class consructor (eg. {
        'timeout': 30.0 }), to implement a timed event-loop.
        """
        logging.info("%s processing(%s, %s)" % (
                self.name(), repr.repr(args), repr.repr(kwargs)))
        self._state = "processing"
        try:
            while self.mincemeat.process(*args, **kwargs):
                self.timeout()
            self.timeout(done=True)
        except Exception, e:
            logging.error("%s failed: %s" % (self.name(), e))
            self._state = "failed: %s" % e
        else:
            # Normal exit; if finished(), assume success
            if self.mincemeat.finished():
                self._state = "success"
            else:
                self._state = "failed: incomplete"
        logging.info("%s stopping: %s" % (
                self.name(), self.state()))

    def timeout(self, done=False):
        """
        Override in derived class (and pass a numeric 'timeout'
        keyword argument on daemon creation), if a timed event-loop is
        desired.  It is invoked with done=True when the event loop
        detects completion.
        """
        pass

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
        self.mincemeat.handle_close()
        self.join(timeout)
        # Still not dead?  Some client must be frozen.  Try harder.
        if self.is_alive():
            close_all(map=self.mincemeat._map, ignore_all=True)
            self.join()

class Client_daemon(Mincemeat_daemon):
    """
    Start a mincemeat.Client (by default), as a daemon.
    """
    def __init__(self, credentials, cls=Client,
                 map=None, schedule=None, shuttout=.5,
                 *args, **kwargs):
        Mincemeat_daemon.__init__(self, credentials, cls=cls, **kwargs)

class Server_daemon(Mincemeat_daemon):
    """
    Start a mincemeat.Server (by default), as a daemon.
    """
    def __init__(self, credentials, cls=Server,
                 map=None, schedule=None, shuttout=.5,
                 *args, **kwargs):
        Mincemeat_daemon.__init__(self, credentials, cls=cls, **kwargs)

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
    client.conn( len(args) > 0 and args[0] or "", options.port)

if __name__ == '__main__':
    run_client()
