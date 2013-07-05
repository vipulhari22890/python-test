'''
WORKER PROTOCOL BASE
-------------------------------------------------------------------------------
The data acquisition protocols, which worker sub-processes use to acquire data,
share similarities. This module, especially the class `Protocol`, captures
those similarities.

When creating a new protocol implementation, the specific protocol class should
be a descendant of class `Protocol`. The minimum requirements are:

    1.    Your class needs to have a constructor which calls the `Protocol`
          constructor explicitly, supplying all the required arguments.
    2.    Your class needs to provide a `loop()` method. The method should not
          require any arguments and should return a number of seconds it spent
          sleeping. If the method does not sleep, it should return 0.
          
It is recommended to browse through the code of Modbus RTU/TCP protocols to see
how this module is used.

Created by FS Engineering & Oilfields Supply Ltd
Authors: Istvan Gallo, Vipul Kumar, Maros Gajdosik
'''
# Python Standard Library Imports
import argparse
import logging
import os.path
import sys
import time

# External Imports
pass

# Custom Imports
import config
import db
import ipc
import logs


class Protocol(object):
    '''
    Data acquisition protocol base class. It provides functionalities common to
    almost all protocol implementations. Specific protocol classes should
    inherit from this class.
    '''
    # A dictionary of previously recorded tag values. Used with `has_changed()`
    # method to check if the values have changed.
    values = {}
    # Incremented by 1 every `start()` loop. Used with `due_tags()` to tell
    # which tags are due to be polled as per their poll rates.
    loop_counter = 1
    
    def __init__(self, loop_interval=None):
        '''
        Creates a new `Protocol` class instance.
        
        ARGUMENTS:
        core_pid         PID of the Core process. The worker will keep checking
                         if this process exists on each loop. If the worker
                         cannot find the Core, it stops.
                         
        port_id          Database row ID of the port which the protocol should
                         use to acquire data.
                         
        loop_interval    Length of every loop cycle in seconds. Floating point
                         values < 1 are welcome. If set None, the loop function
                         will run in its internal endless cycle.
        '''
        args = argparse.ArgumentParser()
        args.add_argument('port_id', type=int, help='ID of the port to use '
            'with this protocol.')
        
        args.add_argument('--core_pid', type=int, help='PID of the Core '
            'process (if any).')
        
        args = args.parse_args()
        self.core_pid = args.core_pid
        self.port = db.Session.query(db.Port).get(args.port_id)
        if not self.port:
            print('Cannot find port with ID "%s".' % args.port_id)
            sys.exit(1)
            
        # Initialise the logging interface.
        logs.initialise(self.port.protocol, 'logs' + self.port.protocol +
            '-%Y-%m-%d.log')
            
        self.loop_interval = loop_interval
        self.ipc_client = ipc.IPCClient(config.ipc.SOCKET_NAME if self.core_pid
            else None)
            
        self.data_sources = self.port.data_sources.filter_by(disabled=0)
        self.data_sources_all = self.data_sources.all()
            
    def get_port_params(self, port, required=[]):
        '''
        Breaks apart `port.params` and returns a dictionary of parameter names
        associated with parameter values. Also, checks if all the parameters
        listed in `required` have been extracted.
        
        ARGUMENTS:
        port        (Port) object which provides the `params` property.
        required    (iterable) Names of parameters which need to be extracted.
                    If the required parameters aren't met, the function will
                    raise `Exception`.
                    
        RETURNS:
        (dict) Extracted parameter names associated with their parameter
        values.
        '''
        result = {k: v for k, v in [param.split('=') for param in
            port.params.split('|')]}
        
        for k in result.iterkeys():
            try:
                result[k] = int(result[k])
            except ValueError:
                try:
                    result[k] = float(result[k])
                except ValueError: pass
            
        for param in required:
            if not param in result:
                raise Exception('Missing required parameter "%s" for port %s.'
                    % (param, port))
                                    
        result['port'] = port.address
        return result
        
    def due_tags(self, data_source):
        '''
        Yields all tags from `data_source` which are due to be polled this
        loop cycle.
        
        ARGUMENTS:
        data_source    Data source of which tags should be checked whether due.
        '''
        tags = data_source.tags.all()
        try:
            tags = sorted(tags, lambda x, y: cmp(int(x.address), int(
                y.address)))
                
        except: pass
        
        for tag in tags:
            if not self.loop_counter % int(tag.poll_rate / self.loop_interval):
                yield tag
                
    def has_changed(self, tag, value):
        return tag.value != value
    
    def is_core_running(self):
        ''' Checks if the Core is running. '''
        return not self.core_pid or os.path.exists('/proc/%s' % self.core_pid)
        
    def start(self, loop):
        '''
        Starts the endless loop, calling the `loop` periodically.
        Sleeps the endless loop by `self.loop_interval` seconds.
        
        ARGUMENTS:
        loop_function    A function to call on every cycle. The function MUST
                         return a number of seconds it slept. If the function
                         does not sleep, it should return 0.
        '''
        try:
            if self.loop_interval is not None:
                while True:
                    if not self.is_core_running():
                        logging.error('The Core is stopped. Terminating.')
                        sys.exit()
                        
                    slept = loop()
                    try:
                        time.sleep(self.loop_interval - slept)
                    except IOError:
                        logging.error('Cannot sleep the loop for %s seconds.',
                            self.loop_interval - slept)
                            
                    self.loop_counter += 1
                    
            else:
                # The loop interval is None -- the loop should have its own
                # endless `loop` function.
                loop()
                
        except KeyboardInterrupt: pass
