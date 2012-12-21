"""
Pooled PostgreSQL database backend for Django.

Requires psycopg 2: http://initd.org/projects/psycopg2
"""
from django.db.backends.postgresql_psycopg2.base import utc_tzinfo_factory
from django.db.backends.postgresql_psycopg2.base import CursorWrapper, \
    DatabaseWrapper as OriginalDatabaseWrapper
from django.db.backends.signals import connection_created
from django.conf import settings
from threading import Lock
import logging
import psycopg2.extensions

logger = logging.getLogger(__name__)

class PooledConnection():
    '''
    Thin wrapper around a psycopg2 connection to handle connection pooling.
    '''
    def __init__(self, pool):
        self._pool = pool
        self._wrapped_connection = pool.getconn()
        logger.debug("Checking out connection %s from pool %s" % (self._wrapped_connection, self._pool))

    def close(self):
        '''
        Override to return the connection to the pool rather than closing it.
        '''
        if self._wrapped_connection and self._pool:
            logger.debug("Returning connection %s to pool %s" % (self._wrapped_connection, self._pool))
            self._pool.putconn(self._wrapped_connection)
            self._wrapped_connection = None

    def __getattr__(self, attr):
        '''
        All other calls proxy through to the "real" connection
        '''
        return getattr(self._wrapped_connection, attr)

'''
This holds our connection pool instances (for each alias in settings.DATABASES that 
uses our PooledDatabaseWrapper.)
'''
connection_pools = {}
connection_pools_lock = Lock()

'''
Simple Postgres pooled connection that uses psycopg2's built-in ThreadedConnectionPool
implementation.  In Django, use this by specifying MAX_CONNS and (optionally) MIN_CONNS 
in the OPTIONS dictionary for the given db entry in settings.DATABASES.  

MAX_CONNS should be equal to the maximum number of threads your app server is configured 
for.  For example, if you are running Gunicorn or Apache/mod_wsgi (in a multiple *process* 
configuration) MAX_CONNS should be set to 1, since you'll have a dedicated python 
interpreter per process/worker.  If you're running Apache/mod_wsgi in a multiple *thread*
configuration set MAX_CONNS to the number of threads you have configured for each process.

By default MIN_CONNS will be set to MAX_CONNS, which prevents connections from being closed.
If your load is spikey and you want to recycle connections, set MIN_CONNS to something lower 
than MAX_CONNS.   I suggest it should be no lower than your 95th percentile concurrency for 
your app server.
'''
class DatabaseWrapper(OriginalDatabaseWrapper):
    def _cursor(self):
        global connection_pools
        settings_dict = self.settings_dict
        if self.connection is None or connection_pools[self.alias]['settings'] != settings_dict:
            # Is this the initial use of the global connection_pools dictionary for 
            # this python interpreter? Build a ThreadedConnectionPool instance and 
            # add it to the dictionary if so.
            if self.alias not in connection_pools or connection_pools[self.alias]['settings'] != settings_dict:
                if settings_dict['NAME'] == '':
                    from django.core.exceptions import ImproperlyConfigured
                    raise ImproperlyConfigured("You need to specify NAME in your Django settings file.")
                conn_params = {
                    'database': settings_dict['NAME'],
                }
                max_conns = settings_dict['OPTIONS'].pop('MAX_CONNS', 1)
                min_conns = settings_dict['OPTIONS'].pop('MIN_CONNS', max_conns)
                conn_params.update(settings_dict['OPTIONS'])
                if 'autocommit' in conn_params:
                    del conn_params['autocommit']
                if settings_dict['USER']:
                    conn_params['user'] = settings_dict['USER']
                if settings_dict['PASSWORD']:
                    conn_params['password'] = settings_dict['PASSWORD']
                if settings_dict['HOST']:
                    conn_params['host'] = settings_dict['HOST']
                if settings_dict['PORT']:
                    conn_params['port'] = settings_dict['PORT']

                connection_pools_lock.acquire()
                try:
                    logger.debug("Creating connection pool for db alias %s" % self.alias)

                    from psycopg2 import pool
                    connection_pools[self.alias] = {
                        'pool': pool.ThreadedConnectionPool(min_conns, max_conns, **conn_params),
                        'settings': dict(settings_dict),
                    }
                finally:
                    connection_pools_lock.release()

            self.connection = PooledConnection(connection_pools[self.alias]['pool'])
            self.connection.set_client_encoding('UTF8')
            tz = 'UTC' if settings.USE_TZ else settings_dict.get('TIME_ZONE')
            if tz:
                try:
                    get_parameter_status = self.connection.get_parameter_status
                except AttributeError:
                    # psycopg2 < 2.0.12 doesn't have get_parameter_status
                    conn_tz = None
                else:
                    conn_tz = get_parameter_status('TimeZone')

                if conn_tz != tz:
                    # Set the time zone in autocommit mode (see #17062)
                    self.connection.set_isolation_level(
                            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                    self.connection.cursor().execute(
                            self.ops.set_time_zone_sql(), [tz])
            self.connection.set_isolation_level(self.isolation_level)
            self._get_pg_version()
            # We'll continue to emulate the old signal frequency in case any code depends upon it
            connection_created.send(sender=self.__class__, connection=self)
        cursor = self.connection.cursor()
        cursor.tzinfo_factory = utc_tzinfo_factory if settings.USE_TZ else None
        return CursorWrapper(cursor)
