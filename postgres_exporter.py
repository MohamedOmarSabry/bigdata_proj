"""
PostgreSQL Metrics Exporter for Prometheus
Monitors PostgreSQL database health and performance
"""

from prometheus_client import start_http_server, Gauge, Counter, Info
import psycopg2
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define Prometheus metrics
postgres_up = Gauge('postgres_up', 'PostgreSQL availability (1=up, 0=down)')
postgres_connections = Gauge('postgres_connections', 'Number of active connections')
postgres_max_connections = Gauge('postgres_max_connections', 'Maximum allowed connections')
postgres_database_size_bytes = Gauge('postgres_database_size_bytes', 'Database size in bytes', ['database'])
postgres_table_rows = Gauge('postgres_table_rows', 'Number of rows in table', ['table'])
postgres_table_size_bytes = Gauge('postgres_table_size_bytes', 'Table size in bytes', ['table'])
postgres_cache_hit_ratio = Gauge('postgres_cache_hit_ratio', 'Cache hit ratio (0-1)')
postgres_transactions_committed = Counter('postgres_transactions_committed_total', 'Total committed transactions', ['database'])
postgres_transactions_rolled_back = Counter('postgres_transactions_rolled_back_total', 'Total rolled back transactions', ['database'])
postgres_query_duration_seconds = Gauge('postgres_longest_query_duration_seconds', 'Duration of longest running query')
postgres_active_queries = Gauge('postgres_active_queries', 'Number of active queries')
postgres_idle_connections = Gauge('postgres_idle_connections', 'Number of idle connections')
postgres_locks = Gauge('postgres_locks', 'Number of locks', ['lock_type'])

# Info metrics
postgres_info = Info('postgres', 'PostgreSQL server information')

class PostgresMetricsExporter:
    def __init__(self, host='localhost', database='globalmart_dw', user='postgres', password='1', port=5432):
        self.db_config = {
            'host': host,
            'database': database,
            'user': user,
            'password': password,
            'port': port
        }
        self.connection = None
        self.tables_to_monitor = [
            'dim_user',
            'dim_product',
            'dim_date',
            'dim_time',
            'fact_transactionevent',
            'fact_productview',
            'fact_cartevent'
        ]
        
    def connect(self):
        """Establish connection to PostgreSQL"""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            postgres_up.set(1)
            logger.info("Successfully connected to PostgreSQL")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            postgres_up.set(0)
            return False
    
    def collect_server_info(self):
        """Collect PostgreSQL server information"""
        try:
            with self.connection.cursor() as cur:
                # Get version
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                
                postgres_info.info({
                    'version': version.split(',')[0] if version else 'unknown',
                    'database': self.db_config['database']
                })
                
        except Exception as e:
            logger.warning(f"Error collecting server info: {e}")
    
    def collect_connection_metrics(self):
        """Collect connection-related metrics"""
        try:
            with self.connection.cursor() as cur:
                # Total connections
                cur.execute("""
                    SELECT count(*) 
                    FROM pg_stat_activity 
                    WHERE datname = %s
                """, (self.db_config['database'],))
                total_connections = cur.fetchone()[0]
                postgres_connections.set(total_connections)
                
                # Max connections
                cur.execute("SHOW max_connections")
                max_conn = int(cur.fetchone()[0])
                postgres_max_connections.set(max_conn)
                
                # Active queries
                cur.execute("""
                    SELECT count(*) 
                    FROM pg_stat_activity 
                    WHERE state = 'active' AND datname = %s
                """, (self.db_config['database'],))
                active = cur.fetchone()[0]
                postgres_active_queries.set(active)
                
                # Idle connections
                cur.execute("""
                    SELECT count(*) 
                    FROM pg_stat_activity 
                    WHERE state = 'idle' AND datname = %s
                """, (self.db_config['database'],))
                idle = cur.fetchone()[0]
                postgres_idle_connections.set(idle)
                
        except Exception as e:
            logger.warning(f"Error collecting connection metrics: {e}")
    
    def collect_database_metrics(self):
        """Collect database-level metrics"""
        try:
            with self.connection.cursor() as cur:
                # Database size
                cur.execute("""
                    SELECT pg_database_size(%s)
                """, (self.db_config['database'],))
                db_size = cur.fetchone()[0]
                postgres_database_size_bytes.labels(database=self.db_config['database']).set(db_size)
                
                # Transaction statistics
                cur.execute("""
                    SELECT 
                        xact_commit,
                        xact_rollback
                    FROM pg_stat_database
                    WHERE datname = %s
                """, (self.db_config['database'],))
                result = cur.fetchone()
                if result:
                    # These are cumulative, so we just set them
                    postgres_transactions_committed.labels(database=self.db_config['database'])._value.set(result[0])
                    postgres_transactions_rolled_back.labels(database=self.db_config['database'])._value.set(result[1])
                
                # Cache hit ratio
                cur.execute("""
                    SELECT 
                        sum(blks_hit) / NULLIF(sum(blks_hit) + sum(blks_read), 0) as cache_hit_ratio
                    FROM pg_stat_database
                    WHERE datname = %s
                """, (self.db_config['database'],))
                cache_ratio = cur.fetchone()[0]
                if cache_ratio is not None:
                    postgres_cache_hit_ratio.set(float(cache_ratio))
                
        except Exception as e:
            logger.warning(f"Error collecting database metrics: {e}")
    
    def collect_table_metrics(self):
        """Collect table-level metrics"""
        try:
            with self.connection.cursor() as cur:
                for table in self.tables_to_monitor:
                    try:
                        # Row count
                        cur.execute(f"SELECT COUNT(*) FROM {table}")
                        row_count = cur.fetchone()[0]
                        postgres_table_rows.labels(table=table).set(row_count)
                        
                        # Table size
                        cur.execute("""
                            SELECT pg_total_relation_size(%s)
                        """, (table,))
                        table_size = cur.fetchone()[0]
                        postgres_table_size_bytes.labels(table=table).set(table_size)
                        
                    except Exception as e:
                        logger.warning(f"Error collecting metrics for table {table}: {e}")
                        
        except Exception as e:
            logger.warning(f"Error collecting table metrics: {e}")
    
    def collect_query_metrics(self):
        """Collect query performance metrics"""
        try:
            with self.connection.cursor() as cur:
                # Longest running query
                cur.execute("""
                    SELECT 
                        EXTRACT(EPOCH FROM (NOW() - query_start)) as duration
                    FROM pg_stat_activity
                    WHERE state = 'active' 
                        AND datname = %s
                        AND query NOT LIKE '%%pg_stat_activity%%'
                    ORDER BY query_start ASC
                    LIMIT 1
                """, (self.db_config['database'],))
                result = cur.fetchone()
                if result and result[0]:
                    postgres_query_duration_seconds.set(float(result[0]))
                else:
                    postgres_query_duration_seconds.set(0)
                    
        except Exception as e:
            logger.warning(f"Error collecting query metrics: {e}")
    
    def collect_lock_metrics(self):
        """Collect lock information"""
        try:
            with self.connection.cursor() as cur:
                cur.execute("""
                    SELECT mode, count(*) 
                    FROM pg_locks 
                    WHERE database = (SELECT oid FROM pg_database WHERE datname = %s)
                    GROUP BY mode
                """, (self.db_config['database'],))
                
                for row in cur.fetchall():
                    lock_type, count = row
                    postgres_locks.labels(lock_type=lock_type).set(count)
                    
        except Exception as e:
            logger.warning(f"Error collecting lock metrics: {e}")
    
    def collect_metrics(self):
        """Collect all PostgreSQL metrics"""
        if not self.connection or self.connection.closed:
            if not self.connect():
                return
        
        try:
            self.collect_server_info()
            self.collect_connection_metrics()
            self.collect_database_metrics()
            self.collect_table_metrics()
            self.collect_query_metrics()
            self.collect_lock_metrics()
            
            # Commit to avoid idle in transaction
            self.connection.commit()
            
        except Exception as e:
            logger.error(f"Error collecting metrics: {e}")
            # Try to reconnect
            try:
                if self.connection:
                    self.connection.close()
                self.connect()
            except:
                pass
    
    def run(self, port=9093, interval=15):
        """Start the metrics exporter server"""
        logger.info(f"Starting PostgreSQL metrics exporter on port {port}")
        start_http_server(port)
        
        if not self.connect():
            logger.error("Failed to connect to PostgreSQL. Retrying...")
        
        while True:
            try:
                self.collect_metrics()
                time.sleep(interval)
            except KeyboardInterrupt:
                logger.info("Shutting down exporter...")
                break
            except Exception as e:
                logger.error(f"Error in metrics collection loop: {e}")
                time.sleep(interval)
        
        # Cleanup
        if self.connection:
            self.connection.close()

if __name__ == "__main__":
    exporter = PostgresMetricsExporter()
    exporter.run(port=9093, interval=10)
