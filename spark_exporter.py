"""
Spark Metrics Exporter for Prometheus
Monitors Spark application health and streaming metrics
"""

from prometheus_client import start_http_server, Gauge, Counter, Info
import requests
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define Prometheus metrics
spark_app_status = Gauge('spark_application_status', 'Spark application status (1=running, 0=stopped)')
spark_active_jobs = Gauge('spark_active_jobs', 'Number of active Spark jobs')
spark_completed_jobs = Counter('spark_completed_jobs_total', 'Total completed Spark jobs')
spark_failed_jobs = Counter('spark_failed_jobs_total', 'Total failed Spark jobs')
spark_streaming_batch_duration = Gauge('spark_streaming_batch_duration_ms', 'Streaming batch processing duration in ms')
spark_streaming_records_processed = Counter('spark_streaming_records_processed_total', 'Total records processed', ['stream'])
spark_executor_count = Gauge('spark_executor_count', 'Number of active executors')
spark_executor_memory = Gauge('spark_executor_memory_used_mb', 'Executor memory used in MB', ['executor_id'])
spark_driver_memory = Gauge('spark_driver_memory_used_mb', 'Driver memory used in MB')
spark_stages_active = Gauge('spark_stages_active', 'Number of active stages')
spark_stages_completed = Counter('spark_stages_completed_total', 'Total completed stages')
spark_stages_failed = Counter('spark_stages_failed_total', 'Total failed stages')

# Info metrics
spark_info = Info('spark_application', 'Spark application information')

class SparkMetricsExporter:
    def __init__(self, spark_ui_url='http://localhost:4040'):
        self.spark_ui_url = spark_ui_url
        self.api_url = f"{spark_ui_url}/api/v1/applications"
        self.app_id = None
        self.last_job_count = 0
        self.last_stage_count = 0
        
    def check_spark_availability(self) -> bool:
        """Check if Spark UI is accessible"""
        try:
            response = requests.get(self.api_url, timeout=5)
            if response.status_code == 200:
                spark_app_status.set(1)
                return True
            else:
                spark_app_status.set(0)
                return False
        except requests.exceptions.RequestException as e:
            logger.warning(f"Spark UI not accessible: {e}")
            spark_app_status.set(0)
            return False
    
    def get_application_id(self):
        """Get the running Spark application ID"""
        try:
            response = requests.get(self.api_url, timeout=5)
            if response.status_code == 200:
                apps = response.json()
                if apps:
                    # Get the first active application
                    for app in apps:
                        if 'id' in app:
                            self.app_id = app['id']
                            # Set application info
                            spark_info.info({
                                'app_id': app.get('id', 'unknown'),
                                'app_name': app.get('name', 'unknown'),
                                'spark_version': app.get('sparkVersion', 'unknown')
                            })
                            logger.info(f"Monitoring Spark app: {app.get('name')} ({self.app_id})")
                            return True
            return False
        except Exception as e:
            logger.error(f"Error getting application ID: {e}")
            return False
    
    def collect_job_metrics(self):
        """Collect Spark job metrics"""
        if not self.app_id:
            return
        
        try:
            jobs_url = f"{self.api_url}/{self.app_id}/jobs"
            response = requests.get(jobs_url, timeout=5)
            
            if response.status_code == 200:
                jobs = response.json()
                
                # Count jobs by status
                active = sum(1 for job in jobs if job.get('status') == 'RUNNING')
                completed = sum(1 for job in jobs if job.get('status') == 'SUCCEEDED')
                failed = sum(1 for job in jobs if job.get('status') == 'FAILED')
                
                spark_active_jobs.set(active)
                
                # Track completed and failed jobs incrementally
                if completed > self.last_job_count:
                    spark_completed_jobs.inc(completed - self.last_job_count)
                    self.last_job_count = completed
                    
        except Exception as e:
            logger.warning(f"Error collecting job metrics: {e}")
    
    def collect_stage_metrics(self):
        """Collect Spark stage metrics"""
        if not self.app_id:
            return
        
        try:
            stages_url = f"{self.api_url}/{self.app_id}/stages"
            response = requests.get(stages_url, timeout=5)
            
            if response.status_code == 200:
                stages = response.json()
                
                # Count stages by status
                active = sum(1 for stage in stages if stage.get('status') == 'ACTIVE')
                completed = sum(1 for stage in stages if stage.get('status') == 'COMPLETE')
                failed = sum(1 for stage in stages if stage.get('status') == 'FAILED')
                
                spark_stages_active.set(active)
                
                if completed > self.last_stage_count:
                    spark_stages_completed.inc(completed - self.last_stage_count)
                    self.last_stage_count = completed
                    
        except Exception as e:
            logger.warning(f"Error collecting stage metrics: {e}")
    
    def collect_executor_metrics(self):
        """Collect Spark executor metrics"""
        if not self.app_id:
            return
        
        try:
            executors_url = f"{self.api_url}/{self.app_id}/executors"
            response = requests.get(executors_url, timeout=5)
            
            if response.status_code == 200:
                executors = response.json()
                
                # Count active executors
                active_executors = len([e for e in executors if e.get('isActive', False)])
                spark_executor_count.set(active_executors)
                
                # Collect memory metrics for each executor
                for executor in executors:
                    if executor.get('isActive', False):
                        executor_id = executor.get('id', 'unknown')
                        memory_used = executor.get('memoryUsed', 0) / (1024 * 1024)  # Convert to MB
                        spark_executor_memory.labels(executor_id=executor_id).set(memory_used)
                        
                        # Driver metrics
                        if executor_id == 'driver':
                            spark_driver_memory.set(memory_used)
                            
        except Exception as e:
            logger.warning(f"Error collecting executor metrics: {e}")
    
    def collect_streaming_metrics(self):
        """Collect Spark Streaming metrics"""
        if not self.app_id:
            return
        
        try:
            # Try to get streaming statistics
            streaming_url = f"{self.spark_ui_url}/api/v1/applications/{self.app_id}/streaming/statistics"
            response = requests.get(streaming_url, timeout=5)
            
            if response.status_code == 200:
                stats = response.json()
                
                # Extract batch duration
                if 'avgBatchDuration' in stats:
                    spark_streaming_batch_duration.set(stats['avgBatchDuration'])
                    
        except Exception as e:
            # Streaming endpoint might not exist for batch jobs
            pass
    
    def collect_metrics(self):
        """Collect all Spark metrics"""
        if not self.check_spark_availability():
            logger.warning("Spark UI not available, skipping metrics collection")
            return
        
        if not self.app_id:
            if not self.get_application_id():
                logger.warning("No Spark application found")
                return
        
        self.collect_job_metrics()
        self.collect_stage_metrics()
        self.collect_executor_metrics()
        self.collect_streaming_metrics()
    
    def run(self, port=9092, interval=15):
        """Start the metrics exporter server"""
        logger.info(f"Starting Spark metrics exporter on port {port}")
        start_http_server(port)
        
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

if __name__ == "__main__":
    exporter = SparkMetricsExporter()
    exporter.run(port=9092, interval=10)
