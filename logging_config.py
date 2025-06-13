import os
import sys
from loguru import logger
import functools
import time

def custom_format(record):
    # Convert the file path to a relative path
    rel_file = os.path.relpath(record["file"].path)
    # Build a format string with time, level, relative file, line number, and message.
    return (
        f"{{time:YYYY-MM-DD HH:mm:ss}} | {{level}} | {rel_file}:{{line}} - {{message}}\n"
    )

def setup_logging(log_level='DEBUG'):
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    log_file_path = os.path.join(log_dir, "app.log")
    log_file_jsonl = os.path.join(log_dir, "app.jsonl")
    error_log_path = os.path.join(log_dir, "error.log")
    error_log_jsonl = os.path.join(log_dir, "error.jsonl")

    logger.remove()

    # Console logging with custom format
    logger.add(
        sys.stdout,
        format=custom_format,
        level=log_level,
        enqueue=True,
        backtrace=True,
        diagnose=True
    )

    # Main application log with rotation, retention, and compression
    logger.add(
        log_file_path,
        format=custom_format,
        level=log_level,
        rotation="50 MB",      # Smaller rotation size for better performance
        retention="10 days",   # Keep logs a bit longer
        compression="zip",
        enqueue=True,
        backtrace=True,
        diagnose=False  # Don't include sensitive info in files
    )
    
    # Separate error log for easier debugging
    logger.add(
        error_log_path,
        format=custom_format,
        level="ERROR",
        rotation="25 MB",      # Smaller for error logs
        retention="30 days",   # Keep error logs longer
        compression="zip",
        enqueue=True,
        backtrace=True,
        diagnose=True   # Include more info for errors
    )

    # JSON structured logs for log analysis tools
    logger.add(
        log_file_jsonl,
        level=log_level,
        rotation="75 MB",      # Optimized size
        retention="7 days",    # Standard retention
        compression="zip",
        serialize=True,
        enqueue=True,
        backtrace=False,       # JSON logs don't need backtrace
        diagnose=False
    )
    
    # JSON error logs for structured error analysis
    logger.add(
        error_log_jsonl,
        level="ERROR",
        rotation="50 MB",
        retention="21 days",   # Keep error analysis data longer
        compression="zip",
        serialize=True,
        enqueue=True,
        backtrace=False,
        diagnose=False
    )
    
    logger.info(f"Logging initialized with level: {log_level}")
    logger.info(f"Log directory: {os.path.abspath(log_dir)}")

def log_function_execution(func):
    """Decorator to log function execution with timing"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.debug(f"Starting execution of {func.__name__}")
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.debug(f"Completed {func.__name__} in {execution_time:.2f}s")
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Error in {func.__name__} after {execution_time:.2f}s: {str(e)}")
            raise
    return wrapper

def log_with_context(level="info", context=None):
    """Log with additional context information"""
    def log_message(message):
        context_str = f" [{context}]" if context else ""
        full_message = f"{message}{context_str}"
        getattr(logger, level)(full_message)
    return log_message