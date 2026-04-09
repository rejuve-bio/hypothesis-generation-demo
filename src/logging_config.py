import os
import sys
import logging
from loguru import logger
import functools
import time

class InterceptHandler(logging.Handler):
    """
    Intercepts standard Python logging messages and routes them to Loguru.
    """
    def emit(self, record):
        # Get corresponding Loguru level if it exists
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.bind(original_name=record.name).opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )

def custom_format(record):
    if "original_name" in record["extra"]:
        name = record["extra"]["original_name"]
        return f"{{time:YYYY-MM-DD HH:mm:ss}} | {{level}} | {name} - {{message}}\n"
        
    rel_file = os.path.relpath(record["file"].path)
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

    # Console logging
    logger.add(sys.stdout, format=custom_format, level=log_level, enqueue=False, backtrace=True, diagnose=True)

    logger.add(log_file_path, format=custom_format, level=log_level, rotation="50 MB", retention="10 days", compression="zip", enqueue=False, backtrace=True, diagnose=False)
    logger.add(error_log_path, format=custom_format, level="ERROR", rotation="25 MB", retention="30 days", compression="zip", enqueue=False, backtrace=True, diagnose=True)
    logger.add(log_file_jsonl, level=log_level, rotation="75 MB", retention="7 days", compression="zip", serialize=True, enqueue=False, backtrace=False, diagnose=False)
    logger.add(error_log_jsonl, level="ERROR", rotation="50 MB", retention="21 days", compression="zip", serialize=True, enqueue=False, backtrace=False, diagnose=False)
    
    # Convert Loguru level to standard logging level
    logging_level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
    
    for logger_name in ["uvicorn", "uvicorn.access", "uvicorn.error", "fastapi"]:
        logging_logger = logging.getLogger(logger_name)
        logging_logger.handlers = [InterceptHandler()]
        logging_logger.propagate = False
        logging_logger.setLevel(logging_level)

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