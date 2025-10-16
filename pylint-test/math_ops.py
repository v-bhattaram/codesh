import logging

logger = logging.getLogger(__name__)

def divide(a, b):
    """Divide two numbers with safe error handling."""
    if b == 0:
        logger.error("Attempted to divide by zero.")
        raise ZeroDivisionError("Division by zero is not allowed.")
    return a / b
