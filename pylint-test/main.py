import logging
from math_ops import divide
from data_service import fetch_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)

def main():
    try:
        data = fetch_data()
        logger.info("Fetched data: %s", data)

        result = divide(10, 2)
        logger.info("Division result: %s", result)

        # simulate an exception
        divide(5, 0)

    except ZeroDivisionError as err:
        logger.exception("Division by zero occurred: %s", err)
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
    finally:
        logger.info("Execution completed.")

if __name__ == "__main__":
    main()
