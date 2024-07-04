from .config_and_logger import config, logger_ws_utilities

################################################################################################
# Probably should not use this. Instead, pass in db_session (from app that called ws_utilities)
# that needs operation done and return db_session to source app to commit/rollback and close
################################################################################################
def wrap_up_session(db_session):
    logger_ws_utilities.info("- accessed wrap_up_session -")
    try:
        # perform some database operations
        db_session.commit()
        logger_ws_utilities.info("- perfomed: db_session.commit() -")
    except Exception as e:
        logger_ws_utilities.info(f"{type(e).__name__}: {e}")
        db_session.rollback()  # Roll back the transaction on error
        logger_ws_utilities.info("- perfomed: db_session.rollback() -")
        raise
    finally:
        db_session.close()  # Ensure the session is closed in any case
        logger_ws_utilities.info("- perfomed: db_session.close() -")