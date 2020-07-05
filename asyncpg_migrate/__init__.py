__version__ = '0.0.7'

try:
    import uvloop
    uvloop.install()
except ImportError:
    from loguru import logger
    logger.info('uvloop is not available, skipping...')
