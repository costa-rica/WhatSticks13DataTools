import os
from ws_config import ConfigWorkstation, ConfigDev, ConfigProd
import logging
from logging.handlers import RotatingFileHandler

match os.environ.get('WS_CONFIG_TYPE'):
    case 'dev':
        config = ConfigDev()
        print('- WhatSticks13DataTools(ws_analysis)/config: Development')
    case 'prod':
        config = ConfigProd()
        print('- WhatSticks13DataTools(ws_analysis)/config: Production')
    case _:
        config = ConfigWorkstation()
        print('- WhatSticks13DataTools(ws_analysis)/config: Workstation')

#Setting up Logger
app_name = "WS11DataTools - ws_analysis"
# formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
formatter = logging.Formatter(f'%(asctime)s - {app_name} - %(name)s - [%(filename)s:%(lineno)d] - %(message)s')

#initialize a logger
logger_ws_analysis = logging.getLogger(__name__)
logger_ws_analysis.setLevel(logging.DEBUG)

#where do we store logging information
file_handler = RotatingFileHandler(os.path.join(config.DIR_LOGS,'ws_analysis.log'), mode='a', maxBytes=5*1024*1024,backupCount=2)
file_handler.setFormatter(formatter)

#where the stream_handler will logger_ws_analysis
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger_ws_analysis.addHandler(file_handler)
logger_ws_analysis.addHandler(stream_handler)

