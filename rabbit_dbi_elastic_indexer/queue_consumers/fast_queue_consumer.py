# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '19 Mar 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from .slow_queue_consumer import SlowDBIQueueConsumer
from rabbit_indexer.utils import PathTools
from rabbit_dbi_elastic_indexer.handlers import FastDirectoryUpdateHandler
import logging

logger = logging.getLogger(__name__)


class FastDBIQueueConsumer(SlowDBIQueueConsumer):
    """
    Replaces the directory handler with fast update handler which uses the message
    to generate the content instead of checking the file system
    """
    def get_handlers(self):
        moles_obs_map_url = self.conf.get("moles", "moles_obs_map_url")

        logger.info('Downloading spot mapping')
        path_tools = PathTools(moles_mapping_url=moles_obs_map_url)

        logger.info('Initialising directory handler')
        self.directory_handler = FastDirectoryUpdateHandler(path_tools=path_tools, conf=self.conf)