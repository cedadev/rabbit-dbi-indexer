# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '19 Mar 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from rabbit_indexer.queue_handler import QueueHandler

from rabbit_dbi_elastic_indexer.handlers import DirectoryUpdateHandler, FastDirectoryUpdateHandler

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class DBIQueueConsumer(QueueHandler):
    """
    Provides the callback for the DBI
    """
    def callback(self, ch, method, properties, body, connection):
        """
        Callback to run during basic consume routine.
        Arguments provided by pika standard message callback method

        :param ch: Channel
        :param method: pika method
        :param properties: pika header properties
        :param body: Message body
        :param connection: Pika connection
        """
        
        logger.info(f'Processing message: {body}')

        try:

            message = self.decode_message(body)

        except IndexError as e:
            # Acknowledge message
            logger.warning(f'Error reading message: {e}')
            self.acknowledge_message(ch, method.delivery_tag, connection)
            return
        
        logger.info(f'decoded message: {message}')

        try:

            # Process README additions
            if message.action in ['DEPOSIT', 'REMOVE']:
                if message.filepath.endswith('00README'):
                    self.queue_handler.process_event(message)

            elif message.action in ['MKDIR', 'RMDIR', 'SYMLINK']:
                self.queue_handler.process_event(message)

            # Acknowledge message
            self.acknowledge_message(ch, method.delivery_tag, connection)

        except Exception as e:
            # Catch all exceptions in the scanning code and log them
            logger.error(f'Error occurred while scanning: ', exc_info=e)
            raise


class SlowDBIQueueConsumer(DBIQueueConsumer):
    """
    DBI queue consumer. This relies on the item being on the filesystem but
    directory events are far less frequent than files so it is OK if this process
    waits.
    """
    HANDLER_CLASS = DirectoryUpdateHandler


class FastDBIQueueConsumer(DBIQueueConsumer):
    """
    Replaces the directory handler with fast update handler which uses the message
    to generate the content instead of checking the file system
    """

    HANDLER_CLASS = FastDirectoryUpdateHandler
