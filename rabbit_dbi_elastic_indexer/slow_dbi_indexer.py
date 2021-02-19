# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '17 Feb 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from rabbit_indexer.queue_handler import QueueHandler
from rabbit_indexer.utils import PathTools
from rabbit_indexer.utils.consumer_setup import consumer_setup

from rabbit_dbi_elastic_indexer.handlers.dbi_update_handler import DirectoryUpdateHandler

import logging
import functools

logger = logging.getLogger()


class SlowDBIQueueConsumer(QueueHandler):
    """
    DBI queue consumer. This relies on the item being on the filesystem but
    directory events are far less frequent than files so it is OK if this process
    waits.
    """

    def get_handlers(self):
        moles_obs_map_url = self.conf.get("moles", "moles_obs_map_url")

        path_tools = PathTools(moles_mapping_url=moles_obs_map_url)

        self.directory_handler = DirectoryUpdateHandler(path_tools=path_tools, conf=self.conf)

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

        try:

            message = self.decode_message(body)

        except IndexError:
            # Acknowledge message
            cb = functools.partial(self._acknowledge_message, ch, method.delivery_tag)
            connection.add_callback_threadsafe(cb)
            return

        try:

            # Process README additions
            if message.action in ['DEPOSIT', 'REMOVE']:
                if message.filepath.endswith('00README'):
                    self.directory_handler.process_event(message)

            elif message.action in ['MKDIR', 'RMDIR', 'SYMLINK']:
                self.directory_handler.process_event(message)

            # Acknowledge message
            cb = functools.partial(self._acknowledge_message, ch, method.delivery_tag)
            connection.add_callback_threadsafe(cb)

        except Exception as e:
            # Catch all exceptions in the scanning code and log them
            logger.error(f'Error occurred while scanning: {message}', exc_info=e)
            raise


def main():
    consumer_setup(SlowDBIQueueConsumer, logger)


if __name__ == '__main__':
    main()