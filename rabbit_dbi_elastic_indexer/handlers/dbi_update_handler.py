# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '17 Feb 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'


from rabbit_indexer.index_updaters.base import UpdateHandler
from rabbit_indexer.utils.decorators import wait_for_file
from ceda_elasticsearch_tools.index_tools import CedaDirs

# Python imports
import os

# Typing imports
from rabbit_indexer.utils import PathTools
from rabbit_indexer.queue_handler.queue_handler import IngestMessage


class DirectoryUpdateHandler(UpdateHandler):
    """
    Handler to update the ceda-dirs directory index based on messages in the
    rabbit queue
    """

    def __init__(self, conf, **kwargs):
        """
        Add the index updater attribute

        :param conf: YamlConfig object
        :param kwargs: kwargs passed to setup_extra
        """
        self.index_updater = None

        super().__init__(conf, **kwargs)

    def setup_extra(self, refresh_interval: int = 30, **kwargs):
        """
        Extra setup for the class

        :param path_tools: PathTools object to provide path mapping and lookups
        :param refresh_interval: Interval in minutues before refreshing cached lookups
        :return:
        """
        super().setup_extra(refresh_interval, **kwargs)

        # Initialise the Elasticsearch connection
        self.index_updater = CedaDirs(
            index=self.conf.get('directory_index', 'name'),
            **{'headers': {
                'x-api-key': self.conf.get('elasticsearch', 'es_api_key')
            },
                'retry_on_timeout': True,
                'timeout': 30
            }
        )

    def process_event(self, message: 'IngestMessage'):
        """
        Takes the events from rabbit and sends them to the appropriate processor

        :param message: rabbitMQ message
        """

        self.logger.info(f'{message.filepath}:{message.action}')

        # Check to see if enough time has elapsed to update the mapping
        self._update_mappings()

        # Send the message to the appropriate processor method
        if message.action == 'MKDIR':
            self._process_creations(message)

        elif message.action == 'RMDIR':
            self._process_deletions(message.filepath)

        elif message.action == 'SYMLINK':
            self._process_symlinks(message)

        elif message.action == 'DEPOSIT' or message.action == 'REMOVE':
            self._process_readmes(message.filepath)

    def _process_creations(self, message: 'IngestMessage'):
        """
        Process the creation of a new directory

        :param path: Directory path
        """

        self._wait_for_file(message, wait_time=0)

        # Get the metadata
        metadata, _ = self.pt.generate_path_metadata(message.filepath)

        # Check for readmes
        if os.path.isdir(message.filepath) and metadata:
            content = self.pt.get_readme(message.filepath)

            if content:
                metadata['readme'] = content

        # Index new directory
        if metadata:
            self.index_updater.add_dirs(
                [
                    {
                        'id': self.pt.generate_id(message.filepath),
                        'document': metadata
                    }
                ]
            )
        else:
            self.logger.info(f"Path does not yet exist: {message.filepath}")

    def _process_deletions(self, path: str):
        """
        Process the deletion of a directory

        :param path: Directory path
        """

        # Delete directory
        self.index_updater.delete_dirs(
            [
                {
                    "id": self.pt.generate_id(path)
                }
            ]
        )

    def _process_symlinks(self, message: 'IngestMessage'):
        """
        Method to make it explicit what action is being
        performed but the actual code to run is the same
        as for creations.

        :param message: parsed message from RabbitMQ
        """

        self._process_creations(message)

    @wait_for_file
    def _process_readmes(self, path: str):
        """
        Process the addition of a 00README file

        :param path: Path to the readme
        """

        # Get the directory containing the 00README
        path = os.path.dirname(path)

        # Get the content of the 00README
        content = self.pt.get_readme(path)

        if content:
            self.index_updater.update_readmes(
                [
                    {
                        "id": self.pt.generate_id(path),
                        "document": {"readme": content}
                    }
                ]
            )


class FastDirectoryUpdateHandler(DirectoryUpdateHandler):

    def _process_creations(self, message: 'IngestMessage'):

        # Get the metadata
        metadata, _ = self.pt.generate_path_metadata(message.filepath)

        # Index new directory
        if metadata:
            self.index_updater.add_dirs(
                [
                    {
                        'id': self.pt.generate_id(message.filepath),
                        'document': metadata
                    }
                ]
            )
        else:
            self.index_updater.add_dirs(
                [
                    {
                        'id': self.pt.generate_id(message.filepath),
                        'document': self._generate_doc_from_message(message.filepath)
                    }
                ]
            )

    @staticmethod
    def _generate_doc_from_message(path: str) -> dict:
        """
        Generate directory document from path without checking file system attributes

        :param path: filepath
        :return: document metadata
        """
        return {
            'depth': path.count('/'),
            'path': path,
            'type': 'dir',
            'dir': os.path.basename(path)
        }
