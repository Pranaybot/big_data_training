import unittest
from datetime import datetime
from unittest import mock
from unittest.mock import patch, MagicMock
import boto3
from botocore.stub import Stubber
import json

# Import functions to be tested
from prProject2IncrementalLoad import (
    get_db_connection, get_last_loaded_timestamp, write_data_to_s3
)


class TestDataLoading(unittest.TestCase):

    @patch('prProject2IncrementalLoad.psycopg2.connect')
    def test_get_db_connection(self, mock_connect):
        conn = MagicMock()
        mock_connect.return_value = conn
        self.assertEqual(get_db_connection(), conn)

    @patch('prProject2IncrementalLoad.get_db_connection')
    @patch('prProject2IncrementalLoad.psycopg2.connect')
    def test_get_last_loaded_timestamp(self, mock_connect, mock_get_db_connection):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        self.assertEqual(get_last_loaded_timestamp(mock_cursor), "1970-01-01 00:00:00")

    @mock.patch('boto3.client')
    def test_write_data_to_s3(self, mock_boto_client):
        s3_client = mock_boto_client.return_value
        stubber = Stubber(s3_client)

        list_objects_response = {
            'Contents': [
                {
                    'Key': 'path/to/your/most_recent_file.parquet',
                    'LastModified': datetime(2024, 1, 1),
                    'Size': 12345,
                }
            ],
            'IsTruncated': False,
        }

        stubber.add_response('list_objects_v2', list_objects_response,
                             {'Bucket': 'prproject2bucket'})
        stubber.activate()

        # Sample data
        data = [{'col1': 'value1', 'col2': 'value2'}]
        colnames = ['col1', 'col2']

        # Call the function that writes data to S3
        write_data_to_s3(data, colnames)

        # Deactivate the stubber after the test
        stubber.deactivate()


if __name__ == '__main__':
    unittest.main()
