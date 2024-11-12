import unittest
import pandas as pd
from io import BytesIO
import boto3
from botocore.stub import Stubber
from datetime import datetime

# Import functions to be tested
from prProject2PreProcessing import (
    clean_text_columns, filter_invalid_rows, remove_duplicates)


class TestDataProcessing(unittest.TestCase):

    def setUp(self):
        # Initialize boto3 S3 client
        self.s3_client = boto3.client('s3', region_name='us-east-1')

        # Stubber setup
        self.stubber = Stubber(self.s3_client)

        # Mock responses for S3 API calls
        self.stubber.add_response('create_bucket', {}, {'Bucket': 'test-bucket'})
        self.stubber.add_response('put_object', {}, {
            'Bucket': 'test-bucket',
            'Key': 'test_path/test.parquet',
            'Body': b'dummy_parquet_data'
        })
        self.stubber.activate()

        # Prepare mock Parquet data
        self.data = pd.DataFrame(
            {'product_name': ['Product 1'], 'user_name': ['User 1'], 'review_text': ['Good product'], 'rating': [5],
             'review_date': [datetime.now()]}
        )
        self.buffer = BytesIO()
        self.data.to_parquet(self.buffer, index=False)

        # Upload mock Parquet data to S3
        self.s3_client.put_object(Bucket='test-bucket', Key='test_path/test.parquet', Body=self.buffer.getvalue())

    def tearDown(self):
        self.stubber.deactivate()

    def test_clean_text_columns(self):
        df = pd.DataFrame(
            {'product_name': [' Product 1 '], 'user_name': [' User 1 '], 'review_text': [' Good product ']}
        )
        cleaned_df = clean_text_columns(df)
        self.assertEqual(cleaned_df['product_name'][0], 'Product 1')

    def test_filter_invalid_rows(self):
        df = pd.DataFrame(
            {'product_name': [None, 'Product 2'], 'user_name': ['User 1', ''], 'review_text': ['Good', '']}
        )
        filtered_df = filter_invalid_rows(df)
        self.assertEqual(len(filtered_df), 0)

    def test_remove_duplicates(self):
        df = pd.DataFrame(
            {'user_name': ['User 1', 'User 1'], 'product_name': ['Product 1', 'Product 1'],
             'review_text': ['Good product', 'Good product']}
        )
        deduped_df = remove_duplicates(df)
        self.assertEqual(len(deduped_df), 1)


if __name__ == '__main__':
    unittest.main()
