import unittest
import pandas as pd
from datetime import datetime

from prProject2DimensionalModeling import (
    create_fact_table, create_product_dimension, create_user_dimension, create_date_dimension)


class TestStarSchema(unittest.TestCase):

    def setUp(self):
        # Initialize the DataFrame with necessary columns and sample data
        now = datetime.now()
        self.df = pd.DataFrame({
            'review_id': [1, 2],
            'product_id': [101, 102],
            'product_name': ['Product 1', 'Product 2'],
            'product_brand': ['Brand A', 'Brand B'],
            'product_price': [99.99, 149.99],
            'user_id': [201, 202],
            'user_name': ['User 1', 'User 2'],
            'user_city': ['City A', 'City B'],
            'user_state': ['ST A', 'ST B'],
            'review_date': [now, now],
            'rating': [5, 4],
            'review_text': ['Great product!', 'Good value'],
            'review_timestamp': [now, now],
            'date_id': [1, 2],  # Assuming date_id is simply an incremental ID for test
        })

        # Extract year, month, and day from review_date
        self.df['year'] = self.df['review_date'].dt.year
        self.df['month'] = self.df['review_date'].dt.month
        self.df['day'] = self.df['review_date'].dt.day

    def test_create_fact_table(self):
        fact_df = create_fact_table(self.df)
        self.assertTrue('product_id' in fact_df.columns)
        self.assertTrue('user_id' in fact_df.columns)
        self.assertTrue('date_id' in fact_df.columns)
        self.assertTrue('rating' in fact_df.columns)
        self.assertTrue('review_text' in fact_df.columns)
        self.assertTrue('review_timestamp' in fact_df.columns)

    def test_create_product_dimension(self):
        product_dim_df = create_product_dimension(self.df)
        self.assertTrue('product_id' in product_dim_df.columns)
        self.assertTrue('product_name' in product_dim_df.columns)
        self.assertTrue('product_brand' in product_dim_df.columns)
        self.assertTrue('product_price' in product_dim_df.columns)

    def test_create_user_dimension(self):
        user_dim_df = create_user_dimension(self.df)
        self.assertTrue('user_id' in user_dim_df.columns)
        self.assertTrue('user_name' in user_dim_df.columns)
        self.assertTrue('user_city' in user_dim_df.columns)
        self.assertTrue('user_state' in user_dim_df.columns)

    def test_create_date_dimension(self):
        # Create the date dimension using the provided DataFrame
        date_dim_df = create_date_dimension(self.df)

        # Check that the date dimension has the correct columns
        self.assertTrue('date_id' in date_dim_df.columns)
        self.assertTrue('review_date' in date_dim_df.columns)
        self.assertTrue('year' in date_dim_df.columns)
        self.assertTrue('month' in date_dim_df.columns)
        self.assertTrue('day' in date_dim_df.columns)

        # Check that the values in the date dimension are correct
        self.assertEqual(date_dim_df['year'].iloc[0], self.df['year'].iloc[0])
        self.assertEqual(date_dim_df['month'].iloc[0], self.df['month'].iloc[0])
        self.assertEqual(date_dim_df['day'].iloc[0], self.df['day'].iloc[0])

        # You can also check the number of unique dates if applicable
        self.assertEqual(date_dim_df['review_date'].nunique(), len(self.df['review_date'].unique()))

    # Additional tests for upload operations can be added.


if __name__ == '__main__':
    unittest.main()
