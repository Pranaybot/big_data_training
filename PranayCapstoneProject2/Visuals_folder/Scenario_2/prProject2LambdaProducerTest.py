import unittest
from unittest.mock import patch
import json
import random
import string
import datetime
import prProject2LambdaProducer # Replace with the actual module name

class TestReviewDataFunctions(unittest.TestCase):

    def test_generate_product_price(self):
        price = prProject2LambdaProducer.generate_product_price()
        self.assertGreaterEqual(price, 20)
        self.assertLessEqual(price, 500)
        self.assertIsInstance(price, float)

    def test_generate_product_category(self):
        category = prProject2LambdaProducer.generate_product_category()
        self.assertIn(category, prProject2LambdaProducer.product_categories)

    def test_generate_product_brand(self):
        brand = prProject2LambdaProducer.generate_product_brand()
        self.assertIn(brand, prProject2LambdaProducer.product_brands)

    def test_generate_user_location(self):
        city_state = prProject2LambdaProducer.generate_user_location()
        self.assertIn(city_state, prProject2LambdaProducer.us_cities_states)

    def test_generate_user_name(self):
        name = prProject2LambdaProducer.generate_user_name()
        self.assertIn(name, prProject2LambdaProducer.user_names)

    def test_generate_review(self):
        review = prProject2LambdaProducer.generate_review()
        self.assertIn(review, prProject2LambdaProducer.reviews)

    @patch('prProject2LambdaProducer.datetime')
    def test_create_review_message(self, mock_datetime):
        mock_datetime.datetime.utcnow.return_value = datetime.datetime(2024, 1, 1, 12, 0, 0)
        last_city_state = ("New York", "NY")
        product_string = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        user_string = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        date_string = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))

        message = prProject2LambdaProducer.create_review_message(product_string, user_string, date_string, last_city_state)
        data = json.loads(message.decode('utf-8'))

        self.assertEqual(data["product_id"], product_string)
        self.assertEqual(data["user_id"], user_string)
        self.assertEqual(data["date_id"], date_string)
        self.assertIn(data["product_name"], prProject2LambdaProducer.product_categories)
        self.assertIn(data["product_brand"], prProject2LambdaProducer.product_brands)
        self.assertGreaterEqual(data["product_price"], 20)
        self.assertLessEqual(data["product_price"], 500)
        self.assertIn(data["user_name"], prProject2LambdaProducer.user_names)
        self.assertNotEqual((data["user_city"], data["user_state"]), last_city_state)
        self.assertIn(data["review_text"], prProject2LambdaProducer.reviews)
        self.assertEqual(data["review_timestamp"], "2024-01-01T12:00:00")

    @patch('prProject2LambdaProducer.kinesis_client.put_record')
    def test_lambda_handler(self, mock_put_record):
        mock_put_record.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
        event = {}
        context = {}

        result = prProject2LambdaProducer.lambda_handler(event, context)

        self.assertEqual(result['statusCode'], 200)
        self.assertEqual(result['body'], json.dumps('Messages sent to Kinesis Stream and JSON saved to S3 successfully!'))
        self.assertEqual(mock_put_record.call_count, 50)

if __name__ == '__main__':
    unittest.main()
