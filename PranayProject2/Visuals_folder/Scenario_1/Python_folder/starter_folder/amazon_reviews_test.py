import csv
import random
from datetime import datetime

# Load the dataset from a CSV file
def load_dataset(file_path):
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        return [row for row in reader]

# Data Completeness Test
def test_data_completeness(data):
    for row in data:
        if any(value == '' for value in row.values()):
            print(f"Missing data in row: {row}")

# Data Type Test
def test_data_types(data):
    for row in data:
        try:
            assert isinstance(int(row['review_id']), int)
            assert isinstance(float(row['product_price']), float)
            assert isinstance(int(row['rating']), int)
            assert isinstance(datetime.fromisoformat(row['review_date']), datetime)
            assert isinstance(datetime.fromisoformat(row['review_timestamp']), datetime)
        except (ValueError, AssertionError) as e:
            print(f"Type mismatch in row: {row}, Error: {str(e)}")

# Uniqueness Test
def test_uniqueness(data):
    seen_ids = set()
    for row in data:
        review_id = row['review_id']
        if review_id in seen_ids:
            print(f"Duplicate review_id found: {review_id}")
        seen_ids.add(review_id)

# Range Test
def test_value_ranges(data):
    for row in data:
        if not (1 <= int(row['rating']) <= 5):
            print(f"Invalid rating in row: {row}")
        if not (20 <= float(row['product_price']) <= 500):
            print(f"Invalid product price in row: {row}")

# Format Test
def test_date_format(data):
    for row in data:
        try:
            datetime.fromisoformat(row['review_date'])
            datetime.fromisoformat(row['review_timestamp'])
        except ValueError:
            print(f"Invalid date format in row: {row}")

# Random Sampling Test
def random_sampling_test(data, sample_size=10):
    sample = random.sample(data, sample_size)
    print("Random Sample Test:")
    for row in sample:
        print(row)

# Statistical Test (Simple Example)
def statistical_analysis(data):
    ratings = [int(row['rating']) for row in data]
    avg_rating = sum(ratings) / len(ratings)
    print(f"Average Rating: {avg_rating:.2f}")

# Main Testing Function
def main():
    file_path = "C:/Amazon/raw/amazon_reviews.csv"
    data = load_dataset(file_path)

    print("Starting Tests...\n")
    test_data_completeness(data)
    test_data_types(data)
    test_uniqueness(data)
    test_value_ranges(data)
    test_date_format(data)
    random_sampling_test(data)
    statistical_analysis(data)
    print("Testing Completed.")

if __name__ == '__main__':
    main()
