import csv
import random
from faker import Faker
from datetime import datetime, timedelta

# Initialize Faker to generate random names, dates, product names, etc.
fake = Faker()

# Define constants
NUM_ROWS = 10000
OUTPUT_FILE = 'C:/Users/Pranay Pentarparthy/Documents/amazon_reviews.csv'

# Predefined list of product categories and brands
product_categories = [
    "Wireless Mouse", "Laptop Stand", "Smartphone Case", "Bluetooth Speaker",
    "Noise Cancelling Headphones", "Smartwatch", "Gaming Keyboard",
    "External Hard Drive", "Fitness Tracker", "USB-C Hub"
]

product_brands = [
    "TechCo", "GadgetWorld", "DeviceMakers", "NextGen", "ProTech",
    "Innovate", "SmartGear", "FutureTech", "GizmoCorp", "ElectroHub"
]

# State abbreviations
states = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
    "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA",
    "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]

# Predefined list of US cities and states
us_cities_states = [
    ("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL"), ("Houston", "TX"),
    ("Phoenix", "AZ"), ("Philadelphia", "PA"), ("San Antonio", "TX"), ("San Diego", "CA"),
    ("Dallas", "TX"), ("San Jose", "CA"), ("Austin", "TX"), ("Jacksonville", "FL"),
    ("Fort Worth", "TX"), ("Columbus", "OH"), ("Charlotte", "NC"), ("Indianapolis", "IN"),
    ("San Francisco", "CA"), ("Seattle", "WA"), ("Denver", "CO"), ("Washington", "DC"),
    ("Boston", "MA"), ("El Paso", "TX"), ("Nashville", "TN"), ("Detroit", "MI"),
    ("Oklahoma City", "OK"), ("Portland", "OR"), ("Las Vegas", "NV"), ("Louisville", "KY"),
    ("Baltimore", "MD"), ("Milwaukee", "WI"), ("Albuquerque", "NM"), ("Tucson", "AZ"),
    ("Fresno", "CA"), ("Sacramento", "CA"), ("Mesa", "AZ"), ("Atlanta", "GA"),
    ("Kansas City", "MO"), ("Colorado Springs", "CO"), ("Miami", "FL"), ("Raleigh", "NC"),
    ("Omaha", "NE"), ("Long Beach", "CA"), ("Virginia Beach", "VA"), ("Minneapolis", "MN"),
    ("Tulsa", "OK"), ("Arlington", "TX"), ("Tampa", "FL"), ("New Orleans", "LA"),
    ("Wichita", "KS"), ("Bakersfield", "CA"), ("Cleveland", "OH"), ("Aurora", "CO"),
    ("Anaheim", "CA"), ("Honolulu", "HI"), ("Santa Ana", "CA"), ("Riverside", "CA"),
    ("Corpus Christi", "TX"), ("Lexington", "KY"), ("Henderson", "NV"), ("Stockton", "CA"),
    ("St. Louis", "MO"), ("Cincinnati", "OH"), ("Pittsburgh", "PA"), ("Greensboro", "NC")
]

# Function to generate a random time based on a given date
def random_time_on_date(review_date):
    # Generate random hours, minutes, and seconds
    random_time = timedelta(
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )
    # Add the random time to the date
    full_timestamp = datetime.combine(review_date, datetime.min.time()) + random_time
    return full_timestamp

# Function to generate a weighted random rating with time-based adjustment
def generate_time_adjusted_rating(review_year):
    """
    Returns a rating based on the year of the review. Earlier years have more
    ratings of 4 and 5, while later years have more ratings of 1, 2, and 3.
    """
    if review_year <= datetime.now().year - 3:
        # In earlier years, give higher weight to ratings 4 and 5
        return random.choices(
            population=[1, 2, 3, 4, 5],
            weights=[0.13, 0.13, 0.13, 0.305, 0.305],
            k=1
        )[0]
    elif review_year <= datetime.now().year - 2:
        # In middle years, slight shift towards lower ratings
        return random.choices(
            population=[1, 2, 3, 4, 5],
            weights=[0.3, 0.3, 0.2, 0.1, 0.1],
            k=1
        )[0]
    else:
        # In recent years, more low ratings (1, 2, 3) are likely
        return random.choices(
            population=[1, 2, 3, 4, 5],
            weights=[0.4, 0.4, 0.15, 0.025, 0.025],
            k=1
        )[0]


# Function to generate random price within a range
def generate_product_price():
    return round(random.uniform(20, 500), 2)  # Price between $20 and $500

# Function to generate random user city and state from the list of US cities
def generate_user_location():
    return random.choice(us_cities_states)

# Generate random data for the dataset, with dates in ascending order
def generate_synthetic_data(num_rows):
    data = []
    last_city_state = None  # Track the last city-state pair

    for _ in range(num_rows):
        # Random product and user details
        product_name = random.choice(product_categories)  # Random product name
        product_brand = random.choice(product_brands)  # Random product brand
        product_price = generate_product_price()  # Random product price

        user_name = fake.name()  # Random user name

        # Ensure the next city-state pair is different from the last one
        while True:
            user_city, user_state = generate_user_location()  # Choose a real US city and state
            if (user_city, user_state) != last_city_state:
                break  # Only proceed if the city-state pair is different

        # Store the current city-state pair as the last one
        last_city_state = (user_city, user_state)

        # Use Faker to generate a valid date (as a datetime.date object)
        review_date = fake.date_this_decade()

        # Check if review_date is a string and convert it to a date object if necessary
        if isinstance(review_date, str):
            review_date = datetime.strptime(review_date, '%Y-%m-%d').date()

        review_year = review_date.year  # Get the year from the review_date

        # Generate a rating based on the year of the review
        rating = generate_time_adjusted_rating(review_year)

        # Generate a random timestamp on the same day as the review_date
        review_timestamp = random_time_on_date(review_date)  # This will be a datetime object

        # Append row data to the list (review_id will be assigned later)
        data.append([
            None, product_name, user_name, rating, fake.sentence(nb_words=15),
            user_city, user_state, product_brand, product_price, review_date, review_timestamp
        ])

    # Sort data by timestamp (oldest first)
    data.sort(key=lambda x: x[-1])  # Sort by review_timestamp

    # Assign review_id in ascending order, starting from 0
    for review_id, row in enumerate(data):
        row[0] = review_id  # Assign review_id

    return data

# Function to save the data to a CSV file
def save_to_csv(data, output_file):
    header = [
        'review_id', 'product_name', 'user_name', 'rating', 'review_text',
        'user_city', 'user_state', 'product_brand', 'product_price',
        'review_date', 'review_timestamp'
    ]

    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(header)

        # Ensure datetime objects are converted to ISO format
        for row in data:
            row[-1] = row[-1].isoformat()  # Convert the review_timestamp to ISO format string
            writer.writerow(row)

    print(f"Data successfully saved to {output_file}")

# Main function to generate and save data
def main():
    data = generate_synthetic_data(NUM_ROWS)
    save_to_csv(data, OUTPUT_FILE)


if __name__ == '__main__':
    main()
