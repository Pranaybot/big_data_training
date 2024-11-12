import pandas as pd
from sqlalchemy import create_engine

# Load the CSV file into a DataFrame
df = pd.read_csv("C:/Amazon/raw/amazon_reviews.csv")
username = 'consultants'
password = 'WelcomeItc@2022'  # Password with @
database = 'testdb'
host = 'ec2-18-132-73-146.eu-west-2.compute.amazonaws.com'

# URL-encode the password
# URL-encode the password
encoded_password = password.replace('@', '%40')

engine = create_engine(f'postgresql://{username}:{encoded_password}@{host}/{database}')

# Write the DataFrame to the database
df.to_sql('amazon_reviews', engine, if_exists='replace', index=False)  # Change 'table_name' as needed

print("Data transferred to PostgreSQL database successfully.")