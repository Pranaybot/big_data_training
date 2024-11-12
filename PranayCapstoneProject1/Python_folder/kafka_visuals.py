import matplotlib.pyplot as plt
import pandas as pd
import json
from datetime import datetime

# Load producer and consumer data from files
try:
    with open('producer_data.json', 'r') as f:
        producer_data = json.load(f)

    with open('consumer_data.json', 'r') as f:
        consumer_data = json.load(f)
except Exception as e:
    print(f"Error loading data: {e}")
    exit()

# Convert timestamps to datetime objects
producer_timestamps = [datetime.strptime(ts, '%Y-%m-%d %H:%M:%S') for ts in producer_data['producer_timestamps']]
consumer_timestamps = [datetime.strptime(ts, '%Y-%m-%d %H:%M:%S') for ts in consumer_data['consumer_timestamps']]
ratings = producer_data['ratings']
message_sizes = producer_data['message_sizes']
time_lags = consumer_data['time_lags']

# 1. Ratings Distribution (Histogram)
plt.figure(figsize=(10, 5))
plt.hist(ratings, bins=range(1, 7), edgecolor='black', color='blue', alpha=0.7)
plt.title('Product Ratings Distribution')
plt.xlabel('Ratings')
plt.ylabel('Frequency')
plt.xticks([1, 2, 3, 4, 5])
plt.grid(True)
plt.savefig('ratings_distribution.png')
plt.show()

# 2. Review Volume by Time (Line Chart)
producer_times = pd.DataFrame({'Produced': producer_timestamps})
consumer_times = pd.DataFrame({'Consumed': consumer_timestamps})

plt.figure(figsize=(10, 5))
plt.plot(producer_times.index, producer_times['Produced'], label='Reviews Produced', color='blue')
plt.plot(consumer_times.index, consumer_times['Consumed'], label='Reviews Consumed', color='red')
plt.title('Reviews Produced vs Consumed Over Time')
plt.xlabel('Message Index')
plt.ylabel('Timestamps')
plt.legend()
plt.grid(True)
plt.savefig('reviews_produced_vs_consumed.png')
plt.show()

# 3. Time Lag Between Production and Consumption (Scatter Plot)
plt.figure(figsize=(10, 5))
plt.scatter(range(len(time_lags)), time_lags, color='purple')
plt.title('Time Lag Between Production and Consumption of Reviews')
plt.xlabel('Message Index')
plt.ylabel('Time Lag (seconds)')
plt.grid(True)
plt.savefig('time_lag_scatter_plot.png')
plt.show()

# 4. Message Size Distribution (Box Plot)
plt.figure(figsize=(10, 5))
plt.boxplot(message_sizes, vert=False)
plt.title('Message Size Distribution')
plt.xlabel('Message Size (Characters)')
plt.grid(True)
plt.savefig('message_size_boxplot.png')
plt.show()

# 5. Ratings by Time (Bar Chart)
reviews_df = pd.DataFrame({
    'timestamp': producer_timestamps,
    'rating': ratings
})

ratings_by_time = reviews_df.groupby(reviews_df['timestamp'].dt.minute)['rating'].mean()

plt.figure(figsize=(10, 5))
ratings_by_time.plot(kind='bar', color='green')
plt.title('Average Ratings Over Time (By Minute)')
plt.xlabel('Minute')
plt.ylabel('Average Rating')
plt.grid(True)
plt.savefig('average_ratings_by_time.png')
plt.show()

