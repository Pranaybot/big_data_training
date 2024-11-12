from pyhive import hive
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# Connect to Hive
conn = hive.Connection(
    host='ip-172-31-8-235.eu-west-2.compute.internal',
    port=10000,
    username='ec2-user',
    database="pr_amazon_reviews_db"
)

# a) Rating Distribution with Price Insights (Bar Plot)
rating_price_query = """
SELECT f.rating, COUNT(*) as count, AVG(p.product_price) as avg_price
FROM fact_reviews_1 f
JOIN amazon_product p ON f.product_id = p.product_id
GROUP BY f.rating
"""
df_rating_price = pd.read_sql(rating_price_query, conn)

# Bar plot for Ratings Distribution with Average Price per Rating
plt.figure(figsize=(10, 6))
bar_plot = sns.barplot(x='rating', y='count', data=df_rating_price, palette='Blues_d')
plt.title('Distribution of Product Ratings with Average Price')
plt.xlabel('Rating')
plt.ylabel('Number of Reviews')

# Annotate bars with review counts and avg price
for p in bar_plot.patches:
    height = p.get_height()
    rating = int(p.get_x() + p.get_width() / 2.)
    avg_price = df_rating_price[df_rating_price['rating'] == rating]['avg_price'].values[0]
    bar_plot.annotate(f'{int(height)}\n${avg_price:.2f}', 
                      (p.get_x() + p.get_width() / 2., height), 
                      ha='center', va='bottom', fontsize=10)

plt.savefig('distribution_ratings_avg_price.png')
plt.show()

# b) Average Rating vs Price and Review Count (Bubble Chart)
avg_rating_vs_price_query = """
SELECT p.product_name, AVG(f.rating) as avg_rating, COUNT(f.product_id) as review_count, AVG(p.product_price) as avg_price
FROM fact_reviews_1 f
JOIN amazon_product p ON f.product_id = p.product_id
GROUP BY p.product_name
"""
df_avg_rating_price = pd.read_sql(avg_rating_vs_price_query, conn)

# Bubble plot showing relationship between review count, average rating, and price
plt.figure(figsize=(12, 8))
sns.scatterplot(data=df_avg_rating_price, x='review_count', y='avg_rating', size='avg_price', sizes=(20, 400), hue='avg_rating', palette='coolwarm', legend=False)
plt.title('Average Rating vs Review Count with Price as Bubble Size')
plt.xlabel('Number of Reviews')
plt.ylabel('Average Rating')
plt.axhline(y=3, color='r', linestyle='--', label='Low Satisfaction Threshold')
plt.savefig('avg_rating_vs_price_bubble.png')
plt.show()

# c) Reviews Over Time with Rating Trends (Line Plot with Annotation)
reviews_over_time_rating_query = """
SELECT d.review_date, COUNT(f.product_id) as review_count, AVG(f.rating) as avg_rating
FROM fact_reviews_1 f
JOIN review_date_table d ON f.date_id = d.date_id
GROUP BY d.review_date
ORDER BY d.review_date
"""
df_reviews_over_time_rating = pd.read_sql(reviews_over_time_rating_query, conn)

# Convert review_date to datetime for plotting
df_reviews_over_time_rating['review_date'] = pd.to_datetime(df_reviews_over_time_rating['review_date'])

# Line plot for Reviews Over Time with Rating Trends
fig, ax1 = plt.subplots(figsize=(14, 6))

ax1.plot(df_reviews_over_time_rating['review_date'], df_reviews_over_time_rating['review_count'], marker='o', color='b')
ax1.set_xlabel('Date')
ax1.set_ylabel('Number of Reviews', color='b')
ax1.tick_params('y', colors='b')

# Create secondary y-axis for Average Rating
ax2 = ax1.twinx()
ax2.plot(df_reviews_over_time_rating['review_date'], df_reviews_over_time_rating['avg_rating'], marker='x', color='g')
ax2.set_ylabel('Average Rating', color='g')
ax2.tick_params('y', colors='g')

plt.title('Reviews Over Time with Rating Trends')
plt.xticks(rotation=45)
plt.savefig('reviews_over_time_with_rating_trend.png')
plt.show()

# d) Product Clustering by Price, Ratings, and Reviews (KMeans Clustering)
df_cluster_data_query = """
SELECT p.product_name, AVG(f.rating) as avg_rating, COUNT(f.product_id) as review_count, AVG(p.product_price) as avg_price
FROM fact_reviews_1 f
JOIN amazon_product p ON f.product_id = p.product_id
GROUP BY p.product_name
"""
df_cluster_data = pd.read_sql(df_cluster_data_query, conn)

# Prepare data for clustering
X = df_cluster_data[['avg_rating', 'review_count', 'avg_price']]

# Standardize the features
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Apply KMeans Clustering
kmeans = KMeans(n_clusters=4, random_state=42)
df_cluster_data['cluster'] = kmeans.fit_predict(X_scaled)

# Plot the clusters
plt.figure(figsize=(12, 8))
sns.scatterplot(data=df_cluster_data, x='review_count', y='avg_rating', hue='cluster', size='avg_price', sizes=(20, 200), palette='Set2')
plt.title('Cluster Graph of Products Based on Ratings, Reviews, and Price')
plt.xlabel('Number of Reviews')
plt.ylabel('Average Rating')
plt.savefig('product_clusters_by_price_rating.png')
plt.show()

# e) Top 20 Users by Review Count with Location (Horizontal Bar Plot)
reviews_by_user_location_query = """
SELECT u.user_name, COUNT(f.user_id) as review_count, u.user_city, u.user_state
FROM fact_reviews_1 f
JOIN amazon_user_1 u ON f.user_id = u.user_id
GROUP BY u.user_name, u.user_city, u.user_state
ORDER BY review_count DESC
LIMIT 20
"""
df_user_review_location = pd.read_sql(reviews_by_user_location_query, conn)

# Horizontal bar plot for Top 20 Users by Number of Reviews with Location
plt.figure(figsize=(12, 8))
sns.barplot(x='review_count', y='user_name', data=df_user_review_location, color='skyblue', orient='h')
plt.title('Top 20 Users by Number of Reviews with Location')
plt.xlabel('Number of Reviews')
plt.ylabel('User Name')

# Annotate with user city and state
for i in range(len(df_user_review_location)):
    plt.text(df_user_review_location['review_count'].values[i], i, 
             f"{df_user_review_location['user_city'].values[i]}, {df_user_review_location['user_state'].values[i]}",
             ha='left', va='center', fontsize=10)

plt.savefig('top_users_by_reviews_with_location.png')
plt.show()

# Close the Hive connection
conn.close()

