import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# Load Parquet files
df_facts = pd.read_parquet('fact_reviews_20241029133739.parquet')
df_products = pd.read_parquet('dim_product_20241029133740.parquet')
df_users = pd.read_parquet('dim_user_20241029133740.parquet')
df_dates = pd.read_parquet('dim_date_20241029133740.parquet')

# Define plotting functions (same as original)
def plot_rating_distribution(df):
    plt.figure(figsize=(10, 6))
    bar_plot = sns.barplot(x='rating', y='count', data=df, palette='Blues_d')
    plt.title('Distribution of Product Ratings with Average Price')
    plt.xlabel('Rating')
    plt.ylabel('Number of Reviews')
    for p in bar_plot.patches:
        bar_plot.annotate(f'{int(p.get_height())}',
                          (p.get_x() + p.get_width() / 2., p.get_height()),
                          ha='center', va='bottom', fontsize=10)
    plt.savefig('distribution_ratings_avg_price.png')
    plt.close()

def plot_avg_rating_vs_price(df):
    plt.figure(figsize=(12, 8))
    sns.scatterplot(data=df, x='review_count', y='avg_rating', size='avg_price', sizes=(20, 400),
                    hue='avg_rating', palette='coolwarm', legend=False)
    plt.title('Average Rating vs Review Count with Price as Bubble Size')
    plt.xlabel('Number of Reviews')
    plt.ylabel('Average Rating')
    plt.axhline(y=3, color='r', linestyle='--', label='Low Satisfaction Threshold')
    plt.savefig('avg_rating_vs_price_bubble.png')
    plt.close()

def plot_reviews_over_time(df):
    df['review_date'] = pd.to_datetime(df['review_date'])
    fig, ax1 = plt.subplots(figsize=(14, 6))
    ax1.plot(df['review_date'], df['review_count'], marker='o', color='b')
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Number of Reviews', color='b')
    ax1.tick_params('y', colors='b')
    ax2 = ax1.twinx()
    ax2.plot(df['review_date'], df['avg_rating'], marker='x', color='g')
    ax2.set_ylabel('Average Rating', color='g')
    ax2.tick_params('y', colors='g')
    plt.title('Reviews Over Time with Rating Trends')
    plt.xticks(rotation=45)
    plt.savefig('reviews_over_time_with_rating_trend.png')
    plt.close()

def plot_product_clusters(df):
    plt.figure(figsize=(12, 8))
    sns.scatterplot(data=df, x='review_count', y='avg_rating', hue='cluster', size='avg_price',
                    sizes=(20, 200), palette='Set2')
    plt.title('Cluster Graph of Products Based on Ratings, Reviews, and Price')
    plt.xlabel('Number of Reviews')
    plt.ylabel('Average Rating')
    plt.savefig('product_clusters_by_price_rating.png')
    plt.close()

def plot_top_users(df):
    plt.figure(figsize=(12, 8))
    sns.barplot(x='review_count', y='user_name', data=df, color='skyblue', orient='h')
    plt.title('Top 20 Users by Number of Reviews with Location')
    plt.xlabel('Number of Reviews')
    plt.ylabel('User Name')
    for i in range(len(df)):
        plt.text(df['review_count'].values[i], i,
                 f"{df['user_city'].values[i]}, {df['user_state'].values[i]}",
                 ha='left', va='center', fontsize=10)
    plt.savefig('top_users_by_reviews_with_location.png')
    plt.close()

# Prepare data for each visualization

# a) Rating Distribution with Price Insights
df_rating_price = df_facts.merge(df_products, on='product_id').groupby('rating').agg(
    count=('product_id', 'size'),
    avg_price=('product_price', 'mean')
).reset_index()
plot_rating_distribution(df_rating_price)

# b) Average Rating vs Price and Review Count
df_avg_rating_price = df_facts.merge(df_products, on='product_id').groupby('product_name').agg(
    avg_rating=('rating', 'mean'),
    review_count=('product_id', 'size'),
    avg_price=('product_price', 'mean')
).reset_index()
plot_avg_rating_vs_price(df_avg_rating_price)

# c) Reviews Over Time with Rating Trends
df_reviews_over_time_rating = df_facts.merge(df_dates, on='date_id').groupby('review_date').agg(
    review_count=('product_id', 'size'),
    avg_rating=('rating', 'mean')
).reset_index()
plot_reviews_over_time(df_reviews_over_time_rating)

# d) Product Clustering by Price, Ratings, and Reviews
X = df_avg_rating_price[['avg_rating', 'review_count', 'avg_price']]

# Standardize the features
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Apply KMeans Clustering
kmeans = KMeans(n_clusters=4, random_state=42)
df_avg_rating_price['cluster'] = kmeans.fit_predict(X_scaled)
plot_product_clusters(df_avg_rating_price)

# e) Top 20 Users by Review Count with Location
df_user_review_location = df_facts.merge(df_users, on='user_id').groupby(['user_name', 'user_city', 'user_state']).size().reset_index(name='review_count').nlargest(20, 'review_count')
plot_top_users(df_user_review_location)

print("Visualizations generated successfully.")
