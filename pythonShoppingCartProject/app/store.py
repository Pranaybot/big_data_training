import mysql.connector
from config import DATABASE_CONFIG

class Store:
    def __init__(self):
        self.connection = mysql.connector.connect(**DATABASE_CONFIG)
        self.cursor = self.connection.cursor()
        self.create_items_table()
        self.insert_initial_data()

    def create_items_table(self):
        """Create the items table if it doesn't exist."""
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS items (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                quantity INT,
                cost DECIMAL(10, 2)
            )
        """)
        self.connection.commit()

    def insert_initial_data(self):
        """Insert initial data into the items table if it's empty."""
        self.cursor.execute("SELECT COUNT(*) FROM items")
        count = self.cursor.fetchone()[0]

        if count == 0:
            # Insert some sample data
            items = [
                ("Apple", 50, 10.0),
                ("Banana", 100, 5.0),
                ("Milk", 30, 50.0)
            ]
            self.cursor.executemany(
                "INSERT INTO items (name, quantity, cost) VALUES (%s, %s, %s)",
                items
            )
            self.connection.commit()

    def display_menu(self):
        """Display the menu of available items"""
        self.cursor.execute("SELECT id, name, quantity, cost FROM items")
        items = self.cursor.fetchall()
        print("\nAvailable Items:\n")
        for item in items:
            print(f"ID: {item[0]} | Name: {item[1]} | Qty: {item[2]} | Cost: {item[3]} Rs")

    def check_item_availability(self, item_id, quantity):
        """Check if an item is available in the required quantity"""
        self.cursor.execute("SELECT quantity FROM items WHERE id = %s", (item_id,))
        result = self.cursor.fetchone()
        return result is not None and result[0] >= quantity

    def update_item_quantity(self, item_id, quantity):
        """Update the quantity of an item in the database"""
        self.cursor.execute("UPDATE items SET quantity = quantity - %s WHERE id = %s", (quantity, item_id))
        self.connection.commit()

    def get_item_details(self, item_id):
        """Retrieve item details based on item ID"""
        self.cursor.execute("SELECT name, cost FROM items WHERE id = %s", (item_id,))
        result = self.cursor.fetchone()
        if result:
            return {'name': result[0], 'cost': result[1]}
        return None

    def calculate_delivery_charges(self, distance):
        """Calculate delivery charges based on distance"""
        if distance <= 10:
            return 50
        elif distance <= 20:
            return 100
        elif distance <= 30:
            return 150
        else:
            return None  # No delivery available for distances greater than 30 km

    def close_connection(self):
        """Close the database connection"""
        self.cursor.close()
        self.connection.close()
