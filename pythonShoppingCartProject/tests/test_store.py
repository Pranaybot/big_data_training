import unittest
from unittest.mock import patch
from app.store import Store


class TestStore(unittest.TestCase):

    def setUp(self):
        self.store = Store()

    def test_calculate_delivery_charges(self):
        self.assertEqual(self.store.calculate_delivery_charges(10), 50)
        self.assertEqual(self.store.calculate_delivery_charges(20), 100)
        self.assertIsNone(self.store.calculate_delivery_charges(35))

    def test_check_item_availability(self):
        with patch('app.store.Store.cursor') as mock_cursor:
            # Simulate item availability
            mock_cursor.fetchone.return_value = (10,)  # Simulating 10 items in stock
            self.assertTrue(self.store.check_item_availability(1, 5))  # 5 items requested
            mock_cursor.fetchone.return_value = (2,)  # Simulating only 2 items in stock
            self.assertFalse(self.store.check_item_availability(1, 5))  # 5 items requested but only 2 available

    @patch('builtins.print')
    def test_display_menu(self, mock_print):
        # Simulate items in the store
        with patch('app.store.Store.cursor') as mock_cursor:
            mock_cursor.fetchall.return_value = [
                (1, 'Apple', 50, 10.0),
                (2, 'Banana', 100, 5.0),
                (3, 'Milk', 30, 50.0)
            ]
            self.store.display_menu()

            # Assert that the correct menu output was printed
            mock_print.assert_any_call("\nAvailable Items:\n")
            mock_print.assert_any_call("ID: 1 | Name: Apple | Qty: 50 | Cost: 10.0 Rs")
            mock_print.assert_any_call("ID: 2 | Name: Banana | Qty: 100 | Cost: 5.0 Rs")
            mock_print.assert_any_call("ID: 3 | Name: Milk | Qty: 30 | Cost: 50.0 Rs")

    def test_insert_initial_data(self):
        with patch('app.store.Store.cursor') as mock_cursor:
            # Simulate the items table being empty initially
            mock_cursor.fetchone.return_value = (0,)

            # Call the method to insert initial data
            self.store.insert_initial_data()

            # Verify that initial data was inserted
            mock_cursor.executemany.assert_called_once_with(
                "INSERT INTO items (name, quantity, cost) VALUES (%s, %s, %s)",
                [
                    ("Apple", 50, 10.0),
                    ("Banana", 100, 5.0),
                    ("Milk", 30, 50.0)
                ]
            )

    def test_get_item_details(self):
        with patch('app.store.Store.cursor') as mock_cursor:
            # Simulate item details for item with ID 1
            mock_cursor.fetchone.return_value = ('Apple', 10.0)
            item_details = self.store.get_item_details(1)

            # Assert that the correct item details were returned
            self.assertEqual(item_details, {'name': 'Apple', 'cost': 10.0})

            # Simulate no item found for invalid ID
            mock_cursor.fetchone.return_value = None
            item_details = self.store.get_item_details(99)
            self.assertIsNone(item_details)

    def tearDown(self):
        self.store.close_connection()


if __name__ == "__main__":
    unittest.main()
