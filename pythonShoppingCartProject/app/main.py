from store import Store

def main():
    store = Store()

    try:
        store.display_menu()

        cart = []
        total_cost = 0

        while True:
            try:
                item_id = int(input("\nEnter the item ID you want to purchase (0 to finish): "))
                if item_id == 0:
                    break
                quantity = int(input("Enter quantity: "))

                if store.check_item_availability(item_id, quantity):
                    store.update_item_quantity(item_id, quantity)
                    item = store.get_item_details(item_id)
                    item_cost = item['cost'] * quantity
                    cart.append((item['name'], quantity, item_cost))
                    total_cost += item_cost
                    print(f"{item['name']} added to cart.")
                else:
                    print("Sorry, item is out of stock or insufficient quantity.")
            except ValueError:
                print("Invalid input. Please enter a valid item ID or quantity.")

        name = input("\nEnter your name: ")
        address = input("Enter your address: ")
        try:
            distance = float(input("Enter distance from store in km: "))
        except ValueError:
            print("Invalid input for distance.")
            return

        delivery_charges = store.calculate_delivery_charges(distance)

        if delivery_charges is not None:
            total_cost += delivery_charges
            print(f"\nDelivery Charges: {delivery_charges} Rs")
        else:
            print("\nNo delivery available for this distance.")

        print("\nFinal Bill:")
        print(f"Customer Name: {name}")
        print(f"Delivery Address: {address}")
        print("\nItems Purchased:")
        for item in cart:
            print(f"{item[0]}: {item[1]} Qty, Total: {item[2]} Rs")

        print(f"Total Cost: {total_cost} Rs")

    finally:
        store.close_connection()

if __name__ == "__main__":
    main()
