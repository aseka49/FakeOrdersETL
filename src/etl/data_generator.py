from faker import Faker
import csv
import random

fake = Faker('ru_RU')


# --- Users ---
def generate_users(N, filepath):
    data = []
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['user_id', 'name', 'city', 'signup_date'])

        for user_id in range(1, N + 1):
            row = [
                user_id,
                fake.name(),
                fake.city(),
                fake.date_between(start_date='-2y', end_date='today')
            ]
            writer.writerow(row)
            data.append(row)
    return data


# --- Restaurants ---
def generate_restaurants(filepath):
    data = []
    restaurant_names = ['Zhyenpok', 'Papa Anuar', 'Rakhatoli', 'ZloyBucks',
                        'SayAssiya', 'SojuBek', 'Alina Khan', 'Arnurio',
                        'Talant', "Adilet's tea", 'Zhaniques', "Daniel's"]

    cuisines = ['Italian', 'Japanese', 'American', 'Korean',
                'Russian', 'Kazakh', 'French']

    brand_to_cuisine = {name: random.choice(cuisines) for name in restaurant_names}
    brand_base_rating = {name: round(random.uniform(3.5, 4.8), 1) for name in restaurant_names}
    restaurant_id_to_cuisine = {}

    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['restaurant_id', 'name', 'city', 'cuisine', 'rating'])

        rid = 1
        for name in restaurant_names:
            for _ in range(random.randint(2, 3)):
                cuisine = brand_to_cuisine[name]
                rating = round(brand_base_rating[name] + random.uniform(-0.2, 0.2), 1)
                rating = min(max(rating, 4.1), 5.0)

                row = [rid, name, fake.city(), cuisine, rating]
                writer.writerow(row)
                data.append(row)
                restaurant_id_to_cuisine[rid] = cuisine
                rid += 1

    return data, restaurant_id_to_cuisine


# --- Dishes ---
def generate_dishes(restaurant_id_to_cuisine, filepath):
    dishes = {
        "Italian": ["Pizza", "Risotto", "Carbonara", "Tiramisu", 'Wine'],
        "Japanese": ["Sushi", "Ramen", "Wagyu", "Mochi", 'Sake'],
        "American": ["Burger", "Barbeque", "Fried Chicken", "Cupcake", "Root Beer"],
        "Korean": ["Bibimbap", "Bulgogi", "Gimbap", "Kimchi", "Soju"],
        "Russian": ["Beef Stroganoff", "Shashlik", "Vinegret", "Sharlotka", "Kissel"],
        "Kazakh": ["Qazy", "Baursak", "Kespe", "Beshbarmak", "Kymyz"],
        "French": ["Croissant", "Baquette", "Macarons", "Creme Brule", "Champagne"],
    }

    data = []
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["dish_id", "restaurant_id", "name", "price"])

        dish_id = 1
        for rid, cuisine in restaurant_id_to_cuisine.items():
            for _ in range(5):
                dish = random.choice(dishes[cuisine])
                price = random.randint(2000, 3000)
                row = [dish_id, rid, dish, price]
                writer.writerow(row)
                data.append(row)
                dish_id += 1
    return data

