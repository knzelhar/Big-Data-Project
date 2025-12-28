"""
Producteur Kafka - Génère des transactions e-commerce en temps réel
"""

from kafka import KafkaProducer
from faker import Faker
import json
import time
import random
from datetime import datetime

# Configuration
KAFKA_BROKER = 'localhost:29092'
TOPIC_NAME = 'ecommerce-transactions'

# Initialisation
fake = Faker()
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Données pour simulation réaliste
CATEGORIES = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Beauty', 'Toys']
PRODUCTS = {
    'Electronics': ['Laptop', 'Smartphone', 'Headphones', 'Tablet', 'Smartwatch'],
    'Clothing': ['T-Shirt', 'Jeans', 'Dress', 'Jacket', 'Shoes'],
    'Books': ['Novel', 'Textbook', 'Comic', 'Magazine', 'E-book'],
    'Home': ['Lamp', 'Cushion', 'Curtain', 'Rug', 'Vase'],
    'Sports': ['Ball', 'Racket', 'Bike', 'Dumbbell', 'Yoga Mat'],
    'Beauty': ['Perfume', 'Lipstick', 'Cream', 'Shampoo', 'Nail Polish'],
    'Toys': ['Doll', 'Car', 'Puzzle', 'Lego', 'Board Game']
}

PAYMENT_METHODS = ['Credit Card', 'PayPal', 'Debit Card', 'Crypto', 'Gift Card']
COUNTRIES = ['USA', 'Canada', 'UK', 'France', 'Germany', 'Spain', 'Italy']

def generate_transaction():
    """Génère une transaction e-commerce aléatoire"""
    
    category = random.choice(CATEGORIES)
    product = random.choice(PRODUCTS[category])
    quantity = random.randint(1, 5)
    
    # Prix selon la catégorie
    price_ranges = {
        'Electronics': (100, 2000),
        'Clothing': (20, 150),
        'Books': (10, 50),
        'Home': (15, 200),
        'Sports': (25, 500),
        'Beauty': (10, 100),
        'Toys': (15, 80)
    }
    
    unit_price = round(random.uniform(*price_ranges[category]), 2)
    total_amount = round(unit_price * quantity, 2)
    
    transaction = {
        'transaction_id': fake.uuid4(),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'customer_id': f"CUST_{random.randint(1000, 9999)}",
        'customer_name': fake.name(),
        'email': fake.email(),
        'country': random.choice(COUNTRIES),
        'city': fake.city(),
        'category': category,
        'product': product,
        'quantity': quantity,
        'unit_price': unit_price,
        'total_amount': total_amount,
        'payment_method': random.choice(PAYMENT_METHODS),
        'status': random.choices(['completed', 'pending', 'failed'], weights=[0.85, 0.10, 0.05])[0]
    }
    
    return transaction

def main():
    """Fonction principale - Envoie des transactions en continu"""
    
    print("=" * 70)
    print("🚀 PRODUCTEUR KAFKA DÉMARRÉ")
    print(f"📡 Topic: {TOPIC_NAME}")
    print(f"🔗 Broker: {KAFKA_BROKER}")
    print("=" * 70)
    print()
    
    transaction_count = 0
    
    try:
        while True:
            # Générer une transaction
            transaction = generate_transaction()
            
            # Envoyer à Kafka
            producer.send(TOPIC_NAME, value=transaction)
            
            transaction_count += 1
            
            # Afficher dans la console
            print(f"✅ Transaction #{transaction_count} envoyée:")
            print(f"   🆔 ID: {transaction['transaction_id'][:8]}...")
            print(f"   👤 Client: {transaction['customer_name']}")
            print(f"   🛒 Produit: {transaction['product']} x{transaction['quantity']}")
            print(f"   💰 Montant: ${transaction['total_amount']}")
            print(f"   📍 Pays: {transaction['country']}")
            print(f"   ⏰ {transaction['timestamp']}")
            print()
            
            # Attendre entre 1 et 3 secondes
            time.sleep(random.uniform(1, 3))
            
    except KeyboardInterrupt:
        print("\n" + "=" * 70)
        print(f"⏹️  ARRÊT DU PRODUCTEUR")
        print(f"📊 Total transactions envoyées: {transaction_count}")
        print("=" * 70)
        producer.close()

if __name__ == "__main__":
    main()