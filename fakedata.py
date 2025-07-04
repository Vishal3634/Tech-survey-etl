import os
import psycopg2
import random
from faker import Faker
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DB_HOST = os.getenv("PG_HOST")
DB_PORT = os.getenv("PG_PORT")
DB_NAME = os.getenv("PG_DBNAME")
DB_USER = os.getenv("PG_USER")
DB_PASSWORD = os.getenv("PG_PASSWORD")

# Initialize Faker
fake = Faker()

# Connect to PostgreSQL
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
cursor = conn.cursor()

# Device and purpose options
device_options = ['Mobile', 'Laptop', 'Tablet', 'Smartwatch', 'Desktop']
tech_purposes = ['Education', 'Entertainment', 'Social Media', 'Work', 'Gaming']
apps = ['Instagram', 'YouTube', 'Notion', 'WhatsApp', 'Telegram', 'Facebook']

# Create the table if it doesn't exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS tech_survey_responses (
    id SERIAL PRIMARY KEY,
    location TEXT,
    age_group TEXT,
    gender TEXT,
    occupation TEXT,
    devices_used TEXT,
    screen_time TEXT,
    tech_purpose TEXT,
    satisfaction INTEGER,
    most_used_app TEXT,
    question TEXT
);
""")
conn.commit()

# Generate and insert 200 fake records
for _ in range(200):
    location = fake.city()
    age_group = random.choice(['<18', '18-25', '26-35', '36-50', '50+'])
    gender = random.choice(['Male', 'Female', 'Other'])
    occupation = random.choice(['Student', 'Professional', 'Unemployed', 'Other'])
    devices_used = ', '.join(random.sample(device_options, k=random.randint(1, 3)))
    screen_time = random.choice(['<2', '2–4', '4–6', '6+'])
    tech_purpose = random.choice(tech_purposes)
    satisfaction = random.randint(1, 5)  # Integer satisfaction
    most_used_app = random.choice(apps)
    question = fake.sentence()

    cursor.execute("""
        INSERT INTO tech_survey_responses (
            location, age_group, gender, occupation, devices_used,
            screen_time, tech_purpose, satisfaction, most_used_app, question
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
    """, (
        location, age_group, gender, occupation, devices_used,
        screen_time, tech_purpose, satisfaction, most_used_app, question
    ))

conn.commit()
cursor.close()
conn.close()

print("✅ 200 dummy survey records inserted successfully!")
