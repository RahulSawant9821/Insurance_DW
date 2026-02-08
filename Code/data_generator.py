import random
from faker import Faker
from datetime import datetime, timedelta, date
import psycopg2
from psycopg2.extras import execute_values

# ------------------- Config -------------------
fake = Faker('en_GB')
NUM_CUSTOMERS = 100

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'InsureMe',
    'user': 'admin',
    'password': 'admin@1234'
}

# ------------------- Connect to PostgreSQL -------------------
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()
print("Connected to PostgreSQL!")

# ------------------- Generate Customers -------------------
customers = []
for i in range(1, NUM_CUSTOMERS + 1):
    customer_id = f"CUST{i:03d}"
    first_name = fake.first_name()
    second_name = fake.last_name()
    postcode = fake.postcode()
    dob = fake.date_of_birth(minimum_age=18, maximum_age=80)
    created_at = fake.date_time_between(start_date='-5y', end_date='now')
    updated_at = created_at + timedelta(days=random.randint(0, 365))
    customers.append((customer_id, first_name, second_name, postcode, dob, created_at, updated_at))

# ------------------- Generate Customer Financial Profiles -------------------
financial_profiles = []
for i in range(1, NUM_CUSTOMERS + 1):
    profile_id = f"CFP{i:03d}"
    customer_id = f"CUST{i:03d}"
    credit_score = random.randint(300, 850)
    source = random.choice(['Experian', 'Equifax', 'TransUnion'])
    retrieved_at = fake.date_between(start_date='-2y', end_date='today')
    created_at = datetime.now()
    updated_at = created_at
    financial_profiles.append((profile_id, customer_id, credit_score, source, retrieved_at, created_at, updated_at))

# ------------------- Generate Policies -------------------
policies = []
policy_types = ['MOTOR', 'HOME']
status_options = ['pending','active','lapsed','terminated','settled','expired']

for i in range(1, NUM_CUSTOMERS + 1):
    num_policies = random.randint(1, 3)  # some customers have multiple policies
    for j in range(num_policies):
        policy_id = f"POL{i:03d}_{j+1}"
        customer_id = f"CUST{i:03d}"
        policy_type = random.choice(policy_types)
        start_date = fake.date_between(start_date='-5y', end_date='today')
        end_date = start_date + timedelta(days=random.randint(365, 1095))
        status = random.choice(status_options)
        annual_premium_amount = round(random.uniform(200, 2000), 2)
        created_at = datetime.now()
        updated_at = created_at
        policies.append((policy_id, customer_id, annual_premium_amount, policy_type, start_date, end_date, status, created_at, updated_at))

# ------------------- Generate Claims -------------------
claims = []
claim_status_options = ['reported','under_review','approved','rejected','settled']

for i, policy in enumerate(policies, start=1):
    if random.random() < 0.5:  # 50% of policies have a claim
        claim_id = f"CLM{i:04d}"
        policy_id = policy[0]

        # Ensure datetime for incident
        incident_start = datetime.combine(policy[4], datetime.min.time())
        incident_end = datetime.combine(policy[5], datetime.min.time())
        delta_days = (incident_end - incident_start).days
        incident_date_time = incident_start + timedelta(days=random.randint(0, max(delta_days,0)))

        # Report date as date object
        report_date = (incident_date_time + timedelta(days=random.randint(0, 10))).date()

        claim_amount = round(random.uniform(100, 5000), 2)
        incident_type = random.choice(['Accident', 'Theft', 'Fire', 'Flood', 'Vandalism'])
        incident_lat = round(random.uniform(50.0, 55.0), 6)  # UK approx
        incident_long = round(random.uniform(-5.0, 1.0), 6)  # UK approx
        incident_location = fake.city()
        status = random.choice(claim_status_options)
        image_id = None
        created_at = datetime.now()
        updated_at = created_at

        claims.append((
            claim_id, policy_id, incident_date_time, report_date, claim_amount,
            incident_type, incident_lat, incident_long, incident_location,
            status, image_id, created_at, updated_at
        ))

# ------------------- Insert Data Into PostgreSQL -------------------
def batch_insert(table_name, columns, data):
    sql = f"INSERT INTO {table_name} ({columns}) VALUES %s ON CONFLICT DO NOTHING"
    execute_values(cursor, sql, data)
    conn.commit()
    print(f"Inserted {len(data)} rows into {table_name}")

batch_insert(
    'customers',
    "customer_id, first_name, second_name, postcode, d_o_b, created_at, updated_at",
    customers
)

batch_insert(
    'customer_financial_profile',
    "profile_id, customer_id, credit_score, source, retrieved_at, created_at, updated_at",
    financial_profiles
)

batch_insert(
    'policy',
    "policy_id, customer_id, annual_premium_amount, policy_type, start_date, end_date, status, created_at, updated_at",
    policies
)

batch_insert(
    'claims',
    "claim_id, policy_id, incident_date_time, report_date, claim_amount, incident_type, incident_lat, incident_long, incident_location, status, image_id, created_at, updated_at",
    claims
)

# ------------------- Close Connection -------------------
cursor.close()
conn.close()
print("All data inserted and connection closed!")
