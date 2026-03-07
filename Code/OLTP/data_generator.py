import random
from faker import Faker
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values

# ------------------- Config -------------------
fake = Faker('en_GB')
NUM_CUSTOMERS = 10000

DB_CONFIG = {
    'host': '*',
    'port': *,
    'database': '*',
    'user': '*',
    'password': '*'
}

# ------------------- Connect -------------------
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()
print("Connected to PostgreSQL")

# ------------------- Generate Customers -------------------
customers = []

for i in range(1, NUM_CUSTOMERS + 1):

    customer_id = f"CUST{i:05d}"

    created_at = fake.date_time_between(start_date='-5y', end_date='now')
    updated_at = created_at + timedelta(days=random.randint(0, 365))

    customers.append((
        customer_id,
        fake.first_name(),
        fake.last_name(),
        fake.postcode(),
        fake.date_of_birth(minimum_age=18, maximum_age=90),
        created_at,
        updated_at
    ))

# ------------------- Generate Financial Profiles -------------------
financial_profiles = []

for i in range(1, NUM_CUSTOMERS + 1):

    profile_id = f"CFP{i:05d}"
    customer_id = f"CUST{i:05d}"

    financial_profiles.append((
        profile_id,
        customer_id,
        random.randint(300, 850),
        random.choice(['Experian', 'Equifax', 'TransUnion']),
        fake.date_between(start_date='-2y', end_date='today'),
        datetime.now(),
        datetime.now()
    ))

# ------------------- Generate Policies -------------------
policies = []

policy_types = ['MOTOR', 'HOME']
status_options = ['pending','active','lapsed','terminated','settled','expired']

for i in range(1, NUM_CUSTOMERS + 1):

    customer_id = f"CUST{i:05d}"

    num_policies = random.randint(1,3)

    for j in range(num_policies):

        policy_id = f"POL{i:05d}_{j+1}"

        start_date = fake.date_between(start_date='-5y', end_date='today')
        end_date = start_date + timedelta(days=random.randint(365,1095))

        policies.append((
            policy_id,
            customer_id,
            round(random.uniform(200,2000),2),
            random.choice(policy_types),
            start_date,
            end_date,
            random.choice(status_options),
            datetime.now(),
            datetime.now()
        ))

# ------------------- Generate Claims -------------------
claims = []

claim_status = ['reported','under_review','approved','rejected','settled']

for idx, policy in enumerate(policies):

    if random.random() < 0.5:

        policy_id = policy[0]
        claim_id = f"CLM{idx:06d}"

        start_date = policy[4]
        end_date = policy[5]

        incident_datetime = datetime.combine(start_date, datetime.min.time()) + timedelta(
            days=random.randint(0,(end_date-start_date).days)
        )

        report_date = (incident_datetime + timedelta(days=random.randint(0,10))).date()

        claims.append((
            claim_id,
            policy_id,
            incident_datetime,
            report_date,
            round(random.uniform(100,5000),2),
            random.choice(['Accident','Theft','Fire','Flood','Vandalism']),
            round(random.uniform(50,55),6),
            round(random.uniform(-5,1),6),
            fake.city(),
            random.choice(claim_status),
            None,
            datetime.now(),
            datetime.now()
        ))

# ------------------- Generate Payments -------------------
payments = []

payment_methods = ['credit_card','bank_transfer','cash','cheque']
payment_status = ['pending','completed','failed','refunded']

for i, claim in enumerate(claims):

    if random.random() < 0.7:

        claim_id = claim[0]
        policy_id = claim[1]

        payments.append((
            f"PAY{i:06d}",
            claim_id,
            policy_id,
            fake.date_between(start_date='-2y', end_date='today'),
            round(random.uniform(100,5000),2),
            random.choice(payment_methods),
            random.choice(payment_status),
            datetime.now(),
            datetime.now()
        ))

# ------------------- Insert Helper -------------------
def batch_insert(table_name, columns, data):

    sql = f"""
    INSERT INTO {table_name} ({columns})
    VALUES %s
    ON CONFLICT DO NOTHING
    """

    execute_values(cursor, sql, data)
    conn.commit()

    print(f"{len(data)} rows inserted into {table_name}")


# ------------------- Insert Order -------------------

batch_insert(
    "Insurance_oltp.Customer",
    "customer_id, first_name, last_name, postcode, d_o_b, created_at, updated_at",
    customers
)

batch_insert(
    "Insurance_oltp.Customer_Financial_Profile",
    "profile_id, customer_id, credit_score, source, retrieved_at, created_at, updated_at",
    financial_profiles
)

batch_insert(
    "Insurance_oltp.Policy",
    "policy_id, customer_id, annual_premium_amount, policy_type, start_date, end_date, status, created_at, updated_at",
    policies
)

batch_insert(
    "Insurance_oltp.Claims",
    """claim_id, policy_id, incident_date_time, report_date, claim_amount,
       incident_type, incident_lat, incident_long, incident_location,
       status, image_id, created_at, updated_at""",
    claims
)

batch_insert(
    "Insurance_oltp.Payment",
    """payment_id, claim_id, policy_id, payment_date, amount,
       payment_method, status_, created_at, updated_at""",
    payments
)

# ------------------- Close -------------------

cursor.close()
conn.close()

print("Data generation completed")
