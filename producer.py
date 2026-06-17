import json
import time
import uuid
import random
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = 'transaction_events'

# ---- Realistic Nigerian fintech user profiles ----
USERS = [
    {
        'user_id': f'user-{str(i).zfill(3)}',
        'name': fake.name(),
        'account_number': fake.bban(),
        'bank': random.choice([
            'GTBank', 'Access Bank', 'Zenith Bank',
            'UBA', 'First Bank', 'Kuda', 'Moniepoint', 'OPay'
        ])
    }
    for i in range(1, 21)
]

CHANNELS = ['mobile_app', 'ussd', 'web', 'pos', 'transfer']
TRANSACTION_TYPES = ['deposit', 'withdrawal', 'transfer']
INVALID_TYPES = ['refund', 'chargeback', 'reversal', 'loan']


def create_transaction(
    user=None,
    amount=None,
    txn_type=None,
    out_of_order=False,
    missing_amount=False,
    missing_user=False,
    invalid_type=False,
    negative_amount=False
):
    txn_id = f'txn-{uuid.uuid4()}'
    timestamp = datetime.now(timezone.utc)
    selected_user = user or random.choice(USERS)

    if out_of_order:
        timestamp = timestamp - timedelta(hours=random.randint(1, 3))

    event = {
        'transaction_id': txn_id,
        'user_id': selected_user['user_id'],
        'user_name': selected_user['name'],
        'account_number': selected_user['account_number'],
        'bank': selected_user['bank'],
        'type': txn_type or random.choice(TRANSACTION_TYPES),
        'amount': amount or round(random.uniform(100, 50000), 2),
        'currency': 'NGN',
        'timestamp': timestamp.isoformat(),
        'channel': random.choice(CHANNELS),
        'description': random.choice([
            'salary payment',
            'airtime purchase',
            'school fees',
            'rent payment',
            'food and groceries',
            'electricity bill',
            'data subscription',
            'transfer to family'
        ])
    }

    # ---- inject chaos ----
    if missing_amount:
        del event['amount']

    if missing_user:
        del event['user_id']

    if invalid_type:
        event['type'] = random.choice(INVALID_TYPES)

    if negative_amount:
        event['amount'] = round(random.uniform(-10000, -100), 2)

    return event


def send_event(event):
    producer.send(TOPIC, value=event)
    producer.flush()
    print(f"Sent: {event.get('transaction_id')} | "
          f"user: {event.get('user_name', 'MISSING')} | "
          f"type: {event.get('type')} | "
          f"amount: {event.get('amount', 'MISSING')}")


# ---- Fraud patterns ----

def simulate_high_frequency(user):
    print(f"\n FRAUD: high frequency — {user['name']}")
    for _ in range(6):
        event = create_transaction(user=user)
        send_event(event)
        time.sleep(0.3)


def simulate_large_withdrawal(user):
    print(f"\n FRAUD: large withdrawal — {user['name']}")
    event = create_transaction(
        user=user,
        amount=round(random.uniform(150000, 500000), 2),
        txn_type='withdrawal'
    )
    send_event(event)


def simulate_repeated_amount(user):
    print(f"\n FRAUD: repeated amount — {user['name']}")
    amount = round(random.uniform(1000, 10000), 2)
    for _ in range(3):
        event = create_transaction(user=user, amount=amount)
        send_event(event)
        time.sleep(0.5)


# ---- Main loop ----

def run():
    print(" Producer started — sending to topic:", TOPIC)
    print(f" {len(USERS)} users loaded\n")
    sent_events = []

    while True:
        chaos = random.random()

        if chaos < 0.50:
            # 50% — normal clean transaction
            event = create_transaction()
            send_event(event)
            sent_events.append(event)

        elif chaos < 0.62:
            # 12% — duplicate
            if sent_events:
                duplicate = random.choice(sent_events)
                print(f"\n  DUPLICATE: resending {duplicate['transaction_id']}")
                send_event(duplicate)

        elif chaos < 0.69:
            # 7% — out of order
            event = create_transaction(out_of_order=True)
            print(f"\n OUT-OF-ORDER: timestamp backdated")
            send_event(event)
            sent_events.append(event)

        elif chaos < 0.74:
            # 5% — missing amount
            event = create_transaction(missing_amount=True)
            print(f"\n MALFORMED: missing amount")
            send_event(event)

        elif chaos < 0.79:
            # 5% — missing user_id
            event = create_transaction(missing_user=True)
            print(f"\n MALFORMED: missing user_id")
            send_event(event)

        elif chaos < 0.83:
            # 4% — invalid transaction type
            event = create_transaction(invalid_type=True)
            print(f"\n MALFORMED: invalid type — {event.get('type')}")
            send_event(event)

        elif chaos < 0.86:
            # 3% — negative amount
            event = create_transaction(negative_amount=True)
            print(f"\n MALFORMED: negative amount — {event.get('amount')}")
            send_event(event)

        elif chaos < 0.91:
            # 5% — high frequency fraud
            user = random.choice(USERS)
            simulate_high_frequency(user)

        elif chaos < 0.95:
            # 4% — large withdrawal fraud
            user = random.choice(USERS)
            simulate_large_withdrawal(user)

        else:
            # 5% — repeated amount fraud
            user = random.choice(USERS)
            simulate_repeated_amount(user)

        time.sleep(1)


if __name__ == '__main__':
    run()