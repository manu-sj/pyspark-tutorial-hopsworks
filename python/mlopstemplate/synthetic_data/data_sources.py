import random
from mlopstemplate.synthetic_data import synthetic_data, cc_features

synthetic_data.set_random_seed(12345)
synthetic_data.FRAUD_RATIO = random.uniform(0.001, 0.005)
synthetic_data.TOTAL_UNIQUE_USERS = 1000
synthetic_data.TOTAL_UNIQUE_TRANSACTIONS = 54000
synthetic_data.CASH_WITHRAWAL_CARDS_TOTAL = 2000
synthetic_data.TOTAL_UNIQUE_CASH_WITHDRAWALS = 1200


def get_datasets():
    credit_cards = synthetic_data.generate_list_credit_card_numbers()
    profiles_df = synthetic_data.create_profiles_as_df(credit_cards)
    trans_df, fraud_labels = synthetic_data.create_transactions_as_df(credit_cards)
    return trans_df, fraud_labels, profiles_df
