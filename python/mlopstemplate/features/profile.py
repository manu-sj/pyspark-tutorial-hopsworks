import pandas as pd


def select_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Args:
    - df: DataFrame
    Returns:
    - DataFrame:
    """
    return df[["cc_num", "cc_provider", "cc_type", "cc_expiration_date", "birthdate", "country_of_residence"]]
