import pandas as pd
from decimal import Decimal, ROUND_HALF_UP

INPUT_FILE = "data/sales.csv"
OUTPUT_FILE = "data/sales_transformed_pandas.csv"
REJECTED_FILE = "data/sales_rejected_pandas.csv"

df = pd.read_csv(INPUT_FILE)

df["amount_numeric"] = pd.to_numeric(
    df["amount"],
    errors="coerce"
)

valid_id = df["id"] > 0

valid_name = (
    df["name"].notna()
    & df["name"].str.strip().ne("")
)

valid_amount_numeric = df["amount_numeric"].notna()

valid_amount_nonnegative = df["amount_numeric"] >= 0

df["error"] = ""

df.loc[~valid_id, "error"] += "ID must be a positive integer; "

df.loc[~valid_name, "error"] += "Name must not be empty; "

df.loc[~valid_amount_numeric, "error"] += \
    "Amount must be a valid decimal number; "

df.loc[
    valid_amount_numeric & ~valid_amount_nonnegative,
    "error"
] += "Amount must not be negative; "

df["error"] = df["error"].str.rstrip("; ")

valid_mask = (
    valid_id
    & valid_name
    & valid_amount_numeric
    & valid_amount_nonnegative
)

valid_df = df[valid_mask].copy()
rejected_df = df[~valid_mask].copy()

valid_df["amount"] = valid_df["amount"].map(
    lambda value: Decimal(value).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP
    )
)

valid_df["vat"] = valid_df["amount"].map(
    lambda amount: (amount * Decimal("0.20")).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP
    )
)

valid_df["amount_with_vat"] = (
    valid_df["amount"] + valid_df["vat"]
)

valid_df = valid_df[
    [
        "id",
        "name",
        "amount",
        "vat",
        "amount_with_vat"
    ]
]

rejected_df = rejected_df[
    [
        "id",
        "name",
        "amount",
        "error"
    ]
]

valid_df.to_csv(
    OUTPUT_FILE,
    index=False
)

rejected_df.to_csv(
    REJECTED_FILE,
    index=False
)

print("\nAFTER NUMERIC CONVERSION:")
print(df)

print("\nDTYPES AFTER CONVERSION:")
print(df.dtypes)

print("\nFINAL VALID DATA:")
print(valid_df)

print("\nFINAL REJECTED DATA:")
print(rejected_df)