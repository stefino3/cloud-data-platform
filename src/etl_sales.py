import csv
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

INPUT_FILE = "data/sales.csv"
OUTPUT_FILE = "data/sales_transformed.csv"
REJECTED_FILE = "data/sales_rejected.csv"

def transform(rows):
    transformed_rows = []
    rejected_rows = []

    for row in rows:
        try:
            id_value = int(row["id"])
            name_value = row["name"]
            amount_value = Decimal(row["amount"])

            if id_value <= 0:
                raise ValueError("ID must be a positive integer.")

            if not name_value.strip():
                raise ValueError("Name must not be empty.")

            if amount_value < 0:
                raise ValueError("Amount must not be negative.")

            vat_value = (amount_value * Decimal("0.20")).quantize(
                Decimal("0.01"),
                rounding=ROUND_HALF_UP
            )

            amount_with_vat = (amount_value + vat_value).quantize(
                Decimal("0.01"),
                rounding=ROUND_HALF_UP
            )

            amount_value = amount_value.quantize(
                Decimal("0.01"),
                rounding=ROUND_HALF_UP
            )

            transformed_row = {
                "id": id_value,
                "name": name_value,
                "amount": amount_value,
                "vat": vat_value,
                "amount_with_vat": amount_with_vat
            }

            transformed_rows.append(transformed_row)


        except InvalidOperation:

            rejected_rows.append({

                "row": row,

                "error": "Amount must be a valid decimal number."

            })


        except ValueError as error:

            rejected_rows.append({

                "row": row,

                "error": str(error)

            })

    return transformed_rows, rejected_rows

def extract():
    rows = []

    with open(INPUT_FILE, mode="r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)

        for row in reader:
            rows.append(row)

    return rows

def load(rows):
    fieldnames = [
        "id",
        "name",
        "amount",
        "vat",
        "amount_with_vat"
    ]

    with open(
        OUTPUT_FILE,
        mode="w",
        encoding="utf-8",
        newline=""
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames
        )

        writer.writeheader()
        writer.writerows(rows)

def load_rejected(rows):
    fieldnames = [
        "id",
        "name",
        "amount",
        "error"
    ]

    with open(
        REJECTED_FILE,
        mode="w",
        encoding="utf-8",
        newline=""
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames
        )

        writer.writeheader()

        for rejected in rows:
            row = rejected["row"]

            writer.writerow({
                "id": row["id"],
                "name": row["name"],
                "amount": row["amount"],
                "error": rejected["error"]
            })

rows = extract()

transformed_rows, rejected_rows = transform(rows)

load(transformed_rows)
load_rejected(rejected_rows)

print("RAW:")
print(rows)

print("TRANSFORMED:")
print(transformed_rows)

print("REJECTED:")
print(rejected_rows)