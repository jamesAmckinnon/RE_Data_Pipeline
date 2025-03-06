import pandas as pd
from pathlib import Path

input_folder_path = Path("/opt/airflow/data/cre_listings/brokerage_listings")
output_folder_path = Path("/opt/airflow/data/cre_listings")
all_listings = []

def get_sale_or_lease(sale_or_lease):
    try:
        if "sale" in sale_or_lease.lower() and "lease" in sale_or_lease.lower():
            sale_or_lease = "sale_or_lease"
        elif "sale" in sale_or_lease.lower():
            sale_or_lease = "sale"
        elif "sublease" in sale_or_lease.lower():
            sale_or_lease = "sublease"
        elif "lease" in sale_or_lease.lower():
            sale_or_lease = "lease"
        return sale_or_lease
    except:
        return None

def combine_broker_listings():
    for csv_file in input_folder_path.glob("*.csv"):
        listing_df = pd.read_csv(csv_file)
        listing_df["sale_or_lease"] = listing_df["sale_or_lease"].apply(get_sale_or_lease)

        # print(f"Adding {len(listing_df)} {str(csv_file).split("Listings")[0].replace("_", " ").strip()} listings to combined listings dataset.")
        all_listings.append(listing_df)

    all_listings_df = pd.concat(all_listings, ignore_index=True)
    print("\n============ Non-nan percentages ============ \n\n", (all_listings_df.count() / len(all_listings_df))*100, "\n")
    all_listings_df.to_csv(output_folder_path / "all_cre_listings.csv")

