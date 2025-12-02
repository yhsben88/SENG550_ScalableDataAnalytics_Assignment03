"""
split_raw.py
Reads the downloaded Kaggle orders CSV (single file) and splits into:
part1/data/raw/{dow}/orders_{dow}.csv  for dow in 0..6
"""

import os
import pandas as pd
import sys

# UPDATE: path to the original orders CSV from Kaggle
INPUT_CSV = "orders.csv.csv"  # place your downloaded file here
OUT_DIR = "part1/data/raw"

def main():
    if not os.path.exists(INPUT_CSV):
        print(f"ERROR: put the Kaggle orders CSV at {INPUT_CSV}")
        sys.exit(1)
    df = pd.read_csv(INPUT_CSV)
    if "order_dow" not in df.columns:
        print("ERROR: input does not contain 'order_dow' column. Check the dataset.")
        sys.exit(1)
    os.makedirs(OUT_DIR, exist_ok=True)
    for dow in sorted(df["order_dow"].unique()):
        subdir = os.path.join(OUT_DIR, str(int(dow)))
        os.makedirs(subdir, exist_ok=True)
        out_path = os.path.join(subdir, f"orders_{int(dow)}.csv")
        df[df["order_dow"] == dow].to_csv(out_path, index=False)
        print(f"Wrote {out_path}")

if __name__ == "__main__":
    main()
