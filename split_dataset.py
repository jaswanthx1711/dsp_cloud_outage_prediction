"""
split_dataset.py — Split a CSV dataset into N files of exactly 10 rows each in raw_data/

Usage:
    python split_dataset.py --input data/cloud_outages_dataset.csv --output data/raw_data --num-files 30
"""
import pandas as pd
import os
import argparse

ROWS_PER_FILE = 10


def split_dataset(input_path: str, output_dir: str, num_files: int, seed: int = 42):
    df = pd.read_csv(input_path)
    os.makedirs(output_dir, exist_ok=True)

    needed_rows = num_files * ROWS_PER_FILE
    if needed_rows > len(df):
        raise ValueError(
            f"Requested {num_files} files x {ROWS_PER_FILE} rows = {needed_rows} rows, "
            f"but the dataset only has {len(df)} rows."
        )

    # Shuffle so each demo run gets a varied mix of rows across files
    sample = df.sample(n=needed_rows, random_state=seed).reset_index(drop=True)

    for i in range(num_files):
        chunk = sample.iloc[i * ROWS_PER_FILE:(i + 1) * ROWS_PER_FILE]
        out_path = os.path.join(output_dir, f"batch_{i + 1:03d}.csv")
        chunk.to_csv(out_path, index=False)
        print(f" Saved {out_path} ({len(chunk)} rows)")

    print(f"\nDone! {num_files} files of {ROWS_PER_FILE} rows saved to '{output_dir}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split dataset into N files of exactly 10 rows each")
    parser.add_argument("--input", required=True, help="Path to input CSV")
    parser.add_argument("--output", default="data/raw_data", help="Output directory")
    parser.add_argument("--num-files", type=int, required=True, help="Number of files to generate")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for row sampling")
    args = parser.parse_args()

    split_dataset(args.input, args.output, args.num_files, args.seed)
