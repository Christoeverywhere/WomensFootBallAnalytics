import pandas as pd
import os

input_file = "events_master.csv"
rows_per_file = 100000
output_folder = "split_output"

os.makedirs(output_folder, exist_ok=True)

chunk_iter = pd.read_csv(
    input_file,
    chunksize=rows_per_file,
    dtype=str,
    low_memory=False
)

for i, chunk in enumerate(chunk_iter, start=1):
    output_file = os.path.join(output_folder, f"events_master{i}.csv")
    chunk.to_csv(output_file, index=False)
    print(f"Saved {output_file} with {len(chunk)} rows")

print("Done splitting!")