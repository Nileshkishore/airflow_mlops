import os
import pandas as pd
from sklearn.datasets import load_iris

def ingest_data():
    print("")
    # Load Iris dataset
    # iris = load_iris(as_frame=True)
    # df = iris.frame  # Load it directly as a DataFrame

    # # Define the file path for saving
    # output_dir = 'data/raw'
    # os.makedirs(output_dir, exist_ok=True)  # Create the directory if it doesn't exist
    # output_file = os.path.join(output_dir, 'iris.csv')

    # # Save the dataset to a CSV file
    # df.to_csv(output_file, index=False)
    # print(f"Iris dataset saved to {output_file}")

if __name__ == "__main__":
    print("Running data ingestion...")
    ingest_data()
