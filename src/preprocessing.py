import os
import pandas as pd

def preprocess_data():
    # File paths
    input_file = 'data/raw/iris.csv'
    output_dir = 'data/processed'
    output_file = os.path.join(output_dir, 'iris_cleaned.csv')

    # Check if the input file exists
    if not os.path.exists(input_file):
        print(f"File not found: {input_file}")
        return

    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Load the dataset
    df = pd.read_csv(input_file)

    # Check for null values
    if df.isnull().values.any():
        print("Null values found! Removing null values...")
        df = df.dropna()  # Remove rows with null values
    else:
        print("No null values found.")

    # Check for duplicate rows
    if df.duplicated().any():
        print("Duplicates found! Removing duplicate rows...")
        df = df.drop_duplicates()  # Remove duplicate rows
    else:
        print("No duplicates found.")

    # Save the cleaned dataset
    df.to_csv(output_file, index=False)
    print(f"Cleaned data saved to {output_file}")

if __name__ == "__main__":
    print("Running data preprocessing...")
    preprocess_data()
