import json
import mlflow
import os

def load_model_info():
    """Load the model info from the JSON file."""
    with open('best_model_and_params.json', 'r') as f:
        return json.load(f)

def download_model(destination_folder):
    model_info = load_model_info()
    model_name = model_info['mlflow_model_name']
    model_version = model_info['mlflow_model_version']
    
    model_uri = f"models:/{model_name}/{model_version}"
    # Ensure destination folder exists
    os.makedirs(destination_folder, exist_ok=True)
    # Download the model
    mlflow.artifacts.download_artifacts(artifact_uri=model_uri, dst_path=destination_folder)
    print(f"Model downloaded to {destination_folder}")

if __name__ == "__main__":
    destination_folder = 'registerd_model'
    download_model(destination_folder)
