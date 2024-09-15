import pandas as pd
from sklearn.model_selection import train_test_split
from evidently import ColumnMapping
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset, DataQualityPreset, TargetDriftPreset
import json

def save_drift_results(results, file_path='drift_results.json'):
    """Save the drift results to a JSON file."""
    with open(file_path, 'w') as f:
        json.dump(results, f, indent=4)

def save_html_report(report, file_path='file_report.html'):
    """Save the HTML report."""
    report.save_html(file_path)

def is_drift_detected(drift_results):
    """
    Check if data drift is detected in the results.
    """
    for metric in drift_results.get('metrics', []):
        result = metric.get('result', {})
        # Check if dataset_drift exists and is True
        if result.get('dataset_drift', False):
            return True
    return False

def append_new_data(new_data, file_path='data/raw/iris.csv'):
    """Append new data to the existing dataset and save it."""
    existing_data = pd.read_csv(file_path)
    updated_data = pd.concat([existing_data, new_data], ignore_index=True)
    updated_data.to_csv(file_path, index=False)
    print("New data appended to the existing dataset.")

def main():
    # Load reference and new data
    reference_data = pd.read_csv('data/processed/iris_cleaned.csv')
    new_data = pd.read_csv('data/new_data/new_data.csv')

    # Define column mapping (update target column name if needed)
    column_mapping = ColumnMapping(
        target='target'  # Replace with your target column name
    )
    
    # Initialize Drift and Quality Presets
    drift_preset = DataDriftPreset()
    quality_preset = DataQualityPreset()
    target_drift_preset = TargetDriftPreset()  # Target Drift Preset added
    
    # Create a Report instance with all presets
    drift_report = Report(metrics=[
        drift_preset,
        quality_preset,
        target_drift_preset  # Include TargetDriftPreset
    ])
    
    # Run the drift and quality tests
    drift_report.run(
        current_data=new_data,
        reference_data=reference_data,
        column_mapping=column_mapping
    )
    
    # Save the drift results to JSON
    drift_results_json = drift_report.json()
    drift_results = json.loads(drift_results_json)  # Convert JSON string to dictionary
    save_drift_results(drift_results)
    
    # Save the HTML report
    save_html_report(drift_report)
    
    # Check if data drift is detected
    drift_detected = is_drift_detected(drift_results)
    if drift_detected:
        print("Data drift detected.")
    else:
        print("No data drift detected.")
    
    print(f"Drift results saved to drift_results.json.")
    print(f"HTML report saved to file_report.html.")

    append_new_data(new_data)  # Append new data to the existing dataset

if __name__ == '__main__':
    main()



# import pandas as pd
# from sklearn.model_selection import train_test_split
# from evidently import ColumnMapping
# from evidently.report import Report
# from evidently.metric_preset import DataDriftPreset, DataQualityPreset
# import json

# # def load_data():
# #     """Load the data from a CSV file."""
# #     return pd.read_csv('data/processed/iris_cleaned.csv')

# def save_drift_results(results, file_path='drift_results.json'):
#     """Save the drift results to a JSON file."""
#     with open(file_path, 'w') as f:
#         json.dump(results, f, indent=4)

# def save_html_report(report, file_path='file_report.html'):
#     """Save the HTML report."""
#     report.save_html(file_path)

# def is_drift_detected(drift_results):
#     """
#     Check if data drift is detected in the results.
#     """
#     for metric in drift_results.get('metrics', []):
#         result = metric.get('result', {})
#         # Check if dataset_drift exists and is True
#         if result.get('dataset_drift', False):
#             return True
#     return False


# def append_new_data(new_data, file_path='data/raw/iris.csv'):
#     """Append new data to the existing dataset and save it."""
#     existing_data = pd.read_csv(file_path)
#     updated_data = pd.concat([existing_data, new_data], ignore_index=True)
#     updated_data.to_csv(file_path, index=False)
#     print("New data appended to the existing dataset.")


# def main():
#     # Load data
#     # data = load_data()

#     # Split the data into reference and new data
#     reference_data= pd.read_csv('data/processed/iris_cleaned.csv')
#     new_data = pd.read_csv('data/new_data/new_data.csv')
#     # reference_data, new_data = data.iloc[:75], data.iloc[75:]

#     # Define column mapping (update target column name if needed)
#     column_mapping = ColumnMapping(
#         target='target'  # Replace with your target column name
#     )
    
#     # Initialize DataDriftPreset
#     drift_preset = DataDriftPreset()
    
#     # Initialize DataQualityPreset
#     quality_preset = DataQualityPreset()
    
#     # Create a Report instance
#     drift_report = Report(metrics=[
#         drift_preset,
#         quality_preset
#     ])
    
#     # Run the drift and quality tests
#     drift_report.run(
#         current_data=new_data,
#         reference_data=reference_data,
#         column_mapping=column_mapping
#     )
    
#     # Save the drift results to JSON
#     drift_results_json = drift_report.json()
#     drift_results = json.loads(drift_results_json)  # Convert JSON string to dictionary
#     save_drift_results(drift_results)
    
#     # Save the HTML report
#     save_html_report(drift_report)
    
#     # Check if drift is detected
#     drift_detected = is_drift_detected(drift_results)
#     if drift_detected:
#         print("Data drift detected.")
#     else:
#         print("No data drift detected.")
    
#     print(f"Drift results saved to drift_results.json.")
#     print(f"HTML report saved to file_report.html.")

#     append_new_data(new_data)  # Append new data to the existing dataset

# if __name__ == '__main__':
#     main()
