import json
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import accuracy_score
import mlflow
import mlflow.sklearn

def load_best_params():
    """Load the best model parameters from a JSON file."""
    try:
        with open('best_model_and_params.json', 'r') as f:
            best_model_info = json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError("The 'best_model_and_params.json' file was not found.")
    except json.JSONDecodeError:
        raise ValueError("Error decoding JSON from 'best_model_and_params.json'.")
    
    return best_model_info

def initialize_model(model_name, params):
    """Initialize the model based on the model name and parameters."""
    if model_name == 'SVM':
        return SVC(C=params['C'], kernel=params['kernel'])
    elif model_name == 'RandomForest':
        return RandomForestClassifier(n_estimators=params['n_estimators'], max_depth=params['max_depth'])
    elif model_name == 'KNN':
        return KNeighborsClassifier(n_neighbors=params['n_neighbors'])
    else:
        raise ValueError(f"Unknown model name {model_name}")

def train_best_model(best_model_name, best_params):
    """Train the best model using the best parameters."""
    # Load the preprocessed data
    data = pd.read_csv('data/processed/iris_cleaned.csv')
    X = data.drop('target', axis=1)
    y = data['target']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

    # Initialize the model with the best parameters
    best_model = initialize_model(best_model_name, best_params)

    # Train the model
    best_model.fit(X_train, y_train)

    # Evaluate the model on the test set
    y_pred = best_model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    print(f"Test accuracy of the best model: {accuracy}")

    # Log and register the final model using MLflow
    mlflow.set_experiment("Iris Final Training")
    with mlflow.start_run(run_name="Final_Model_Training") as run:
        mlflow.log_params(best_params)
        mlflow.log_metric("test_accuracy", accuracy)
        mlflow.sklearn.log_model(best_model, "final_trained_model")
        
        # Register the model
        model_uri = f"runs:/{run.info.run_id}/final_trained_model"
        model_name = "Iris_Model"
        
        try:
            result = mlflow.register_model(model_uri, model_name)
            version = result.version
        except Exception as e:
            print(f"Error during model registration: {e}")
            version = "Not Registered"
        
        # Save the run ID, model name, version, and accuracy to the JSON file
        best_model_info = {
            'best_model_name': best_model_name,
            'best_params': best_params,
            'mlflow_run_id': run.info.run_id,
            'mlflow_model_name': model_name,
            'mlflow_model_version': version,
            'best_score': accuracy,  # Save the best score as well
            'mlflow_experiment_id': mlflow.get_experiment_by_name("Iris Final Training").experiment_id  # Save experiment ID
        }
        try:
            with open('best_model_and_params.json', 'w') as f:
                json.dump(best_model_info, f, indent=4)
        except IOError:
            print("Error writing to 'best_model_and_params.json'.")

    print("Final model training, logging, and registration complete.")

if __name__ == "__main__":
    best_model_info = load_best_params()
    best_model_name = best_model_info['best_model_name']
    best_params = best_model_info['best_params']
    train_best_model(best_model_name, best_params)
# nilesh