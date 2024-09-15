








# import json
# import pandas as pd
# from sklearn.model_selection import train_test_split
# from sklearn.svm import SVC
# from sklearn.ensemble import RandomForestClassifier
# from sklearn.neighbors import KNeighborsClassifier
# from sklearn.metrics import accuracy_score
# import mlflow
# import mlflow.sklearn
# import ray

# # Initialize Ray
# ray.init(ignore_reinit_error=True)

# @ray.remote
# def train_model(config, model_name):
#     data = pd.read_csv('data/processed/iris_cleaned.csv')
#     X = data.drop('target', axis=1)
#     y = data['target']
    
#     X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    
#     if model_name == 'SVM':
#         model = SVC(C=config["C"], kernel=config["kernel"])
#     elif model_name == 'RandomForest':
#         model = RandomForestClassifier(n_estimators=config["n_estimators"], max_depth=config["max_depth"])
#     elif model_name == 'KNN':
#         model = KNeighborsClassifier(n_neighbors=config["n_neighbors"])
#     else:
#         raise ValueError(f"Unknown model name {model_name}")

#     model.fit(X_train, y_train)
#     y_pred = model.predict(X_test)
#     accuracy = accuracy_score(y_test, y_pred)
    
#     return accuracy, config

# def perform_hyperparameter_optimization():
#     search_space = {
#         'SVM': [
#             {'C': 0.1, 'kernel': 'linear'},
#             {'C': 0.1, 'kernel': 'rbf'},
#             {'C': 1, 'kernel': 'linear'},
#             {'C': 1, 'kernel': 'rbf'},
#             {'C': 10, 'kernel': 'linear'},
#             {'C': 10, 'kernel': 'rbf'}
#         ],
#         'RandomForest': [
#             {'n_estimators': 10, 'max_depth': 5},
#             {'n_estimators': 10, 'max_depth': 10},
#             {'n_estimators': 10, 'max_depth': 20},
#             {'n_estimators': 50, 'max_depth': 5},
#             {'n_estimators': 50, 'max_depth': 10},
#             {'n_estimators': 50, 'max_depth': 20},
#             {'n_estimators': 100, 'max_depth': 5},
#             {'n_estimators': 100, 'max_depth': 10},
#             {'n_estimators': 100, 'max_depth': 20}
#         ],
#         'KNN': [
#             {'n_neighbors': 3},
#             {'n_neighbors': 5},
#             {'n_neighbors': 7}
#         ]
#     }

#     best_models = {}
#     for model_name, configs in search_space.items():
#         futures = [train_model.remote(config, model_name) for config in configs]
#         results = ray.get(futures)
        
#         best_accuracy = -1
#         best_config = None
#         for accuracy, config in results:
#             if accuracy > best_accuracy:
#                 best_accuracy = accuracy
#                 best_config = config
        
#         print(f"Best Model for {model_name}: {best_config}")
#         print(f"Best Score: {best_accuracy}")

#         best_models[model_name] = {
#             'best_config': best_config,
#             'best_score': best_accuracy
#         }

#     best_model_name = max(best_models, key=lambda model: best_models[model]['best_score'])
#     best_params = best_models[best_model_name]['best_config']

#     print(f"Best overall model: {best_model_name} with params {best_params}")

#     mlflow.set_experiment("Iris Model Selection")
#     with mlflow.start_run(run_name=f"Best_Model_{best_model_name}"):
#         mlflow.log_params(best_params)
#         mlflow.log_metric("best_accuracy", best_models[best_model_name]['best_score'])
#         mlflow.log_params({"model_name": best_model_name})
#         run_id = mlflow.active_run().info.run_id
#         mlflow.log_params({"mlflow_run_id": run_id})

#     best_model_info = {
#         'best_model_name': best_model_name,
#         'best_params': best_params,
#         'mlflow_run_id': run_id
#     }

#     with open('best_model_and_params.json', 'w') as f:
#         json.dump(best_model_info, f, indent=4)

#     return best_model_name, best_params

# if __name__ == "__main__":
#     perform_hyperparameter_optimization()
#     # Gracefully shutdown Ray after optimization
#     ray.shutdown()


import json
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import accuracy_score
# import mlflow

def train_model(config, model_name):
    data = pd.read_csv('data/processed/iris_cleaned.csv')
    X = data.drop('target', axis=1)
    y = data['target']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    
    if model_name == 'SVM':
        model = SVC(C=config["C"], kernel=config["kernel"])
    elif model_name == 'RandomForest':
        model = RandomForestClassifier(n_estimators=config["n_estimators"], max_depth=config["max_depth"])
    elif model_name == 'KNN':
        model = KNeighborsClassifier(n_neighbors=config["n_neighbors"])
    else:
        raise ValueError(f"Unknown model name {model_name}")

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    
    return accuracy, config

def perform_hyperparameter_optimization():
    search_space = {
        'SVM': [
            {'C': 0.1, 'kernel': 'linear'},
            {'C': 0.1, 'kernel': 'rbf'},
            {'C': 1, 'kernel': 'linear'},
            {'C': 1, 'kernel': 'rbf'},
            {'C': 10, 'kernel': 'linear'},
            {'C': 10, 'kernel': 'rbf'}
        ],
        'RandomForest': [
            {'n_estimators': 10, 'max_depth': 5},
            {'n_estimators': 10, 'max_depth': 10},
            {'n_estimators': 10, 'max_depth': 20},
            {'n_estimators': 50, 'max_depth': 5},
            {'n_estimators': 50, 'max_depth': 10},
            {'n_estimators': 50, 'max_depth': 20},
            {'n_estimators': 100, 'max_depth': 5},
            {'n_estimators': 100, 'max_depth': 10},
            {'n_estimators': 100, 'max_depth': 20}
        ],
        'KNN': [
            {'n_neighbors': 3},
            {'n_neighbors': 5},
            {'n_neighbors': 7}
        ]
    }

    best_models = {}
    for model_name, configs in search_space.items():
        best_accuracy = -1
        best_config = None
        for config in configs:
            accuracy, config = train_model(config, model_name)
            if accuracy > best_accuracy:
                best_accuracy = accuracy
                best_config = config
        
        print(f"Best Model for {model_name}: {best_config}")
        print(f"Best Score: {best_accuracy}")

        best_models[model_name] = {
            'best_config': best_config,
            'best_score': best_accuracy
        }

    best_model_name = max(best_models, key=lambda model: best_models[model]['best_score'])
    best_params = best_models[best_model_name]['best_config']

    print(f"Best overall model: {best_model_name} with params {best_params}")

    # mlflow.set_experiment("Iris Model Selection")
    # with mlflow.start_run(run_name=f"Best_Model_{best_model_name}"):
    #     mlflow.log_params(best_params)
    #     mlflow.log_metric("best_accuracy", best_models[best_model_name]['best_score'])
    #     mlflow.log_params({"model_name": best_model_name})
    #     run_id = mlflow.active_run().info.run_id
    #     mlflow.log_params({"mlflow_run_id": run_id})

    best_model_info = {
        'best_model_name': best_model_name,
        'best_params': best_params,
        # 'mlflow_run_id': run_id
    }

    with open('best_model_and_params.json', 'w') as f:
        json.dump(best_model_info, f, indent=4)

    return best_model_name, best_params

if __name__ == "__main__":
    perform_hyperparameter_optimization()