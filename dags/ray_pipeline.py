# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
# from airflow.utils.dates import days_ago
# from airflow.models import Variable
# import os
# from dotenv import load_dotenv
# import smtplib
# from email.mime.text import MIMEText
# from email.mime.multipart import MIMEMultipart
# import sys
# import ray  # Import Ray

# # Add the directory where src/ is located to the Python path
# sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# # Importing your pipeline steps from the existing modules
# from src.data_ingestion import ingest_data
# from src.preprocessing import preprocess_data
# from src.data_profiling import profile_data
# from src.train import load_best_params, train_best_model
# from src.hpt import perform_hyperparameter_optimization
# from src.download_best_model import download_model
# from src.datadrift import main
# from src.read_json import print_file_contents

# # Load environment variables from .env file
# load_dotenv()

# # Email configuration from environment variables
# SMTP_SERVER = os.getenv('SMTP_SERVER')
# SMTP_PORT = 587
# EMAIL_SENDER = os.getenv('EMAIL_SENDER')
# EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')
# EMAIL_RECEIVER = 'nileshkishore2001@gmail.com'

# def send_email(subject, body):
#     """Send an email with the specified subject and body."""
#     msg = MIMEMultipart()
#     msg['From'] = EMAIL_SENDER
#     msg['To'] = EMAIL_RECEIVER
#     msg['Subject'] = subject

#     msg.attach(MIMEText(body, 'plain'))

#     try:
#         with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
#             server.starttls()
#             server.login(EMAIL_SENDER, EMAIL_PASSWORD)
#             text = msg.as_string()
#             server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, text)
#     except Exception as e:
#         print(f"Failed to send email: {e}")

# # Define the DAG
# default_args = {
#     'owner': 'NILESH KISHORE',
#     'depends_on_past': False,
#     'email_on_failure': True,
#     'email_on_retry': False,
#     'retries': 1,
# }

# with DAG(
#     dag_id='pipeline_mlops_ray',
#     default_args=default_args,
#     description='A pipeline to run data ingestion, preprocessing, training, and drift detection',
#     schedule_interval='@daily',  # Adjust this to your schedule needs
#     start_date=days_ago(1),
#     catchup=False,
# ) as dag:

#     # Ray initialization function
#     def start_ray():
#         ray.init()

#     # Shutdown function for Ray
#     def stop_ray():
#         ray.shutdown()

#     # Functions to run with Ray
#     @ray.remote
#     def run_data_ingestion():
#         print("Running data ingestion...")
#         ingest_data()

#     @ray.remote
#     def run_data_drift():
#         print("Running data drift...")
#         main()

#     @ray.remote
#     def run_data_preprocessing():
#         print("Running data preprocessing...")
#         preprocess_data()

#     @ray.remote
#     def run_data_profiling():
#         print("Running data profiling...")
#         profile_data()

#     @ray.remote
#     def run_hyperparameter_optimization():
#         print("Running hyperparameter optimization...")
#         perform_hyperparameter_optimization()

#     @ray.remote
#     def run_model_training():
#         print("Training the best model...")
#         best_model_info = load_best_params()
#         best_model_name = best_model_info['best_model_name']
#         best_params = best_model_info['best_params']
#         train_best_model(best_model_name, best_params)

#     @ray.remote
#     def run_download_model():
#         print("Loading registered model...")
#         destination_folder = 'registered_model'
#         download_model(destination_folder)

#     @ray.remote
#     def run_json_reading():
#         print("Reading JSON...")
#         print_file_contents('best_model_and_params.json')

#     def send_success_email():
#         send_email(
#             "Pipeline Execution Successful",
#             "The pipeline executed successfully without errors."
#         )

#     def send_failure_email():
#         send_email(
#             "Pipeline Execution Failed",
#             "The pipeline failed. Please check Airflow logs for details."
#         )

#     # Create Airflow tasks for each step of your pipeline
#     start_ray_task = PythonOperator(
#         task_id='start_ray',
#         python_callable=start_ray
#     )

#     data_ingestion = PythonOperator(
#         task_id='data_ingestion',
#         python_callable=lambda: ray.get(run_data_ingestion.remote())
#     )

#     data_drift = PythonOperator(
#         task_id='data_drift',
#         python_callable=lambda: ray.get(run_data_drift.remote())
#     )

#     data_preprocessing = PythonOperator(
#         task_id='data_preprocessing',
#         python_callable=lambda: ray.get(run_data_preprocessing.remote())
#     )

#     data_profiling = PythonOperator(
#         task_id='data_profiling',
#         python_callable=lambda: ray.get(run_data_profiling.remote())
#     )

#     hyperparameter_optimization = PythonOperator(
#         task_id='hyperparameter_optimization',
#         python_callable=lambda: ray.get(run_hyperparameter_optimization.remote())
#     )

#     model_training = PythonOperator(
#         task_id='model_training',
#         python_callable=lambda: ray.get(run_model_training.remote())
#     )

#     download_model_task = PythonOperator(
#         task_id='download_model',
#         python_callable=lambda: ray.get(run_download_model.remote())
#     )

#     json_reading = PythonOperator(
#         task_id='json_reading',
#         python_callable=lambda: ray.get(run_json_reading.remote())
#     )

#     success_email = PythonOperator(
#         task_id='send_success_email',
#         python_callable=send_success_email,
#         trigger_rule='all_success'  # Only send success email if all tasks succeed
#     )

#     failure_email = PythonOperator(
#         task_id='send_failure_email',
#         python_callable=send_failure_email,
#         trigger_rule='one_failed'  # Send failure email if any task fails
#     )

#     # Get the build ID from Airflow variables
#     build_id = Variable.get('run_id', default_var='latest')  # Provide a default value if needed

#     # Define the Bash script execution task
#     run_docker_script = BashOperator(
#         task_id='run_bash_docker_script',
#         bash_command=f'cd /home/sigmoid/Documents/airflow_mlops && docker build -t gcr.io/nileshproject-435805/airflow_iris_model:{build_id} . && docker push gcr.io/nileshproject-435805/airflow_iris_model:{build_id}',
#     )

#     # Define the Bash script execution task
#     deploy_on_k8s = BashOperator(
#         task_id='deploy_on_k8s',
#         bash_command=f"""cd /home/sigmoid/Documents/airflow_mlops && sed -e 's|<DOCKER_IMAGE_TAGGED>|gcr.io/nileshproject-435805/airflow_iris_model:{build_id}|' k8s/combined-deployment-and-service.yaml > k8s/deployment-final.yaml""",
#     )
    

#     # Define task dependencies
#     start_ray_task >> [data_ingestion, data_drift, data_preprocessing, data_profiling, hyperparameter_optimization, model_training, download_model_task, json_reading]
#     [data_ingestion, data_drift, data_preprocessing, data_profiling, hyperparameter_optimization, model_training, download_model_task, run_docker_script, deploy_on_k8s] >> failure_email
#     data_ingestion >> data_drift >> data_preprocessing >> data_profiling
#     data_profiling >> hyperparameter_optimization >> model_training
#     model_training >> download_model_task >> json_reading
#     json_reading >> run_docker_script >> deploy_on_k8s >> success_email

#     # Stop Ray after all tasks are done
#     stop_ray_task = PythonOperator(
#         task_id='stop_ray',
#         python_callable=stop_ray
#     )

#     # Ensure Ray stops after all tasks are done
#     [data_ingestion, data_drift, data_preprocessing, data_profiling, hyperparameter_optimization, model_training, download_model_task, json_reading] >> stop_ray_task
