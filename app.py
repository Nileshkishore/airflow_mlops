from flask import Flask, request, jsonify, render_template_string, send_file
import mlflow.pyfunc
import traceback
import sys
import numpy as np
import os

app = Flask(__name__)

# Load the model
try:
    model = mlflow.pyfunc.load_model("/app/model")  # <-- docker 
    print("Model loaded successfully", file=sys.stderr)
except Exception as e:
    print(f"Error loading model: {str(e)}", file=sys.stderr)
    print(traceback.format_exc(), file=sys.stderr)

# Default HTML form
default_page = '''
<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Predictor</title>
</head>
<body>
    <h1>Predictor Form</h1>
    <form action="/" method="post">
        <label for="input1">sepal length (cm):</label>
        <input type="text" id="input1" name="input1" required><br><br>
        <label for="input2">sepal width (cm):</label>
        <input type="text" id="input2" name="input2" required><br><br>
        <label for="input3">petal length (cm):</label>
        <input type="text" id="input3" name="input3" required><br><br>
        <label for="input4">petal width (cm):</label>
        <input type="text" id="input4" name="input4" required><br><br>
        <input type="submit" value="Predict">
    </form>
    {% if prediction is not none %}
    <h2>Prediction Result:</h2>
    <p>{{ prediction }}</p>
    {% endif %}
    <br>
    <a href="/datadrift">View Data Drift Report</a>
    <br>
    <a href="/data_profiling">View Data profile Report</a>
</body>
</html>
'''

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        try:
            # Retrieve inputs from form
            input1 = float(request.form.get('input1'))
            input2 = float(request.form.get('input2'))
            input3 = float(request.form.get('input3'))
            input4 = float(request.form.get('input4'))

            # Convert inputs to numpy array
            input_data = np.array([[input1, input2, input3, input4]])
            
            # Get predictions
            predictions = model.predict(input_data)
            result = predictions.tolist()

            return render_template_string(default_page, prediction=result)
        except Exception as e:
            print(f"Error occurred: {str(e)}", file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)
            return render_template_string(default_page, prediction=f"Error: {str(e)}")

    return render_template_string(default_page, prediction=None)

@app.route('/predict', methods=['POST'])
def predict():
    try:
        # Ensure request is in JSON format
        if request.content_type != 'application/json':
            raise ValueError("Content-Type must be 'application/json'")
        
        data = request.json
        print(f"Received data: {data}", file=sys.stderr)
        
        # Convert input data to numpy array
        input_data = np.array(data['data'])
        print(f"Converted input data: {input_data}", file=sys.stderr)
        
        predictions = model.predict(input_data)
        print(f"Predictions: {predictions}", file=sys.stderr)
        
        return jsonify(predictions.tolist())
    except Exception as e:
        print(f"Error occurred: {str(e)}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        return jsonify({"error": str(e)}), 500

@app.route('/datadrift')
def datadrift():
    """Serve the Data Drift report as an HTML file."""
    try:
        # Path to your saved report
        report_path = "file_report.html"
        
        if os.path.exists(report_path):
            return send_file(report_path)
        else:
            return "Report file not found", 404
    except Exception as e:
        print(f"Error occurred: {str(e)}", file=sys.stderr)
        return "An error occurred", 500
    

@app.route('/data_profiling')
def data_profile():
    """Serve the Data Drift report as an HTML file."""
    try:
        # Path to your saved report
        report_path = "iris_profile_report.html"
        
        if os.path.exists(report_path):
            return send_file(report_path)
        else:
            return "Report file not found", 404
    except Exception as e:
        print(f"Error occurred: {str(e)}", file=sys.stderr)
        return "An error occurred", 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
