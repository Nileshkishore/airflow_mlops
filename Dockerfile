
FROM python:3.12-slim

WORKDIR /app

# Copy only the registerd_model directory into /app
COPY registerd_model/ /app/model/

# Copy other files to the root of the working directory
COPY file_report.html iris_profile_report.html app.py ./

# Install dependencies in a single layer
RUN pip install --no-cache-dir -r /app/model/requirements.txt

EXPOSE 5000

CMD ["python", "app.py"]


# FROM python:3.12-slim

# WORKDIR /app
# COPY file_report.html .
# COPY iris_profile_report.html .

# COPY registerd_model/ /app/model/
# RUN pip install --no-cache-dir -r model/requirements.txt
# COPY app.py .

# EXPOSE 5000

# CMD ["python", "app.py"]