# Start from the official Airflow image
FROM apache/airflow:2.7.1-python3.9

# Copy the requirements file into the image's working directory
COPY requirements.txt /opt/airflow/

# Switch to the 'airflow' user for security
USER airflow

# Install Python dependencies from the requirements file
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt