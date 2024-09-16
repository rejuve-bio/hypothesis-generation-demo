# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container
COPY . /app

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose port 5000 for Flask
EXPOSE 5001

# Command to run your Flask app with the specified arguments
CMD ["python", "main.py", "--ensembl-hgnc-map", "data/ensembl_to_hgnc.pkl", "--hgnc-ensembl-map", "data/hgnc_to_ensembl.pkl", "--go-map", "data/go_map.pkl"]
