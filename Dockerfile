# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY ledger/requirements.txt ledger/

# Copy the shared directory into the container at /app
COPY shared/ shared/

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r ledger/requirements.txt

# Copy the current directory contents into the container at /app
COPY ledger/ ledger/

# Set the working directory to the service folder
WORKDIR /app/ledger

# Run main.py when the container launches
CMD ["python", "main.py"]
