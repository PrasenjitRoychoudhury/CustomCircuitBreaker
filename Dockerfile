FROM python:3.11-slim

# Install boto3 library
RUN pip install boto3 flask awscli

# Copy your Python script to the container
COPY HelloService.py /app/

# Set the working directory
WORKDIR /app

# Run your Python script when the container starts
# CMD ["python", "HelloService.py"]
CMD ["python", "-u", "HelloService.py"]