# Use official Python image as base
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir 'faststream[cli]'

# Copy the rest of the application code
COPY . .

# Expose port (adjust if your app uses a different port)
EXPOSE 8000

# Set environment variables (optional, uncomment if needed)
# ENV SOME_ENV_VAR=some_value

# Run the application
CMD ["python", "modern_ui_server.py"]