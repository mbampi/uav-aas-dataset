# Use a slim python image compatible with Raspberry Pi (ARM)
FROM python:3.9-slim

# Install the docker library to talk to the socket
RUN pip install docker

# Set working directory
WORKDIR /app

# Copy the script into the container
COPY resource_metrics.py .

# Create directory for output
RUN mkdir /output

# Run python in unbuffered mode (-u) so logs show up immediately
CMD ["python", "-u", "resource_metrics.py"]