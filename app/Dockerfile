FROM python:3.11-slim


# Install git and clean up apt cache to keep the image small
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*
    
RUN pip install gsutil

WORKDIR /app

# copy the dependencies file to the working directory
COPY requirements.txt .

# install dependencies
RUN pip install -r requirements.txt  --no-cache-dir

# copy the content of the local dao_node directory to the working directory
COPY . .
EXPOSE 8000
CMD ["sanic", "server.app", "--host=0.0.0.0", "--port=8000"]
