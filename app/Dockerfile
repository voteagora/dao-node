FROM python:3.11-slim


# Install git and clean up apt cache to keep the image small
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*
    

# Steps to get compiled crcmod

# RUN apt-get install -y python3-pip gcc python3-dev
# Install the compiled version of crcmod
RUN pip3 install --no-cache-dir crcmod
# Clean up to reduce image size
# RUN apt-get remove -y gcc python3-dev && apt-get autoremove -y && apt-get clean


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
