FROM python:3.11-slim

ARG GIT_COMMIT_SHA
ENV GIT_COMMIT_SHA=$GIT_COMMIT_SHA

# Install system dependencies needed for building the C extension
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip (optional but often recommended)
RUN pip install --no-cache-dir --upgrade pip

# Install crcmod, which will compile the C extension if a compiler is available
RUN pip install --no-cache-dir crcmod

# (Optionally) remove build-essential to keep image small, if you no longer need it:
# RUN apt-get remove -y build-essential && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

RUN pip install gsutil

WORKDIR /app

# copy the dependencies file to the working directory
COPY requirements.txt .

# install dependencies
RUN pip install -r requirements.txt --no-cache-dir

# copy the content of the local directory to the working directory
COPY . .

# Set PYTHONPATH to include the project root and tell Sanic where the app lives
ENV PYTHONPATH=/app
ENV SANIC_APP=app.server:app

# Expose the port the app runs on
EXPOSE 8000

# Command to run the application, honoring the PORT Railway provides
CMD ["sh", "-c", "sanic ${SANIC_APP} --host=0.0.0.0 --port=${PORT:-8000}"]
