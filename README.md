# DAO Node

A service for DAO-centric applications to serve blazing fast data backed by an in-ram data model maintained at tip.  

Note, it's not really a Node at all, and has nothing to do with NodeJS.

# Vision

A service that optimizes for serving data (post-boot performance), flexibility (time-to-market of data-model changes), testability, and hosting costs for multi-region clients.

This service will take the following config:
- A list of contracts relevant to a DAO
- A JSON-RPC Endpoint
- Optionally, pointers for one or more archive data sources

----
## Table of contents
- [Introduction]()
- [Get Started](#getstarted)
- [Init Git](#git)
- [How to run the project](#run)
- [Useful Commands](#commands)

----
## Introduction <a name="introduction"></a>

The service relies on booting using archives of events pulled from a waterfall of sources, each with speed-vs-cost trade-offs.

## Get Started <a name="getstarted"></a>

Before you can run this project, you need to have python installed.

You can use [pyenv](https://github.com/pyenv/pyenv) to create a virtual environment.

### Python >= 3.11:
Consider installing with [Homebrew](https://docs.brew.sh/):
```bash
brew update
# install pyenv by Homebrew
brew install pyenv
# confirm installation
pyenv --version  # pyenv 1.2.26
# create a python virtual env
pyenv virtualenv 3.11 daonode-env
# activate the virtual env
pyenv activate daonode-env
```

----
### How to run the project <a name="run"></a>

#### Via Python

TODO 

#### Via Docker

##### Build
```bash
cd app
docker build -t daonode .
```

##### Sync Archive Data for Tenant
```bash
docker run daonode -e AGORA_CONFIG_FILE="/path/to/your/config.yaml" DAO_NODE_GCLOUD_BUCKET="daonode-us-public" CONTRACT_DEPLOYMENT="main" python app/cli.py sync-from-gcs .
```

##### Run Serverfor Tenant
```bash
docker run daonode -e AGORA_CONFIG_FILE="/path/to/your/config.yaml" CONTRACT_DEPLOYMENT="main"
```

##### Run Service


----
### Deployment

// TODO: To ECS

----
### Dev commands  <a name="commands"></a>

Automatically create requirements
```bash
python -m pip freeze > requirements.txt
python -m pip install -r requirements.txt
```
