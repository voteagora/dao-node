# DAO Node

A service for DAO-centric applications to serve blazing fast chain data backed by an in-ram data model maintained at tip.  

# Vision

A service that optimizes for serving data (post-boot performance), flexibility (time-to-market of data-model changes), testability, and hosting costs for multi-region applications.

This service relies on:
- A config file setting contracts, per the Agora YAML spec
- Publically hosted ABIs for each contracts
- A JSON-RPC Endpoint
- Optionally, archive data sources can be used to accelerate boot-times for large DAOs


## Getting Started <a name="getstarted"></a>

DAO Node requires a dedicated Python >= 3.11 environment and/or Docker, along with basic knowledge of both to setup.  It should run easily on Windows, Mac or Linux.

DAO Node needs the ABIs for every contract used by the service, hosted at a public URL, with one file per contract.  

For example, if your DAO is built using foundry, you would save 3 ABIs...

```
jq '.abi' mydao-contracts/out/MyToken.sol/MyToken.json > 0x4200000000000000000000000000000000000042.json
jq '.abi' mydao-contracts/out/ProposalTypesConfigurator.sol/ProposalTypesConfigurator.json > 0x67ecA7B65Baf0342CE7fBf0AA15921524414C09f.json
jq '.abi' mydao-contracts/out/AgoraGovernor.sol/AgoraGovernor.json > 0xcDF27F107725988f2261Ce2256bDfCdE8B382B10.json
```

...then host those three files somewhere.  Set `ABI_URL` to the path.

Alternatively, a PR to search for the ABIs before reaching out to the internet would be accepted. 

### Setup a YAML Config File

A sample of the minimum compliment for a YAML config is below.  Save this as `mydao-config.yaml` somewhere accessible to DAO Node.  Set `AGORA_CONFIG_FILE` to this file.

```
friendly_short_name: MyDAO

token_spec: 
  name: erc20
  version: '?' # U -> Uniswap's style, E -> ENS's style, G -> latest github hosted most starred standard, '?' -> unknown

governor_spec:
  name: agora
  version: 1.0

deployments:
  main:
    chain_id: 10
    token: 
      address: '0x4200000000000000000000000000000000000042'
    gov: 
      address: '0xcDF27F107725988f2261Ce2256bDfCdE8B382B10'
    ptc:
      address: '0x67ecA7B65Baf0342CE7fBf0AA15921524414C09f'
```

### How to run the project <a name="run"></a>

#### Via Python

##### Sync Archive Data

```
AGORA_CONFIG_FILE="/path/to/your/config.yaml" 
DAO_NODE_GCLOUD_BUCKET="daonode-us-public" 
CONTRACT_DEPLOYMENT="main" 
python -m app.cli sync-from-gcs data
```

#### Via Docker

##### Build
```bash
cd app
docker build -t daonode .
```

##### Run
```bash
docker run daonode -e AGORA_CONFIG_FILE="/path/to/your/mydao-config.yaml" ABI_URL="http://your-abi-host.com/abis/" CONTRACT_DEPLOYMENT="main" DAO_NODE_ARCHIVE_NODE_HTTP="http://your-geth-node" DAO_NODE_REALTIME_NODE_WS="ws://your-geth-node"
```

# Error Codes

DAO Node uses a warn-and-beg-forgiveness style of flagging errors in a material portion of the code base.  The logic is that there could be say 1 missing event that compromises the data for say 1 user, but that shouldn't stop the server from booting.  If there is application logic that has an issue, rather than the data source, this can mean a barfing of tracebacks that overwhelm stdout and any human looking at the logs.  

So we use error codes rather than printing full stack trackes for known high risk failure modes.

The Error code number is of the form "E{line_number_as_of_date_added}{YYMMDD}{optional-suffix}".  This means error codes can be easily created by devs without thinking too hard or checking some index, but it's likely impossible for a collission as well.

# API Endpoint Support

🚀 - Prod Grade + Performance Tests & Integration Tests Exists

✅ - Prod Grade (Functional & Unit Tests Exists)

🚧 - WIP, Buyer Beware

🕓 - Not Supported, but Planned Soon

❌ - Not Supported, No ETA

| Endpoint | AG 0.1 | Bravo | OZ 4.x | AG 1.x & OZ 5.x | AG 2.x |
|----------|-------|----------------|---------|---------|---------|
| `/balances/{address}` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `/proposals` | 🚧 | - | - | - | - |
| `/proposals/{id}` | 🚧 | - | - | - | - |
| `/proposal-types`  | - | - | - | - | - |
| `/proposal-types/{id}` | - | - | - | - | - |
| `/delegates` | 🚧 | - | - | - | - |
| `/delegates/{address}` | 🚧 | - | - | - | - |
| `/delegates/{address}/voting-power/{block}` | 🚧 | - | - | - | - |
| `/voting-power` | - | - | - | - | - |
| `/health` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `/config` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `/deployment` | ✅ | ✅ | ✅ | ✅ | ✅ |