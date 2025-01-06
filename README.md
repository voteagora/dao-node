# DAO Node

A service for DAO-centric applications to serve blazing fast data backed by an in-ram data model maintained at tip.  

# Vision

A service that optimizes for serving data (post-boot performance), flexibility (time-to-market of data-model changes), testability, and hosting costs for multi-region clients.

This service relies on:
- A config file setting contracts, per the Agora YAML spec
- Publically hosted ABIs for each contracts
- A JSON-RPC Endpoint
- Optionally, archive data sources can be used to accelerate boot-times for large DAOs


## Introduction <a name="introduction"></a>

The service relies on booting using archives of events pulled from a waterfall of sources, each with speed-vs-cost trade-offs.

## Getting Started <a name="getstarted"></a>

To this project assumes a dedicate Python >= 3.11 environment and/or Docker, along with basic knowledge of both to setup.  It should run easily on Windows, Mac or Linux.

### Setup your ABIs

DAO Node needs the ABIs for every contract used by the service, hosted at a public URL, with one file per contract.  

For example, if your DAO is built using foundry, you would save 3 ABIs...

```
jq '.abi' mydao-contracts/out/MyToken.sol/MyToken.json > 0x4200000000000000000000000000000000000042.json
jq '.abi' mydao-contracts/out/ProposalTypesConfigurator.sol/ProposalTypesConfigurator.json > 0x67ecA7B65Baf0342CE7fBf0AA15921524414C09f.json
jq '.abi' mydao-contracts/out/AgoraGovernor.sol/AgoraGovernor.json > 0xcDF27F107725988f2261Ce2256bDfCdE8B382B10.json
```

...then host those three files somewhere.  Set `ABI_URL` to the path.

Alternatively, a patch to search for the ABIs on disk would be accepted. 

### Setup a YAML Config File

Here is a sample of the minimum compliment for a YAML config.  Save this as mydao-config.yaml somewhere accessible.  Set `AGORA_CONFIG_FILE` to this file.

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
