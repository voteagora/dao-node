#!/usr/bin/env python3
from dotenv import load_dotenv

load_dotenv()

import os
import subprocess
from argh import arg, dispatch_commands
import yaml
from pprint import pprint
from pathlib import Path

@arg('dir', help='Disk or RAM directory to download blobs into.')
def sync_from_gcs(dir: str, multi_processing=False, strict=False):

    agora_config_file = os.environ.get('AGORA_CONFIG_FILE')
    dao_node_gcloud_bucket = os.environ.get('DAO_NODE_GCLOUD_BUCKET')
    contract_deployment = os.environ.get('CONTRACT_DEPLOYMENT')

    print(f"agora_config_file={agora_config_file}")

    if not dao_node_gcloud_bucket:
        raise ValueError("The environment variable DAO_NODE_GCLOUD_BUCKET must be set.")
    if not contract_deployment:
        raise ValueError("The environment variable CONTRACT_DEPLOYMENT must be set.")

    with open(agora_config_file, "r") as file:
        config = yaml.safe_load(file)

    pprint(config)

    deployment = config['deployments'][contract_deployment]

    chain_id = deployment['chain_id']

    addresses = []

    for contract in deployment.keys():

        address = None

        try:
            address = deployment[contract].get('address', None)
        except:
            pass

        if address:
            addresses.append(address.lower())

    for address in addresses:

        source_path = f"gs://{dao_node_gcloud_bucket}/v1/snapshot/v1/events/{chain_id}/{address}"

        print(f"Syncing archive event data from : {source_path}")

        cmd = ["gsutil"]

        if multi_processing:
            cmd.append("-m")

        dest = (Path(dir) / str(chain_id))

        dest.mkdir(parents=True, exist_ok=True)
        
        cmd.extend(["cp", "-D", "-r", source_path, str(dest.absolute())])

        try:
            subprocess.check_call(cmd)
            print(f"Successfully synced from {source_path} to {dir}")
        except subprocess.CalledProcessError as e:
            if strict:
                raise RuntimeError(f"Failed to sync from GCS: {e}")
            else:
                cmd = " ".join(cmd)
                print(f"Warning, cmd : '{cmd}' finished with error: {e}")

if __name__ == '__main__':
    dispatch_commands([sync_from_gcs])