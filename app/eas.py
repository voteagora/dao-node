import os
import json
from urllib import request, error

from web3 import Web3


EAS_ATTESTED = 'Attested(address,address,bytes32,bytes32)'
EAS_REVOKED = 'Revoked(address,address,bytes32,bytes32)'

EAS_EVENT_SIGNATURES = (EAS_ATTESTED, EAS_REVOKED)

EAS_CONTRACTS = {
    1: "0xA1207F3BBa224E2c9c3c6D5aF63D0eb1582Ce587",
    11155111: "0xC2679fBD37d54388Ce493F1DB75320D236e1815e",
    10: "0x4200000000000000000000000000000000000021",
    11155420: "0x4200000000000000000000000000000000000021",
    8453: "0x4200000000000000000000000000000000000021",
}

OODAO_SCHEMAS = {
    11155111: {
        'INSTANTIATE': '0xa45718ef6b8758277682e9914ed85b960e19fd8331ed75e24641d228b7efcd2d',
        'CREATE_PROPOSAL_TYPE': '0x4147434e77680f972dcaa494427b876fa0f5ecdfde56131dd24a988ad90a6950',
        'CREATE_PROPOSAL': '0x38bfba767c2f41790962f09bcf52923713cfff3ad6d7604de7cc77c15fcf169a',
        'SET_PROPOSAL_TYPE': '0xa6ca209ead271e33d86bf969fb5b9d5f559bf3fb22765ede70652b1faa4973b5',
        'SET_PARAM_VALUE': '0xa1e21d322b14d3d79bd697b106b7374e19a61eb766907ef27d392dd635d9642f',
        'SIMPLE_VOTE': '0x19c36b80a224c4800fd6ed68901ec21f591563c8a5cb2dd95382d430603f91ff',
        'ADVANCED_VOTE': '0x991b014c62b19364882fc89dbf3baa6104b4598ee2c4f29152be2cbcfcb4cb81',
        'BADGE_DEFINITION': '0x44c0a23e342cc3b74cae094dd9be5b38447ec67045ccea4868c74d6387a52fca',
        'IDENTITY_BADGE': '0x0b9dd04e9927bd43e11ddd02c31a971859ffc23abed1a0eb226fa13bfd5046d4',
    },
    1: {
        'INSTANTIATE': '0x4564d3a746bafcf78838969daaff3ba173e9f5ab73ac3023ea98dd9220953e75',
        'CREATE_PROPOSAL_TYPE': '0xab4e473a3f8a0a0a490619bcd2ecf23ad1be4720d4033fa16f2d1cbd1519caa1',
        'CREATE_PROPOSAL': '0x38bfba767c2f41790962f09bcf52923713cfff3ad6d7604de7cc77c15fcf169a',
        'SET_PROPOSAL_TYPE': '0x0519039455b478a51b33c82934bf28814f5755f6bb21c20fc47fd98c2a3fafa3',
        'SET_PARAM_VALUE': '0x5c27757206150b56764617513558234fc12ab9bb64eee71afb525fa9689c4842',
        'SIMPLE_VOTE': '0x12cd8679de42e111a5ece9f2aee44dc8b8351024dea881cda97c2ff5b58349f6',
        'ADVANCED_VOTE': '0xc4465af5d96b474b1c7a6418500461d3de1fc35552679bf695eb2b3124817dce',
    },
    8453: {
        'INSTANTIATE': '0xbab63b7b41b5350afcf392b0f5b6031ea8990a6fee079ef523851ee28a4a7b74',
        'CREATE_PROPOSAL_TYPE': '0xf3f87e20caefe2aac522d0eae399b03851c32d28840979c6e47c4575be2b089a',
        'CREATE_PROPOSAL': '0x38bfba767c2f41790962f09bcf52923713cfff3ad6d7604de7cc77c15fcf169a',
        'SET_PROPOSAL_TYPE': '0xd9a4856778d6c4ad4f692f5e498bc757f96890c101bcbf5b76b847604929fb50',
        'SET_PARAM_VALUE': '0x8e20525ba72e66262408abc256801c048b7291e6801bd3f0612c5dd72d33f74a',
        'SIMPLE_VOTE': '0x72edbb9603b8ff8ae5310c1d33912f4a7998bea0c03afc0e06a64e41d32b78b9',
        'ADVANCED_VOTE': '0xadeeda60a3496bacee2d9fb250dc3b2af802d2623feb31e211cfc8f210119fea',
        'BADGE_DEFINITION': '0x37b439c809cec771782b95da77af9d1ed1724fe5d4ce14bce55b5b8c2373fe30',
        'IDENTITY_BADGE': '0x29283083ab37d4c00e1f2186492d9df145e9648229da70810c36da04f35c2e13',
    },
}


def event_topic(signature):
    return "0x" + Web3.keccak(text=signature).hex()


def topic_to_address(topic_hex):
    if not topic_hex:
        return None
    if isinstance(topic_hex, bytes):
        topic_hex = "0x" + topic_hex.hex()
    if topic_hex.startswith("0x"):
        return "0x" + topic_hex[-40:].lower()
    return "0x" + topic_hex[-40:].lower()


def extract_uid(topic2, data_hex):
    """
    EAS versions differ in indexed fields.
    If topic2 looks like an address slot, fallback to the first word in data.
    """
    if isinstance(topic2, bytes):
        topic2 = "0x" + topic2.hex()

    if isinstance(data_hex, bytes):
        data_hex = "0x" + data_hex.hex()
    elif isinstance(data_hex, str) and not data_hex.startswith("0x"):
        data_hex = "0x" + data_hex

    if isinstance(topic2, str):
        low = topic2.lower()
        padded_addr = low.startswith("0x000000000000000000000000")
        if not padded_addr:
            return low

    if isinstance(data_hex, str) and data_hex.startswith("0x") and len(data_hex) >= 66:
        return "0x" + data_hex[2:66].lower()

    return topic2.lower() if isinstance(topic2, str) else None


class EASDecodeClient:
    def __init__(self):
        self.base_url = os.getenv("DAO_NODE_BLOCKCACHE_URL", "http://localhost:8000").rstrip("/")
        self.alchemy_key = os.getenv("ALCHEMY_API_KEY", "")
        self._cache = {}

    def get_decoded_attestation(self, chain_id, uid):
        cache_key = f"{chain_id}:{uid.lower()}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        url = f"{self.base_url}/decoded_eas/{chain_id}/attestation/{uid}"
        headers = {}
        if self.alchemy_key:
            headers["alchemy-api-key"] = self.alchemy_key

        req = request.Request(url, headers=headers)
        try:
            with request.urlopen(req, timeout=20) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except error.URLError:
            payload = {"attestation": {}, "decoded_data": {}}

        attestation = payload.get("attestation", {})
        out = {
            "decoded_attestation": payload.get("decoded_data", {}) or {},
            "ref_uid": (attestation.get("refUID") or "").lower(),
            "attestation_time": int(attestation.get("time") or 0),
            "expiration_time": int(attestation.get("expirationTime") or 0),
            "attester": (attestation.get("attester") or "").lower(),
            "recipient": (attestation.get("recipient") or "").lower(),
        }
        self._cache[cache_key] = out
        return out
