from setuptools import setup
from app._version import __version__

setup(
    name="dao-node",
    version=__version__,
    description="A blazing fast server for indexed, abstracted, objectified & forked-chain data to DAO clients.",
    author="DAO Node",
    author_email="jeff@voteagora.com",
    license="MIT license",
    python_requires=">=3.7",
)
