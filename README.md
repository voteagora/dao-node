# DAO Node

A service for DAO-centric applications to serve blazing fast data backed by an in-ram data model maintained at tip.  

Note, it's not really a Node at all, and has nothing to do with NodeJS.

# Vision

A service that optimizes for serving data (post-boot performance), flexibility (time-to-market of data-model changes), testability, and hosting costs for multi-region clients.

This service will take the following config:
- A list of contracts relevant to a DAO
- A JSON-RPC Endpoint
- Optionally, pointers for one or more archive data sources

