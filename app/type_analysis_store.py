"""
Type Analysis Store for indexing, hashing, and comparing event type schemas.

This module stores type analysis results from different clients and supports
both memory-only and disk-persisted modes.
"""

import os
import json
import hashlib
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime

from .type_detector import detect_event_schema, schema_diff, schemas_match


# Environment variable to control disk persistence
PERSIST_TO_DISK = os.getenv('TYPE_ANALYZER_PERSIST_TO_DISK', 'false').lower() in ('true', '1')
OUTPUT_DIR = Path(os.getenv('TYPE_ANALYZER_OUTPUT_DIR', 'type_analysis_output'))


def compute_schema_hash(schema: dict) -> str:
    """Compute a SHA256 hash of a schema for quick comparison."""
    # Sort keys for deterministic hashing
    canonical = json.dumps(schema, sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


class TypeAnalysisEntry:
    """Stores type analysis for a single (client, chain, address, signature) combination."""
    
    def __init__(self, client_name: str, chain_id: int, address: str, signature: str):
        self.client_name = client_name
        self.chain_id = chain_id
        self.address = address
        self.signature = signature
        self.schema: Optional[dict] = None
        self.schema_hash: Optional[str] = None
        self.sample_count = 0
        self.samples: List[dict] = []
        self.max_samples = 5  # Keep up to 5 sample events
        self.last_updated: Optional[str] = None
    
    def update(self, event: dict) -> bool:
        """
        Update the entry with a new event.
        
        Returns True if the schema changed, False otherwise.
        """
        new_schema = detect_event_schema(event)
        schema_changed = False
        
        if self.schema is None:
            self.schema = new_schema
            self.schema_hash = compute_schema_hash(new_schema)
            schema_changed = True
        elif not schemas_match(self.schema, new_schema):
            # Schema changed - this is a type inconsistency within the same client!
            # Merge schemas to capture all observed types
            for key, type_str in new_schema.items():
                if key not in self.schema:
                    self.schema[key] = type_str
                    schema_changed = True
                elif self.schema[key] != type_str:
                    # Record both types seen
                    existing = self.schema[key]
                    if '|' not in existing or type_str not in existing:
                        self.schema[key] = f"{existing}|{type_str}"
                        schema_changed = True
            
            if schema_changed:
                self.schema_hash = compute_schema_hash(self.schema)
        
        self.sample_count += 1
        
        # Keep a few samples for debugging
        if len(self.samples) < self.max_samples:
            # Convert any non-serializable types for storage
            sample = self._make_serializable(event)
            self.samples.append(sample)
        
        self.last_updated = datetime.utcnow().isoformat()
        
        return schema_changed
    
    def _make_serializable(self, obj: Any) -> Any:
        """Convert an object to JSON-serializable form."""
        if isinstance(obj, bytes):
            return f"<bytes:{obj.hex()}>"
        if isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [self._make_serializable(v) for v in obj]
        return obj
    
    def to_dict(self) -> dict:
        """Convert to a dictionary for JSON serialization."""
        return {
            'signature': self.signature,
            'address': self.address,
            'client': self.client_name,
            'chain_id': self.chain_id,
            'schema_hash': self.schema_hash,
            'fields': self.schema,
            'sample_count': self.sample_count,
            'samples': self.samples,
            'last_updated': self.last_updated
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'TypeAnalysisEntry':
        """Create from a dictionary (for loading from disk)."""
        entry = cls(
            client_name=data['client'],
            chain_id=data['chain_id'],
            address=data['address'],
            signature=data['signature']
        )
        entry.schema = data.get('fields')
        entry.schema_hash = data.get('schema_hash')
        entry.sample_count = data.get('sample_count', 0)
        entry.samples = data.get('samples', [])
        entry.last_updated = data.get('last_updated')
        return entry


class TypeAnalysisStore:
    """
    Central store for all type analysis data.
    
    Indexes by (client_name, chain_id, address, signature) and supports
    both memory-only and disk-persisted modes.
    """
    
    def __init__(self, persist_to_disk: bool = None, output_dir: Path = None):
        self.persist_to_disk = persist_to_disk if persist_to_disk is not None else PERSIST_TO_DISK
        self.output_dir = output_dir or OUTPUT_DIR
        
        # Main storage: nested dict for fast lookup
        # entries[client_name][chain_id][signature][address] = TypeAnalysisEntry
        self.entries: Dict[str, Dict[int, Dict[str, Dict[str, TypeAnalysisEntry]]]] = \
            defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
        
        # Track schema changes for logging
        self.change_log: List[dict] = []
        
        if self.persist_to_disk:
            self._load_from_disk()
    
    def record_event(self, client_name: str, chain_id: int, address: str, 
                     signature: str, event: dict) -> bool:
        """
        Record a new event and update type analysis.
        
        Returns True if this caused a schema change.
        """
        address = address.lower()  # Normalize address
        
        # Get or create entry
        if address not in self.entries[client_name][chain_id][signature]:
            entry = TypeAnalysisEntry(client_name, chain_id, address, signature)
            self.entries[client_name][chain_id][signature][address] = entry
        else:
            entry = self.entries[client_name][chain_id][signature][address]
        
        schema_changed = entry.update(event)
        
        if schema_changed:
            self.change_log.append({
                'timestamp': datetime.utcnow().isoformat(),
                'client': client_name,
                'chain_id': chain_id,
                'address': address,
                'signature': signature,
                'new_hash': entry.schema_hash
            })
            
            if self.persist_to_disk:
                self._save_entry(entry)
        
        return schema_changed
    
    def get_entry(self, client_name: str, chain_id: int, address: str, 
                  signature: str) -> Optional[TypeAnalysisEntry]:
        """Get a specific entry."""
        address = address.lower()
        return self.entries[client_name][chain_id][signature].get(address)
    
    def get_all_signatures(self) -> List[str]:
        """Get all unique event signatures across all clients."""
        signatures = set()
        for client_data in self.entries.values():
            for chain_data in client_data.values():
                signatures.update(chain_data.keys())
        return sorted(signatures)
    
    def get_all_clients(self) -> List[str]:
        """Get all client names that have recorded data."""
        return sorted(self.entries.keys())
    
    def get_comparison(self, chain_id: int, signature: str) -> dict:
        """
        Get a side-by-side comparison of a signature across all clients.
        
        Returns a dict with client names as keys and their schemas/hashes.
        """
        result = {
            'chain_id': chain_id,
            'signature': signature,
            'clients': {},
            'hashes_match': True,
            'differences': []
        }
        
        hashes = set()
        schemas_by_client = {}
        
        for client_name in self.entries.keys():
            addresses = self.entries[client_name][chain_id][signature]
            if addresses:
                # Use the first address's schema (they should all be the same)
                entry = next(iter(addresses.values()))
                result['clients'][client_name] = {
                    'schema': entry.schema,
                    'hash': entry.schema_hash,
                    'sample_count': entry.sample_count,
                    'addresses': list(addresses.keys())
                }
                hashes.add(entry.schema_hash)
                schemas_by_client[client_name] = entry.schema
        
        result['hashes_match'] = len(hashes) <= 1
        
        # If there are differences, compute them
        if len(schemas_by_client) > 1:
            client_names = list(schemas_by_client.keys())
            for i, name1 in enumerate(client_names):
                for name2 in client_names[i+1:]:
                    diff = schema_diff(schemas_by_client[name1], schemas_by_client[name2])
                    if diff['only_in_first'] or diff['only_in_second'] or diff['type_mismatches']:
                        result['differences'].append({
                            'clients': [name1, name2],
                            'diff': diff
                        })
        
        return result
    
    def get_all_comparisons(self) -> List[dict]:
        """Get comparisons for all signatures."""
        comparisons = []
        
        # Collect all (chain_id, signature) pairs
        pairs = set()
        for client_data in self.entries.values():
            for chain_id, chain_data in client_data.items():
                for signature in chain_data.keys():
                    pairs.add((chain_id, signature))
        
        for chain_id, signature in sorted(pairs):
            comparison = self.get_comparison(chain_id, signature)
            comparisons.append(comparison)
        
        return comparisons
    
    def get_differences_only(self) -> List[dict]:
        """Get only the comparisons that have mismatches."""
        return [c for c in self.get_all_comparisons() if not c['hashes_match']]
    
    def get_hashes_summary(self) -> dict:
        """Get a summary of all schema hashes by client and signature."""
        summary = {}
        
        for client_name, client_data in self.entries.items():
            summary[client_name] = {}
            for chain_id, chain_data in client_data.items():
                for signature, addresses in chain_data.items():
                    if addresses:
                        entry = next(iter(addresses.values()))
                        key = f"{chain_id}.{signature}"
                        summary[client_name][key] = entry.schema_hash
        
        return summary
    
    def get_full_summary(self) -> dict:
        """Get a complete summary of all recorded data."""
        summary = {
            'clients': self.get_all_clients(),
            'signatures': self.get_all_signatures(),
            'total_entries': 0,
            'entries_by_client': {},
            'comparisons': self.get_all_comparisons(),
            'differences_count': len(self.get_differences_only())
        }
        
        for client_name, client_data in self.entries.items():
            count = 0
            for chain_data in client_data.values():
                for addresses in chain_data.values():
                    count += len(addresses)
            summary['entries_by_client'][client_name] = count
            summary['total_entries'] += count
        
        return summary
    
    # Disk persistence methods
    
    def _get_entry_path(self, entry: TypeAnalysisEntry) -> Path:
        """Get the file path for an entry."""
        # Structure: type_analysis_output/{chain_id}/{signature}/{client}/{address}.json
        safe_signature = entry.signature.replace('(', '_').replace(')', '_').replace(',', '_')
        return self.output_dir / str(entry.chain_id) / safe_signature / entry.client_name / f"{entry.address}.json"
    
    def _save_entry(self, entry: TypeAnalysisEntry):
        """Save a single entry to disk."""
        path = self._get_entry_path(entry)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(path, 'w') as f:
            json.dump(entry.to_dict(), f, indent=2)
    
    def _load_from_disk(self):
        """Load all entries from disk on startup."""
        if not self.output_dir.exists():
            return
        
        for json_file in self.output_dir.rglob('*.json'):
            if json_file.name == 'comparison_summary.json':
                continue
            
            try:
                with open(json_file) as f:
                    data = json.load(f)
                
                entry = TypeAnalysisEntry.from_dict(data)
                self.entries[entry.client_name][entry.chain_id][entry.signature][entry.address] = entry
            except Exception as e:
                print(f"Warning: Failed to load {json_file}: {e}")
    
    def save_comparison_summary(self):
        """Save a comparison summary to disk."""
        if not self.persist_to_disk:
            return
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
        summary_path = self.output_dir / 'comparison_summary.json'
        
        summary = self.get_full_summary()
        summary['generated_at'] = datetime.utcnow().isoformat()
        
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
    
    def print_console_summary(self):
        """Print a human-readable summary to console."""
        print("\n" + "=" * 70)
        print("TYPE ANALYSIS SUMMARY")
        print("=" * 70)
        
        clients = self.get_all_clients()
        print(f"\nClients: {', '.join(clients) if clients else 'None'}")
        
        comparisons = self.get_all_comparisons()
        
        if not comparisons:
            print("\nNo data recorded yet.")
            return
        
        matches = [c for c in comparisons if c['hashes_match']]
        mismatches = [c for c in comparisons if not c['hashes_match']]
        
        print(f"\nTotal event types: {len(comparisons)}")
        print(f"  ✓ Matching:    {len(matches)}")
        print(f"  ✗ Mismatched:  {len(mismatches)}")
        
        if mismatches:
            print("\n" + "-" * 70)
            print("MISMATCHES:")
            print("-" * 70)
            
            for comp in mismatches:
                print(f"\n  {comp['chain_id']}.{comp['signature']}")
                for diff_info in comp['differences']:
                    c1, c2 = diff_info['clients']
                    diff = diff_info['diff']
                    
                    if diff['only_in_first']:
                        print(f"    Only in {c1}: {list(diff['only_in_first'].keys())}")
                    if diff['only_in_second']:
                        print(f"    Only in {c2}: {list(diff['only_in_second'].keys())}")
                    if diff['type_mismatches']:
                        for field, types in diff['type_mismatches'].items():
                            print(f"    {field}: {c1}={types['first']} vs {c2}={types['second']}")
        
        print("\n" + "=" * 70 + "\n")


# Singleton instance for easy access
_store_instance: Optional[TypeAnalysisStore] = None


def get_store(persist_to_disk: bool = None, output_dir: Path = None) -> TypeAnalysisStore:
    """Get or create the singleton store instance."""
    global _store_instance
    if _store_instance is None:
        _store_instance = TypeAnalysisStore(persist_to_disk=persist_to_disk, output_dir=output_dir)
    return _store_instance


def reset_store():
    """Reset the singleton store (mainly for testing)."""
    global _store_instance
    _store_instance = None

