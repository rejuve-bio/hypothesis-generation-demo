"""
GWAS Library Handler

Manages the GWAS library collection in MongoDB, storing metadata for GWAS files
that can be downloaded on-demand and cached in MinIO.
"""

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Dict, Optional
from loguru import logger
from .base_handler import BaseHandler
from src.utils import gwas_file_has_n_column

# FinnGen release used by scripts/finngen_manifest_parser.py to build both our
# library filenames and OpenTargets' study_id (see scripts/load_credible_sets_to_postgres.py).
FINNGEN_RELEASE = "R12"

_GCST_ACCESSION_RE = re.compile(r"(GCST\d+)", re.IGNORECASE)



@dataclass(frozen=True)
class SampleSizeResolution:
    value: Optional[int]
    source: str
    is_user_provided: bool
    message: str

    @property
    def is_editable(self) -> bool:
        return self.source != "library_verified"

    @property
    def prefill_value(self) -> Optional[int]:
        """Value the UI should autofill; None when the file already has N."""
        return self.value

    @property
    def pipeline_value(self) -> int:
        """N passed to harmonization when the file lacks a per-variant N column."""
        if self.value is not None:
            return self.value
        return GWASLibraryHandler.DEFAULT_SAMPLE_SIZE_FALLBACK

    def to_api_dict(self) -> dict:
        return {
            "sample_size": self.value,
            "sample_size_source": self.source,
            "sample_size_message": self.message,
            "sample_size_is_user_provided": self.is_user_provided,
            "sample_size_editable": self.is_editable,
            "sample_size_prefill": self.prefill_value,
        }


class GWASLibraryHandler(BaseHandler):
    """Handler for GWAS library collection"""

    DEFAULT_SAMPLE_SIZE_FALLBACK = 10_000

    @staticmethod
    def sample_size_fields_for_library_entry(
        entry: Dict, resolution: SampleSizeResolution
    ) -> dict:
        """Shared sample-size API fields for library list and preview endpoints."""
        return {
            **resolution.to_api_dict(),
            "default_sample_size": entry.get("default_sample_size"),
        }

    @classmethod
    def resolve_sample_size_info(
        cls,
        entry: Optional[Dict] = None,
        form_sample_size: Optional[int] = None,
    ) -> SampleSizeResolution:
        """
        Resolve N for harmonization / LDSC with provenance for API display.
        """
        if form_sample_size is not None:
            value = int(form_sample_size)
            return SampleSizeResolution(
                value=value,
                source="user_provided",
                is_user_provided=True,
                message=f"Using user-provided sample size (N={value:,}).",
            )
        if entry:
            if entry.get("sample_size") is not None:
                value = int(entry["sample_size"])
                return SampleSizeResolution(
                    value=value,
                    source="library_verified",
                    is_user_provided=False,
                    message=(
                        f"Using verified sample size from library metadata "
                        f"(N={value:,})."
                    ),
                )
            if entry.get("default_sample_size") is not None:
                value = int(entry["default_sample_size"])
                message = (
                    f"No verified sample size found. Using library default sample "
                    f"size (N={value:,})."
                )
            else:
                value = cls.DEFAULT_SAMPLE_SIZE_FALLBACK
                message = (
                    f"No verified sample size found. Using default sample size "
                    f"(N={value:,})."
                )
            return SampleSizeResolution(
                value=value,
                source="library_default",
                is_user_provided=False,
                message=message,
            )
        value = cls.DEFAULT_SAMPLE_SIZE_FALLBACK
        return SampleSizeResolution(
            value=value,
            source="upload_default",
            is_user_provided=False,
            message=(
                f"No sample size provided. Using default sample size (N={value:,}) "
                f"when your file lacks an N column."
            ),
        )

    @staticmethod
    def resolve_opentargets_study_id(
        entry: Optional[Dict] = None, uploaded_filename: Optional[str] = None
    ) -> Optional[str]:
        """
        Best-effort resolution of the OpenTargets study_id for a selected GWAS
        file, so the analysis pipeline can look up precomputed credible sets.

        - FinnGen library entries: OpenTargets indexes FinnGen studies under
          f"FINNGEN_{release}_{phenotype_code}" (verified against the
          credible-sets DB, e.g. phenotype_code "K11_UC_STRICT2" ->
          "FINNGEN_R12_K11_UC_STRICT2").
        - GWAS-Catalog-accessioned uploads: if the filename still carries its
          original GCST accession (OpenTargets/GWAS Catalog's own harmonised
          file naming convention), that accession *is* the study_id.
        - UK Biobank (Neale round 2) library entries have no corresponding
          OpenTargets study under any known identifier, so this returns None
          for them (verified: not just absent, never deposited under any
          accession).
        """
        if entry and entry.get("source") == "FinnGen":
            phenotype_code = entry.get("phenotype_code")
            if phenotype_code:
                return f"FINNGEN_{FINNGEN_RELEASE}_{phenotype_code.upper()}"

        for filename in (uploaded_filename, entry.get("filename") if entry else None):
            if not filename:
                continue
            match = _GCST_ACCESSION_RE.search(filename)
            if match:
                return match.group(1).upper()

        return None

    @classmethod
    def resolve_upload_sample_size_info(
        cls, file_path: Optional[str] = None
    ) -> SampleSizeResolution:
        """Preview sample-size resolution for a user-uploaded GWAS file."""

        if file_path and gwas_file_has_n_column(file_path):
            return SampleSizeResolution(
                value=None,
                source="file_has_n",
                is_user_provided=False,
                message=(
                    "Your GWAS file includes an N column; per-variant sample sizes "
                    "will be used during harmonization."
                ),
            )
        return cls.resolve_sample_size_info(entry=None, form_sample_size=None)

    @classmethod
    def resolve_pipeline_sample_size(
        cls,
        entry: Optional[Dict] = None,
        form_sample_size: Optional[int] = None,
    ) -> int:
        """
        N passed to harmonization / LDSC: explicit form value, else verified library
        sample_size, else default_sample_size from library metadata, else global
        fallback (10,000).
        """
        return cls.resolve_sample_size_info(entry, form_sample_size).pipeline_value

    def __init__(self, mongodb_uri: str, db_name: str):
        """
        Initialize the GWAS library handler
        
        Args:
            mongodb_uri (str): MongoDB connection URI
            db_name (str): Database name
        """
        super().__init__(mongodb_uri, db_name)
        self.collection = self.db['gwas_library']
        
        # Create indexes for efficient queries
        self._create_indexes()
    
    def _create_indexes(self):
        """Create indexes on commonly queried fields"""
        try:
            # Unique index on file_id (filename is the unique identifier)
            self.collection.create_index('file_id', unique=True)
            
            # Index on phenotype_code for searching (not unique - can be N/A or duplicate)
            self.collection.create_index('phenotype_code')
            
            # Index on display_name for searching
            self.collection.create_index('display_name')
            
            # Index on sex for filtering
            self.collection.create_index('sex')

            # Index on source for filtering (UK Biobank vs FinnGen)
            self.collection.create_index('source')
            
            # Index on downloaded status (cached in MinIO)
            self.collection.create_index('downloaded')
            
            # Text index for full-text search
            self.collection.create_index([
                ('display_name', 'text'),
                ('description', 'text'),
                ('phenotype_code', 'text'),
                ('filename', 'text')
            ])
            
            logger.info("GWAS library indexes created successfully")
        except Exception as e:
            logger.warning(f"Could not create indexes (may already exist): {e}")
    
    @staticmethod
    def _search_substring_query(search_term: str) -> dict:
        """Substring match on key string fields (case-insensitive)."""
        escaped = re.escape(search_term.strip())
        return {
            "$or": [
                {"file_id": {"$regex": escaped, "$options": "i"}},
                {"filename": {"$regex": escaped, "$options": "i"}},
                {"display_name": {"$regex": escaped, "$options": "i"}},
                {"description": {"$regex": escaped, "$options": "i"}},
                {"phenotype_code": {"$regex": escaped, "$options": "i"}},
            ]
        }

    def get_gwas_entry(self, file_id: str) -> Optional[Dict]:
        """
        Get a GWAS entry by file_id (filename)
        
        Args:
            file_id (str): File ID (filename)
            
        Returns:
            dict or None: GWAS entry if found
        """
        try:
            entry = self.collection.find_one({'file_id': file_id})
            
            if entry:
                # Remove MongoDB _id field
                entry.pop('_id', None)
            
            return entry
            
        except Exception as e:
            logger.error(f"Error getting GWAS entry {file_id}: {e}")
            return None
    
    def get_all_gwas_entries(
        self, 
        search_term: Optional[str] = None,
        sex_filter: Optional[str] = None,
        source_filter: Optional[str] = None,
        limit: int = 100,
        skip: int = 0
    ) -> List[Dict]:
        """
        Get all GWAS entries with optional filtering and pagination
        
        Args:
            search_term (str, optional): Substring search on file_id, filename, display_name, description, phenotype_code
            sex_filter (str, optional): Filter by sex ('both_sexes', 'male', 'female')
            source_filter (str, optional): Filter by data source ('UK Biobank', 'FinnGen')
            limit (int): Maximum number of entries to return
            skip (int): Number of entries to skip (for pagination)
            
        Returns:
            list: List of GWAS entries
        """
        try:
            # Build query
            query = {}

            if search_term and search_term.strip():
                query.update(self._search_substring_query(search_term))

            if sex_filter:
                query['sex'] = sex_filter

            if source_filter:
                query['source'] = source_filter

            # Execute query with pagination
            cursor = self.collection.find(query).skip(skip).limit(limit)
            
            # Sort by download count (most popular first) and then by display name
            cursor = cursor.sort([
                ('download_count', -1),
                ('display_name', 1)
            ])
            
            entries = []
            for entry in cursor:
                # Remove MongoDB _id field
                entry.pop('_id', None)
                entries.append(entry)
            
            return entries
            
        except Exception as e:
            logger.error(f"Error getting GWAS entries: {e}")
            return []
    
    def get_entry_count(
        self,
        search_term: Optional[str] = None,
        sex_filter: Optional[str] = None,
        source_filter: Optional[str] = None,
    ) -> int:
        """
        Get count of GWAS entries matching filters
        
        Args:
            search_term (str, optional): Search term
            sex_filter (str, optional): Filter by sex
            source_filter (str, optional): Filter by data source
            
        Returns:
            int: Count of matching entries
        """
        try:
            query = {}

            if search_term and search_term.strip():
                query.update(self._search_substring_query(search_term))

            if sex_filter:
                query['sex'] = sex_filter

            if source_filter:
                query['source'] = source_filter

            return self.collection.count_documents(query)
            
        except Exception as e:
            logger.error(f"Error counting GWAS entries: {e}")
            return 0

    def get_source_counts(self) -> List[Dict]:
        """
        Return entry counts grouped by source for library tab UI.

        Returns:
            list: [{"name": "UK Biobank", "count": N}, ...]
        """
        try:
            pipeline = [
                {"$group": {"_id": "$source", "count": {"$sum": 1}}},
                {"$sort": {"_id": 1}},
            ]
            results = []
            for doc in self.collection.aggregate(pipeline):
                name = doc["_id"] or "Unknown"
                results.append({"name": name, "count": doc["count"]})
            return results
        except Exception as e:
            logger.error(f"Error getting source counts: {e}")
            return []
    
    def update_gwas_entry(self, file_id: str, update_data: Dict) -> bool:
        """
        Update a GWAS entry
        
        Args:
            file_id (str): File ID (filename)
            update_data (dict): Fields to update
            
        Returns:
            bool: True if successful
        """
        try:
            # Add updated_at timestamp
            update_data['updated_at'] = datetime.now(timezone.utc)
            
            result = self.collection.update_one(
                {'file_id': file_id},
                {'$set': update_data}
            )
            
            if result.modified_count > 0:
                logger.info(f"Updated GWAS entry: {file_id}")
                return True
            else:
                logger.warning(f"No changes made to GWAS entry: {file_id}")
                return False
            
        except Exception as e:
            logger.error(f"Error updating GWAS entry {file_id}: {e}")
            return False
    
    def mark_as_downloaded(self, file_id: str, minio_path: str, file_size: int) -> bool:
        """
        Mark a GWAS entry as downloaded and cached in MinIO
        
        Args:
            file_id (str): File ID (filename)
            minio_path (str): MinIO object key where file is cached
            file_size (int): File size in bytes
            
        Returns:
            bool: True if successful
        """
        try:
            update_data = {
                'downloaded': True,
                'minio_path': minio_path,
                'file_size': file_size,
                'last_accessed': datetime.now(timezone.utc),
                'updated_at': datetime.now(timezone.utc)
            }
            
            result = self.collection.update_one(
                {'file_id': file_id},
                {'$set': update_data}
            )
            
            if result.modified_count > 0:
                logger.info(f"Marked as downloaded: {file_id} -> s3://{minio_path}")
                return True
            else:
                logger.warning(f"Failed to mark as downloaded: {file_id}")
                return False
            
        except Exception as e:
            logger.error(f"Error marking {file_id} as downloaded: {e}")
            return False
    
    def increment_download_count(self, file_id: str) -> bool:
        """
        Increment the download count for a GWAS entry
        
        Args:
            file_id (str): File ID (filename)
            
        Returns:
            bool: True if successful
        """
        try:
            result = self.collection.update_one(
                {'file_id': file_id},
                {
                    '$inc': {'download_count': 1},
                    '$set': {
                        'last_accessed': datetime.now(timezone.utc),
                        'updated_at': datetime.now(timezone.utc)
                    }
                }
            )
            
            if result.modified_count > 0:
                logger.debug(f"Incremented download count for: {file_id}")
                return True
            else:
                logger.warning(f"Failed to increment download count: {file_id}")
                return False
            
        except Exception as e:
            logger.error(f"Error incrementing download count for {file_id}: {e}")
            return False
    
    def clear_collection(self) -> int:
        """Delete all entries from the collection. Returns deleted count."""
        result = self.collection.delete_many({})
        logger.info(f"Cleared {result.deleted_count} entries from gwas_library")
        return result.deleted_count

    def bulk_create_gwas_entries(
        self,
        entries: List[Dict],
        *,
        skip_existing: bool = False,
    ) -> Dict:
        """
        Bulk insert GWAS entries into the collection
        
        Args:
            entries (List[Dict]): List of GWAS entries to insert
            
        Returns:
            dict: inserted_count, updated_count, skipped_existing_count
        """
        try:
            from datetime import datetime, timezone
            
            inserted_count = 0
            updated_count = 0
            skipped_existing_count = 0
            
            for entry in entries:
                if skip_existing and self.collection.find_one(
                    {"file_id": entry["file_id"]}, {"_id": 1}
                ):
                    skipped_existing_count += 1
                    continue
                # Add timestamps
                entry['created_at'] = datetime.now(timezone.utc)
                entry['updated_at'] = datetime.now(timezone.utc)
                
                # Add default fields if not present
                entry.setdefault('downloaded', False)
                entry.setdefault('download_count', 0)
                entry.setdefault('minio_path', None)
                entry.setdefault('file_size', None)
                entry.setdefault('last_accessed', None)
                entry.setdefault(
                    'default_sample_size',
                    self.DEFAULT_SAMPLE_SIZE_FALLBACK,
                )
                entry.setdefault('genome_build', None)
                # Repopulate: clear stale sample_size when parser omits it (no CSV / no scrape).
                if 'sample_size' not in entry:
                    entry['sample_size'] = None
                
                try:
                    result = self.collection.update_one(
                        {"file_id": entry["file_id"]},
                        {"$set": entry},
                        upsert=True,
                    )
                    if result.upserted_id:
                        inserted_count += 1
                    else:
                        updated_count += 1
                except Exception as e:
                    logger.warning(f"Error upserting entry {entry.get('file_id')}: {e}")
                    updated_count += 1
            
            logger.info(
                f"Bulk upsert complete: {inserted_count} inserted, {updated_count} updated, "
                f"{skipped_existing_count} skipped (existing)"
            )
            
            return {
                'inserted_count': inserted_count,
                'updated_count': updated_count,
                'skipped_existing_count': skipped_existing_count,
            }
            
        except Exception as e:
            logger.error(f"Error during bulk insert: {e}")
            raise