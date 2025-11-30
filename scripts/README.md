# Scripts Directory

## populate_gwas_library.py

Script to populate the GWAS library collection from a UK Biobank manifest file.

### Prerequisites

1. MongoDB must be running and accessible
2. Environment variables must be set:
   ```bash
   export MONGODB_URI="mongodb://localhost:27017"
   export DB_NAME="hypothesis_db"
   ```

### Usage

#### 1. Validate Manifest File

Before populating the database, validate your manifest file:

```bash
python scripts/populate_gwas_library.py path/to/manifest.tsv --validate-only
```

This will:
- Parse the manifest file
- Validate all entries
- Report any issues
- **Not** insert into database

#### 2. Dry Run

Test the population process without modifying the database:

```bash
python scripts/populate_gwas_library.py path/to/manifest.tsv --dry-run
```

This will:
- Parse and validate the manifest
- Show sample entries that would be inserted
- **Not** insert into database

#### 3. Populate Database

Populate the database with GWAS library entries:

```bash
python scripts/populate_gwas_library.py path/to/manifest.tsv
```

This will:
- Parse and validate the manifest
- Insert valid entries into MongoDB
- Skip entries that already exist (by phenotype_code)
- Report results

#### 4. Clear and Re-populate

**⚠️ DESTRUCTIVE**: This will delete all existing entries before populating:

```bash
python scripts/populate_gwas_library.py path/to/manifest.tsv --clear
```

Use this option only when:
- Starting fresh
- Updating the entire catalog
- You have a backup of the data

### Manifest File Format

The manifest file should be a TSV (tab-separated) or CSV file with the following columns:

| Column Name | Description | Required |
|------------|-------------|----------|
| Phenotype Code | UK Biobank phenotype/field code | ✓ |
| Phenotype Description | Full description of the phenotype | ✓ |
| UK Biobank Data Showcase Link | URL to UK Biobank showcase page | |
| Sex | Sex category (both_sexes, male, female) | |
| File | Original filename | |
| wget command | Command to download file via wget | * |
| AWS File | AWS S3 URL | * |
| Dropbox File | Dropbox URL | * |
| md5s | MD5 checksum | |

\* At least one download method (wget, AWS, or Dropbox) is required

### Example Manifest

See `data/manifest_example.tsv` for a sample manifest file.

### Output Example

```
==============================================================
GWAS Library Population Script
==============================================================
Manifest file: data/manifest.tsv
Dry run: False
Validate only: False
==============================================================

[1/4] Parsing manifest file...
✓ Parsed 150 entries from manifest

[2/4] Validating entries...
✓ Validation complete:
  - Total entries: 150
  - Valid entries: 148
  - Invalid entries: 2

⚠ Found 2 invalid entries:
  - 99999: Missing description
  - 88888: No download method available (wget_command, aws_url, or dropbox_url)

[3/4] Sample valid entries (first 5):

  1. 21001 - Body Mass Index (BMI)
     Description: Body mass index...
     Sex: both_sexes
     Source: UK Biobank
     Download available: True

  2. 50 - Standing Height
     Description: Standing height...
     Sex: both_sexes
     Source: UK Biobank
     Download available: True

  ...

[4/4] Inserting into database...
✓ Connected to MongoDB
Inserting 148 entries...
✓ Successfully inserted 148 entries
✓ Total entries in collection: 148

==============================================================
✓ GWAS library population complete!
==============================================================
```

### Troubleshooting

#### Error: "Manifest file not found"
- Check that the file path is correct
- Use absolute path if relative path doesn't work

#### Error: "Missing required MongoDB configuration"
- Ensure MONGODB_URI and DB_NAME environment variables are set
- Or provide them via command-line:
  ```bash
  python scripts/populate_gwas_library.py manifest.tsv \
    --mongodb-uri "mongodb://localhost:27017" \
    --db-name "hypothesis_db"
  ```

#### Error: "Failed to connect to MongoDB"
- Ensure MongoDB is running
- Check connection string is correct
- Verify network access to MongoDB server

#### Warning: "No entries were inserted"
- Entries may already exist in database (identified by phenotype_code)
- Use `--clear` option to delete existing entries first (⚠️ destructive)

### Advanced Usage

#### Custom MongoDB Connection

```bash
python scripts/populate_gwas_library.py manifest.tsv \
  --mongodb-uri "mongodb://user:pass@host:27017/?authSource=admin" \
  --db-name "my_database"
```

#### Pipeline Example

```bash
# Step 1: Download manifest from UK Biobank
wget https://example.com/ukbb_gwas_manifest.tsv -O data/manifest.tsv

# Step 2: Validate
python scripts/populate_gwas_library.py data/manifest.tsv --validate-only

# Step 3: Populate database
python scripts/populate_gwas_library.py data/manifest.tsv

# Step 4: Verify
python -c "
from db import GWASLibraryHandler
handler = GWASLibraryHandler('mongodb://localhost:27017', 'hypothesis_db')
print(f'Total entries: {handler.get_entry_count()}')
"
```

### Notes

- The script uses bulk insertion for efficiency
- Duplicate phenotype codes are skipped (unique constraint)
- Invalid entries are logged but don't stop the process
- Progress is shown in real-time during execution

