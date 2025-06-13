import subprocess
import os
import tempfile
import requests
from loguru import logger
from prefect import task, flow
import time



def parse_variant(variant_input):
    """Parses a variant string into chromosome, position, ref, alt, or fetches details if rsID."""
    try:
        if variant_input.startswith("rs"):
            return fetch_variant_from_rsid(variant_input)
        else:
            # Regular variant format (chrom:pos:ref>alt)
            chrom, pos, ref_alt = variant_input.split(":")
            ref, alt = ref_alt.split(">")
            logger.debug(f"Parsed variant: chrom={chrom}, pos={pos}, ref={ref}, alt={alt}")
            return chrom, pos, ref, alt
    except Exception as e:
        logger.error(f"Error parsing variant input '{variant_input}': {e}")
        raise

def fetch_variant_from_rsid(rsid):
    """Fetch variant details for an rsID from Ensembl REST API"""
    try:
        ensembl_rsid = rsid
        url = f"https://rest.ensembl.org/variation/human/{ensembl_rsid}?content-type=application/json"
        response = requests.get(url)
        response.raise_for_status()  
        
        data = response.json()
        logger.debug(data)
        mappings = data.get('mappings', [])
        
        if not mappings:
            raise ValueError(f"No mappings found for {rsid}")

        mapping = mappings[0]
        chrom = mapping['seq_region_name']
        chrom = f"chr{chrom}" if chrom != "MT" else "chrM"

        pos = str(mapping['start'])
        alleles = mapping['allele_string'].split('/')
        ref, alt = alleles[0], alleles[1]

        logger.debug(f"Fetched variant: {chrom=}, {pos=}, {ref=}, {alt=}")
        return chrom, pos, ref, alt

    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed for {rsid}: {e}")
        raise
    except (KeyError, IndexError) as e:
        logger.error(f"Malformed API response for {rsid}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching {rsid}: {e}")
        raise


def create_tsv(chrom, pos, ref, alt, output_dir):
    """Creates a TSV file inside the given output directory."""
    try:
        os.makedirs(output_dir, exist_ok=True)
        tsv_file = os.path.join(output_dir, "input.tsv")
        with open(tsv_file, "w") as temp_file:
            temp_file.write("chrom\tpos\tref\talt\n")
            temp_file.write(f"{chrom}\t{pos}\t{ref}\t{alt}\n")
        logger.debug(f"Created TSV file at {tsv_file}")
        return tsv_file
    except Exception as e:
        logger.error(f"Error creating TSV file: {e}")
        raise

def generate_temp_dir():
    """Generates a temporary directory."""
    try:
        temp_dir = tempfile.mkdtemp()
        logger.debug(f"Generated temporary directory at {temp_dir}")
        return temp_dir
    except Exception as e:
        logger.error(f"Error generating temporary directory: {e}")
        raise

def run_opencravat(tsv_file, annotators, output_dir, genome_build):
    """
    Runs OpenCravat with the given genome build first, and if not given
    run with the default genomes.
    """
    def execute_opencravat(genome_build):
        oc_command = [
            'oc', 'run', tsv_file,
            '-l', genome_build,
            '-d', output_dir,
            '-a'
        ]
        oc_command.extend(annotators)
        try:
            result = subprocess.run(oc_command, capture_output=True, text=True, check=True)
            if not result.stdout:
                raise Exception("No output from OpenCravat")
            logger.success(f"OpenCravat command output ({genome_build}): {result.stdout}")
        except subprocess.CalledProcessError as e:
            logger.error(f"OpenCravat command failed ({genome_build}) with error: {e.stderr}")
            return None
        except Exception as e:
            logger.error(f"Error running OpenCravat ({genome_build}): {e}")
            return None

        sqlite_file = None
        try:
            for file in os.listdir(output_dir):
                if file.endswith(".sqlite"):
                    sqlite_file = os.path.join(output_dir, file)
                    break
            
            if sqlite_file:
                report = convert_to_json(sqlite_file)
                return report
            else:
                logger.error(f"No SQLite file found in the output directory ({genome_build}).")
                return None
        except Exception as e:
            logger.error(f"Error processing OpenCravat output ({genome_build}): {e}")
            return None

    # First attempt with the given genome build
    report = execute_opencravat(genome_build)
    if report and any(report.get("annotations", {}).values()):
        return report
    
    next_genome = "hg38" if genome_build == "hg19" else "hg19"
    
    # if the given genome build didnt return try with a different genome build
    logger.info(f"No annotations found with {genome_build}. Retrying with {next_genome}.")

    return execute_opencravat(next_genome)

def convert_to_json(sqlite_file):
    """
    Converts the OpenCravat SQLite output into a JSON-compatible dictionary.
    """
    import sqlite3
    try:
        conn = sqlite3.connect(sqlite_file)
        cursor = conn.cursor()

        result = {
            "annotations": {},
            "variant": {}
        }

        cursor.execute("SELECT * FROM variant")
        variant_row = cursor.fetchone()
        if variant_row:
            columns = [col[0] for col in cursor.description]
            variant_data = dict(zip(columns, variant_row))
            for col in columns:
                if col.startswith("base__"):
                    json_key = col.replace("base__", "", 1)
                    result["variant"][json_key] = variant_data[col]

        cursor.execute("SELECT * FROM gene")
        gene_row = cursor.fetchone()
        if gene_row:
            gene_columns = [col[0] for col in cursor.description]
            gene_data = dict(zip(gene_columns, gene_row))

        annotator_names = set()

        cursor.execute("SELECT name FROM variant_annotator")
        for row in cursor.fetchall():
            annotator_names.add(row[0])

        cursor.execute("SELECT name FROM gene_annotator")
        for row in cursor.fetchall():
            annotator_names.add(row[0])

        # Skip these annotators
        skip_annotators = {"base", "original_input", "tagsampler"}

        for annotator in annotator_names:
            if annotator in skip_annotators:
                continue


        for annotator in annotator_names:
            if annotator in skip_annotators:
                continue
            result["annotations"][annotator] = {}

            if gene_data and gene_columns:
                for col in gene_columns:
                    if col.startswith(f"{annotator}__"):
                        field = col.split("__", 1)[1]
                        if gene_data.get(col) is not None:
                            result["annotations"][annotator][field] = gene_data.get(col)

            if variant_row:
                for col in columns:
                    if col.startswith(f"{annotator}__"):
                        field = col.split("__", 1)[1]
                        if variant_data.get(col) is not None:
                            result["annotations"][annotator][field] = variant_data.get(col)
            

        logger.debug(f"Converted SQLite data to JSON for file {sqlite_file}")
        return result
    except Exception as e:
        logger.error(f"Error converting SQLite to JSON: {e}")
        return None
    finally:
        conn.close()

def clean_directory(output_dir):
    """Deletes all files in the output directory."""
    try:
        for file in os.listdir(output_dir):
            file_path = os.path.join(output_dir, file)
            os.remove(file_path)
        os.rmdir(output_dir) 
        logger.debug(f"Cleaned up and removed directory {output_dir}")
    except Exception as e:
        logger.warning(f"Error cleaning up directory {output_dir}: {e}")

def process_summary(variant_input, annotators, db, user_id, hypothesis_id, genome_build):    
    if db.check_processing_status(variant_input):
        logger.info("Variant is already being annotated. Waiting for the result...")
        max_wait_time = 180  # 3 minutes timeout
        wait_time = 0
        while db.check_processing_status(variant_input):
            if wait_time >= max_wait_time:
                return {"message": "Processing timeout. Please try again later."}, 408
            time.sleep(1)
            wait_time += 1
        global_summary = db.check_global_summary(variant_input)

        if global_summary:
            logger.debug("Retrieved global summary from saved db")
            #if a user-specific summary does not already exist, create one using the global summary.
            user_summary = db.check_summary(user_id, hypothesis_id)

            if not user_summary:
                db.create_summary(user_id, hypothesis_id, global_summary)

            return {
                "summary": global_summary.get('summary'),
                "hypothesis_id": hypothesis_id,
                "user_id": user_id
            }, 200
        
        return db.get_summary(user_id, hypothesis_id) or {"message": "Report retrieval failed."}, 500
    
    # Mark variant as processing
    db.set_processing_status(variant_input, True)
    
    try:
        output_dir = generate_temp_dir()
        chrom, pos, ref, alt = parse_variant(variant_input)
        tsv_file = create_tsv(chrom, pos, ref, alt, output_dir)

        if not genome_build:
            logger.info("User didn't provide a genome build, defaulting to hg38")
            genome_build = "hg38"

        report = run_opencravat(tsv_file, annotators, output_dir, genome_build)
    
    finally:
        # Clean up the directory after processing
        clean_directory(output_dir)
        # Mark as not processing
        db.set_processing_status(variant_input, False)
    
    if report is not None:
        db.create_global_summary(variant_input, report)
        return db.create_summary(user_id, hypothesis_id, report)
    
    return {"message": "Report generation failed."}, 500






    
