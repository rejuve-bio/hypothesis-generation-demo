import requests
import csv
import pickle
import os
from io import StringIO
from datetime import datetime, timedelta

class HGNCSymbolProcessor:
    def __init__(self):
        self.base_data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
        self.hgnc_to_ensembl_path = os.path.join(self.base_data_path, 'test_hgnc_to_ensembl.pkl')
        self.ensembl_to_hgnc_path = os.path.join(self.base_data_path, 'test_ensembl_to_hgnc.pkl')
        self.version_file_path = os.path.join(self.base_data_path, 'test_hgnc_version.txt')
        self.update_interval = timedelta(hours=48)  # Update every 48 hours
        self.hgnc_url = "https://www.genenames.org/cgi-bin/download/custom?col=gd_app_sym&col=gd_pub_ensembl_id&status=Approved&format=text&submit=submit"

    def check_update_needed(self) -> bool:
        """Check if data needs to be updated based on last update time."""
        if not os.path.exists(self.version_file_path):
            print("Version file not found. Update needed.")
            return True

        with open(self.version_file_path, 'r') as f:
            last_update_str = f.read().strip()

        try:
            last_update = datetime.fromisoformat(last_update_str)
            if datetime.now() - last_update > self.update_interval:
                print("Update interval exceeded. Update needed.")
                return True
            print("Data is up-to-date. No update needed.")
            return False
        except ValueError:
            print("Invalid version file format. Forcing update.")
            return True

    def save_update_time(self):
        """Save the current time as the last update time."""
        os.makedirs(self.base_data_path, exist_ok=True)
        current_time = datetime.now().isoformat()
        with open(self.version_file_path, 'w') as f:
            f.write(current_time)
        print(f"Saved update time: {current_time}")

    def fetch_and_process_data(self):
        """Fetch data from HGNC and process it into mappings."""
        print("Fetching HGNC data...")
        try:
            response = requests.get(self.hgnc_url, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return

        hgnc_to_ensembl = {}
        ensembl_to_hgnc = {}

        reader = csv.DictReader(StringIO(response.text), delimiter='\t')
        for row in reader:
            hgnc_symbol = row.get('Approved symbol')
            ensembl_id = row.get('Ensembl gene ID')
            if hgnc_symbol and ensembl_id:
                hgnc_to_ensembl[hgnc_symbol] = ensembl_id
                ensembl_to_hgnc[ensembl_id] = hgnc_symbol

        
        sorted_hgnc_to_ensembl = dict(sorted(hgnc_to_ensembl.items()))
        sorted_ensembl_to_hgnc = dict(sorted(ensembl_to_hgnc.items()))

        self.save_pickle(self.hgnc_to_ensembl_path, sorted_hgnc_to_ensembl)
        self.save_pickle(self.ensembl_to_hgnc_path, sorted_ensembl_to_hgnc)
        print("Mappings updated successfully.")

    def save_pickle(self, file_path, data):
        """Save data to a pickle file."""
        os.makedirs(self.base_data_path, exist_ok=True)
        with open(file_path, 'wb') as f:
            pickle.dump(data, f)
        print(f"Data saved to {file_path}.")

    def load_pickle(self, file_path):
        """Load data from a pickle file."""
        if os.path.exists(file_path):
            with open(file_path, 'rb') as f:
                return pickle.load(f)
        print(f"File {file_path} not found.")
        return {}

    def update_data(self):
        """Update data if needed."""
        if self.check_update_needed():
            self.fetch_and_process_data()
            self.save_update_time()
        else:
            print("Using existing data.")

if __name__ == "__main__":
    processor = HGNCSymbolProcessor()
    processor.update_data()  
