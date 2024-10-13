import os
from dotenv import load_dotenv

load_dotenv()

a = os.getenv('DB_NAME')
print(a)