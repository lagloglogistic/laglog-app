import sys
import os

# Replace 'DEIN_USERNAME' with your PythonAnywhere username
project_home = '/home/DEIN_USERNAME/laglog'
if project_home not in sys.path:
    sys.path.insert(0, project_home)

# Set data directory to persistent home folder
os.environ['DATA_DIR'] = '/home/DEIN_USERNAME/laglog/data'

from app import app as application
