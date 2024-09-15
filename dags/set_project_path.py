import sys
import os

def set_project_path():
    project_root = os.path.abspath(os.path.dirname(__file__) + '/../')
    sys.path.append(project_root)