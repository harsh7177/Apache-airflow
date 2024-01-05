import tarfile
import os

def unzip(tar_file_path, destination_path):
    with tarfile.open(tar_file_path, 'r:gz') as tar_ref:
        tar_ref.extractall(destination_path)

# Example usage
tar_file_path = r"C:\Users\gusai\OneDrive\Desktop\DE\tolldata.tgz"
destination_path = r"C:\Users\gusai\OneDrive\Desktop\DE\unzip-data"

untar(tar_file_path, destination_path)

        