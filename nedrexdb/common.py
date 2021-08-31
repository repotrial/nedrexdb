import os
from contextlib import contextmanager


@contextmanager
def change_directory(directory: str):
    """Context manager to temporarily change to a specified directory"""
    current_directory = os.path.abspath(".")
    os.chdir(directory)
    yield
    os.chdir(current_directory)
