"""Minimal setup.py for matsya."""
from setuptools import setup, find_packages

setup(
    name="matsya",
    version="0.1.0",
    description="AI-powered web crawler with LLM-guided extraction",
    author="Officethree Technologies",
    license="MIT",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.9",
)
