from setuptools import setup, find_packages
from pkg_resources import parse_requirements
import os 

req_file_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')

with open(req_file_path) as f:
    req_file = f.read()
    reqs = [str(ir) for ir in parse_requirements(req_file)]

setup(
    long_description=open('README.md', 'r').read(),
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    install_requires=reqs,
    long_description_content_type = "text/markdown"
)