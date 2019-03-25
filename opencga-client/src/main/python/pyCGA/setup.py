import os 
import sys 

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

requirements=[
    'pip >= 7.1.2',
    'requests >= 2.7',
    'pathlib >= 1.0.1',
    'requests_toolbelt >= 0.7.0',
    'PyYAML'
]

enforced_version = os.environ.get("ARVO_PYTHON_VERSION", None)
interpreter_version = str(sys.version_info[0])
target_version = enforced_version if enforced_version else interpreter_version
if target_version == '2':
    requirements += ["avro==1.7.7"]
elif target_version == '3':
    requirements += ["avro-python3==1.8.2"]
else:
    raise ValueError("Not supported python version {}".format(target_version))

setup(
    name='pyCGA',
    version='1.3.1',
    description='A REST client for OpenCGA web services',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    packages=['pyCGA', 'pyCGA.Utils'],
    url='https://github.com/opencb/opencga/tree/develop/opencga-client/src/main/python',
    license='Apache Software License',
    author='antonior,dapregi,ernesto-ocampo',
    author_email='antonio.rueda-martin@genomicsengland.co.uk,daniel.perez-gil@genomicsengland.co.uk,kenan.mcgrath@genomicsengland.co.uk',
    keywords='opencb opencga bioinformatics genomic database',
    install_requires=requirements,
    extras_require={'test':['httpretty', 'sure']},
    project_urls={
        'Documentation': 'http://docs.opencb.org/display/opencga/RESTful+Web+Services+and+Clients',
        'Source': 'https://github.com/opencb/opencga/tree/develop/opencga-client/src/main/python',
        'OpenCGA': 'https://github.com/opencb/opencga',
        'OpenCGA Documentation': 'http://docs.opencb.org/display/opencga',
        'Bug Reports': 'https://github.com/opencb/opencga/issues',
    }
)
