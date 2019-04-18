try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='pyOpenCGA',
    version='0.8',
    description='A REST client for OpenCGA web services',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    packages=['pyopencga', 'pyopencga.Utils', 'pyopencga.rest_clients'],
    license='Apache Software License',
    author='David Gomez-Peregrina',
    author_email='david.gomez@mgviz.org',
    keywords='opencb opencga bioinformatics genomic database',
    install_requires=[
        'pip >= 7.1.2',
        'requests >= 2.7',
        'avro == 1.7.7',
        'pathlib >= 1.0.1',
        'requests_toolbelt >= 0.7.0',
        'pyyaml',
        'retrying'
    ],
    project_urls={
        'Documentation': 'http://docs.opencb.org/display/opencga/RESTful+Web+Services+and+Clients',
        'Source': 'https://github.com/opencb/opencga/tree/develop/opencga-client/src/main/python',
        'OpenCGA': 'https://github.com/opencb/opencga',
        'OpenCGA Documentation': 'http://docs.opencb.org/display/opencga',
        'Bug Reports': 'https://github.com/opencb/opencga/issues',
    }
)

