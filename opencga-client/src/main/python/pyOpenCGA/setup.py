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

setup(
    name='pyopencga',
    version='1.4.0',
    description='A REST client for OpenCGA REST web services',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    url='https://github.com/opencb/opencga/tree/develop/opencga-client/src/main/python/pyOpenCGA',
    packages=['pyopencga', 'pyopencga.rest_clients'],
    license='Apache Software License',
    author='David Gomez-Peregrina, Pablo Marín-García',
    author_email='david.gomez@mgviz.org, pmarin@kanteron.com',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
    ],
    keywords='opencb opencga bioinformatics genomic database',
    install_requires=[
        'requests >= 2.7',
        'pip >= 7.1.2',
        'pathlib >= 1.0.1',
        'pyyaml >= 3.12'
    ],
    project_urls={
        'Documentation': 'http://docs.opencb.org/display/opencga/Python',
        'Source': 'https://github.com/opencb/opencga/tree/develop/opencga-client/src/main/python/pyOpenCGA',
        'OpenCGA': 'https://github.com/opencb/opencga',
        'OpenCGA Documentation': 'http://docs.opencb.org/display/opencga',
        'Bug Reports': 'https://github.com/opencb/opencga/issues',
    }
)
