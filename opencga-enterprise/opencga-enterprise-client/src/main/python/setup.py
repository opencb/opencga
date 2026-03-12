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
    name='pyxetabase',
    version='PYXETABASE_VERSION',
    description='A REST client for OpenCGA enterprise REST web services',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    packages=['pyxetabase', 'pyxetabase.rest_clients'],
    license='Apache Software License',
    author='Daniel Perez-Gil',
    author_email='daniel.perez@zettagenomics.com',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
    ],
    keywords='zetta-genomics opencga-enterprise bioinformatics genomic database',
    install_requires=[
        'requests >= 2.7',
        'pip >= 7.1.2',
        'pathlib >= 1.0.1',
        'pyyaml >= 3.12',
        'pandas >= 1.1.5',
        'flask >= 2.0.3'
    ],
    project_urls={
        'OpenCGA-enterprise': 'https://github.com/zetta-genomics/opencga-enterprise',
        'Bug Reports': 'https://github.com/zetta-genomics/opencga-enterprise/issues',
    }
)
