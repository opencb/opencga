from distutils.core import setup

import os

setup(
    name='pyCGA',
    packages=['pyCGA', 'pyCGA.Utils'],
    version='0.2.4',
    scripts=[os.path.join(os.path.dirname(__file__),'pyCGA/Scripts/pyCGA'), os.path.join(os.path.dirname(__file__),'pyCGA/Scripts/pyCGAIdConverter'),
             os.path.join(os.path.dirname(__file__),'pyCGA/Scripts/pyCGAVariantFetcher'), os.path.join(os.path.dirname(__file__),'pyCGA/Scripts/pyCGALogin')],
    url='',
    license='',
    author='antonio',
    author_email='antonio.rueda-martin@genomicsengland.co.uk',
    description='',
    install_requires=[
        'pip >= 7.1.2',
        'requests >= 2.7',
        'avro == 1.7.7',
        'pathlib >= 1.0.1',
        'requests_toolbelt >= 0.7.0',

    ],
)
