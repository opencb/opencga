from distutils.core import setup

setup(
    name='pyCGA',
    version='1.2.0',
    packages=['pyCGA', 'pyCGA.Utils'],
    url='https://github.com/genomicsengland/opencga/tree/pycga-1.0/opencga-client/src/main/python',
    license='',
    author='antonior,dapregi,ernesto-ocampo',
    author_email='antonio.rueda-martin@genomicsengland.co.uk,daniel.perez-gil@genomicsengland.co.uk,ernesto.ocampo@genomicsengland.co.uk',
    description='Version 1.0.10: Changes in the AvroSchema module',
    install_requires=[
        'pip >= 7.1.2',
        'requests >= 2.7',
        'avro == 1.7.7',
        'pathlib >= 1.0.1',
        'requests_toolbelt >= 0.7.0',
        'PyYAML'
    ],
)
