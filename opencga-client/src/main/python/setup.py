from distutils.core import setup

setup(
    name='pyCGA',
    version='1.0.7',
    packages=['pyCGA', 'pyCGA.Utils'],
    url='',
    license='',
    author='antonior,dapregi,ernesto-ocampo',
    author_email='antonio.rueda-martin@genomicsengland.co.uk,daniel.perez-gil@genomicsengland.co.uk,ernesto.ocampo@genomicsengland.co.uk',
    description='',
    install_requires=[
        'pip >= 7.1.2',
        'requests >= 2.7',
        'avro == 1.7.7',
        'pathlib >= 1.0.1',
        'requests_toolbelt >= 0.7.0',
        'PyYAML'
    ],
)
