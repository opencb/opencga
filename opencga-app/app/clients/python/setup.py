from distutils.core import setup
import os
reqs = [
    "requests>=2.7",
    "avro==1.7.7",
    "requests_toolbelt"
]
setup(
    name='pyCGA',
    packages=['pyCGA', 'pyCGA.Utils'],
    version='0.2.1',
    scripts=[os.path.join(os.path.dirname(__file__),'pyCGA/Scripts/pyCGA'), os.path.join(os.path.dirname(__file__),'pyCGA/Scripts/pyCGAIdConverter'),
             os.path.join(os.path.dirname(__file__),'pyCGA/Scripts/pyCGAVariantFetcher'), os.path.join(os.path.dirname(__file__),'pyCGA/Scripts/pyCGALogin')],
    url='',
    license='',
    author='antonio',
    author_email='antonio.rueda-martin@genomicsengland.co.uk',
    description='',
    install_requires=reqs,
)
