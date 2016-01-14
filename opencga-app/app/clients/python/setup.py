from distutils.core import setup
from pip.req import parse_requirements
import os
install_reqs = parse_requirements(os.path.join(os.path.dirname(__file__), "requirements.txt"))
reqs = [str(ir.req) for ir in install_reqs if ir.req is not None]
setup(
    name='pyCGA',
    version='0.1.2',
    packages=['pyCGA', 'pyCGA.Utils'],
    scripts=['pyCGA/Scripts/pyCGA', 'pyCGA/Scripts/pyCGAIdConverter',
             'pyCGA/Scripts/pyCGAVariantFetcher', 'pyCGA/Scripts/pyCGALogin'],
    url='',
    license='',
    author='antonio',
    author_email='antonio.rueda-martin@genomicsengland.co.uk',
    description='',
    install_requires=reqs,
)
