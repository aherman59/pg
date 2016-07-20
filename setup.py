"""
Configuration de la création du package pg

  python setup.py sdist

Le résultat est dans le répertoire dist

Installation de l'archive sur un autre environnement

  pip install pg-0.1.4.zip


"""

from setuptools import setup
from setuptools import find_packages


setup(
    name='pg',
    version='0.1.4',
    description='module facilitant les interactions avec une base PostgreSQL',
    author='Antoine HERMAN',
    author_email='antoine.herman@cerema.fr',
    url='http://www.cerema.fr',
    package_dir={'pg': 'pg'},
    packages = find_packages(),
    package_data = {'pg': ['*.sql']},
    include_package_data=True,
    install_requires=['psycopg2==2.6.1'],
)

# eof
