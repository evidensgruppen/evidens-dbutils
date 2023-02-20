from setuptools import setup

# För att Geopandas ska kunna användas krävs att även psycopg2 är installerat. Detta installeras  
# dock enklast med den binära versionen psycopg2-binary. Enligt officiell dokumentation bör denna 
# dock inte läggas in i install_requires i setup(), se nedan:

# The psycopg2-binary package is meant for beginners to start playing with Python and PostgreSQL 
# without the need to meet the build requirements.

# If you are the maintainer of a published package depending on psycopg2 you shouldn’t use 
# psycopg2-binary as a module dependency. For production use you are advised to use the source 
# distribution.

# Källa:
# https://www.psycopg.org/docs/install.html#install-from-source

setup(
    name='dbutils',
    version='0.1',
    description='Återkommande databasfunktioner',
    author='oj',
    license='BSD 2-clause',
    packages=['dbutils'],
    install_requires=[
        'sqlalchemy',
        'pymysql',
        'pandas>=1.3.3'
    ],
)



