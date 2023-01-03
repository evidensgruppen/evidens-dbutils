from setuptools import setup

setup(
    name='dbutils',
    version='0.1.0',
    description='Test 1',
    author='oj',
    license='BSD 2-clause',
    packages=['dbutils'],
    install_requires=['sqlalchemy',
                      'pymysql',
                      ],
)



