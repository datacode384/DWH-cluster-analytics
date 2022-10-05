from setuptools import find_packages, setup
from package import Package

setup(
    author="krishna damarla",
    packages=find_packages(),
    include_package_data=True,
    #install_requires=['db2jcc4.jar', 'logfile', 'log'],
    #package_data={'': ['*.jar']},
    cmdclass={
        "package": Package
    }
)
