from setuptools import find_packages, setup

setup(
    name="ALM_technical_Assignment",
    version="0.0.1",
    description="Package with solutions to assessment",
    author="Bart Joosten",
    author_email="bjoosten@ilionx.com",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pip==24.0",
        "pandas==2.2.0",
        "pytest==8.2.1",
        "pytest-spark==0.6.0",
    ],
)
