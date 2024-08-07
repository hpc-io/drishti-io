from setuptools import find_packages, setup

with open("README.md", "r") as f:
    long_description = f.read()

with open("requirements.txt") as f:
    requirements = f.readlines()

setup(
    name="drishti-io",
    keywords="drishti",
    version="0.6",
    author="Jean Luca Bez, Suren Byna",
    author_email="jlbez@lbl.gov, sbyna@lbl.gov",
    description="",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/hpc-io/drishti",
    install_requires=[
        'argparse',
        'pandas',
        'darshan>=3.4.4.0',
        'rich==12.5.1',
        'recorder-utils',
    ],
    packages=find_packages(),
    package_data={
        'drishti.includes': [
            'drishti/includes/snippets/*'
        ],
    },
    include_package_data=True,
    entry_points={
        "console_scripts": [
            "drishti=drishti.reporter:main"
        ]
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: Other/Proprietary License",
        "Programming Language :: Python :: 3 :: Only"
    ],
    python_requires='>=3.6',
)
