from setuptools import setup, find_packages

long_description = "Bookmarks utility project"

setup(
    name='bookmarks-util',
    version='1.0.0',

    description='This project is created to perform bookmark operation in python shell',
    author='Srinivasarao Daruna',

    author_email='daruns@amazon.com',

    classifiers=[
        'Development Status :: 1 - Alpha',
    ],

    package_dir={'': 'src'},  # Optional

    packages=find_packages(where='src'),

    python_requires='>=3.6, <4',

    install_requires=['pytest','awswrangler'])
