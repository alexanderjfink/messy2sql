from setuptools import setup, find_packages

long_desc = """
Quick add-on to Open Knowledge Foundation's Messytables
"""


setup(
    name='messy2sql',
    version='0.1.0a10',
    description="Convert messytables types to SQL create/insert statements",
    long_description=long_desc,
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
    ],
    keywords='',
    author='Alexander J Fink',
    author_email='alexanderjfink@gmail.com',
    url='http://publicfragments.org',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'test', 'docs']),
    namespace_packages=[],
    include_package_data=False,
    zip_safe=False,
    install_requires=[
        'xlrd>=0.8.0',
        'python-magic==0.4.3',  # used for type guessing
        'chardet==2.1.1',
        'python-dateutil>=1.5.0,<2.0.0',
        'json-table-schema',
        'lxml>=3.2',
        'pdftables>=0.0.3',
        'messytables',
    ],
    extras_require={},
    tests_require=[],
    entry_points=\
    """
    """,
)
