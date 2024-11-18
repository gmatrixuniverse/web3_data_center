from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='web3_data_center',
    version='0.4.7',
    description='Web3 data center integrating multiple APIs for blockchain data analysis',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Gmatrixuniverse',
    author_email='gmatrixuniverse@gmail.com',
    url='https://github.com/gmatrixuniverse/web3_data_center',
    packages=find_packages(exclude=['tests*']),
    install_requires=[
        'asyncio',
        'aiohttp',
        'opensearch-py',
        'psycopg2-binary',
        'beautifulsoup4',
        'pytesseract',
        'Pillow',
        'python-dotenv',
        'evm-decoder',
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    python_requires='>=3.7',
    keywords='web3 blockchain data analysis',
)