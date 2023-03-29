from setuptools import find_packages, setup
setup(
    name='market_wss',
    packages=find_packages(include=['market_wss']),
    version='0.1',
    description='API websockets to market data',
    author='',
    license='',
    install_requires=[],
    setup_requires=['pytest-runner'],
    tests_require=['pytest==4.4.1'],
    test_suite='tests',
)