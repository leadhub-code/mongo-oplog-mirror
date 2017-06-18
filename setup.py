from setuptools import setup, find_packages


setup(
    name='mongo-oplog-mirror',
    version='0.0.1',
    author='Petr Messner',
    author_email='petr.messner@gmail.com',
    license='MIT',
    packages=find_packages(exclude=['doc', 'tests']),
    install_requires=['pymongo', 'pyyaml', 'simplejson'],
    entry_points={
        'console_scripts': [
            'dump-oplog=mongo_oplog_mirror:dump_oplog_main',
            'sync-docs=mongo_oplog_mirror:sync_documents_main',
        ],
    })
