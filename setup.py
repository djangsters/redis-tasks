import sys
import os
from setuptools import setup, find_packages


def get_version():
    basedir = os.path.dirname(__file__)
    with open(os.path.join(basedir, 'redis_tasks/version.py')) as f:
        locals = {}
        exec(f.read(), locals)
        return locals['VERSION']
    raise RuntimeError('No version info found.')


def get_dependencies():
    deps = ['redis >= 2.7.0', 'click >= 3.0']
    return deps


setup(
    name='redis_tasks',
    version=get_version(),
    url='https://github.com/chronial/redis_tasks/',
    license='TODO',
    author='Christian Fersch',
    author_email='chronial@visiondesigns.de',
    description='redis_tasks is a lightweight library for processing background tasks',
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    python_requires='>=3.6.0',
    install_requires=['redis >= 2.10.0'],
    entry_points={
        'console_scripts': [
            'redis_tasks = redis_tasks.cli:main',  # TODO
        ],
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        # 'Development Status :: 4 - Beta',
        # 'Development Status :: 5 - Production/Stable',
        # 'Development Status :: 6 - Mature',
        # 'Development Status :: 7 - Inactive',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 3',

    ]
)
