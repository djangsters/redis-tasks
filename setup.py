import os

from setuptools import find_packages, setup


def get_version():
    basedir = os.path.dirname(__file__)
    with open(os.path.join(basedir, 'redis_tasks/__init__.py')) as f:
        version_line = next(l for l in f if l.startswith('__version__'))
        return eval(version_line.split('=')[1])
    raise RuntimeError('No version info found.')


setup(
    name='redis-tasks',
    version=get_version(),
    url='https://github.com/djangsters/redis-tasks/',
    author='Christian Fersch',
    author_email='christian@djangsters.de',
    description='redis-tasks is a lightweight library for processing background tasks',
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    python_requires='>=3.6',
    install_requires=['redis >= 2.10.0', 'click', 'croniter', 'pytz'],
    entry_points={
        'console_scripts': [
            'redis_tasks = redis_tasks.cli:main',
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
        'Programming Language :: Python :: 3.6',
    ]
)
