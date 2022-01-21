import os

from setuptools import find_packages, setup


def get_version():
    basedir = os.path.dirname(__file__)
    with open(os.path.join(basedir, 'redis_tasks/__init__.py')) as f:
        version_line = next(line for line in f
                            if line.startswith('__version__'))
        return eval(version_line.split('=')[1])
    raise RuntimeError('No version info found.')


def get_long_description():
    this_directory = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
        long_description = f.read()
    return long_description


setup(
    name='redis-tasks',
    version=get_version(),
    url='https://github.com/djangsters/redis-tasks/',
    author='Christian Fersch',
    author_email='christian@djangsters.de',
    description='redis-tasks is a lightweight library for processing background tasks',
    long_description=get_long_description(),
    long_description_content_type='text/markdown',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    python_requires='>=3.7',
    install_requires=['redis >=3.2.0',
                      'click',
                      'croniter >=0.3.23',
                      'pytz'],
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
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ]
)
