# redis-tasks

[![Build Status](https://travis-ci.com/djangsters/redis-tasks.svg?branch=master)](https://travis-ci.com/djangsters/redis-tasks)
[![Coverage Status](https://coveralls.io/repos/github/djangsters/redis-tasks/badge.svg?branch=master)](https://coveralls.io/github/djangsters/redis-tasks?branch=master)
[![Documentation Status](https://readthedocs.org/projects/redis-tasks/badge/?version=latest)](https://redis-tasks.readthedocs.io/en/latest/?badge=latest)
[![PyPI version](https://badge.fury.io/py/redis-tasks.svg)](https://badge.fury.io/py/redis-tasks)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/redis-tasks)

`redis-tasks` is a lightweight framework created to reliably process background 
tasks. Built-in `SIGTERM` signal handling makes it a great choice for a task 
manager running on Heroku. Out-of-the-box provided task scheduler with a 
user-friendly interface makes periodic task management easy and scalable.

For more information and usage instructions, see our [documentation](
https://redis-tasks.readthedocs.io/en/latest/).

## Monitoring
Want to monitor `redis-tasks` tasks, queues and workers? Check out [rt-dashboard](
https://github.com/djangsters/rt-dashboard) - a monitoring tool for `redis-tasks`.

## Contributing
[tox](https://tox.readthedocs.io/en/latest/index.html) is used to run the tests 
and automatically sets up virtual environments to run the tests in. It 
implicitly uses [virtualenv]( https://virtualenv.pypa.io/en/latest/).
To install `tox` run
```
pip install tox
```
Make sure to install the supported python versions on your local machine.
If you don't want to install all supported python versions, you can either 
explicitly specify environments you want to run `tox` in or alternatively you
could run `tox` with the [--skip_missing_interpreters](
https://tox.readthedocs.io/en/latest/config.html#conf-skip_missing_interpreters)
flag.

### Running tests
You can run all the tests with
```
tox
```
or run specific test environments, for example only tests on Python 3.8, with
```
tox -e py38
```
If you want to customize the pytest run, you can pass in pytest arguments 
after the `--` like so
```
tox -e py38 -- tests/test_cli.py
```
This will run tests only on Python 3.8 and from one module `tests/test_cli.py`
only.
    
### Running linter checks
You can run `flake8` checks with 
```
tox -e flake8
```
`flake8` checks are also run as a part of the all tests run with
```
tox
```

### Building documentation locally
You can build documentation locally with 
```
tox -e docs
```
The built html files will be located in `docs/build/html`.
