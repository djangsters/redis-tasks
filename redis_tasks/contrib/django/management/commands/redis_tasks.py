import os.path

import click

from redis_tasks.cli import main as cli_main


class Command:
    def run_from_argv(self, argv):
        """Called when run from the command line."""
        prog_name = "{} {}".format(os.path.basename(argv[0]), argv[1])
        return self.main(args=argv[2:], prog_name=prog_name)

    def print_help(self, prog_name, subcommand):
        prog_name = "{} {}".format(prog_name, subcommand)
        return self.main(['--help'], prog_name=prog_name)

    def main(self, args, prog_name):
        decorators = [
            click.option(
                '--pythonpath',
                metavar='PYTHONPATH',
                expose_value=False,
                help=('A directory to add to the Python path, e.g. '
                      '"/home/djangoprojects/myproject".'),
            ),
            click.option(
                '--settings',
                metavar='SETTINGS',
                expose_value=False,
                help=('The Python path to a settings module, e.g. '
                      '"myproject.settings.main". If this is not provided, the '
                      'DJANGO_SETTINGS_MODULE environment variable will be used.'),
            ),
        ]
        command = cli_main
        for decorator in decorators:
            command = decorator(command)
        return command.main(args=args, prog_name=prog_name)
