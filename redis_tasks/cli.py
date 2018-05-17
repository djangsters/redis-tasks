import datetime
import logging
import math
import sys
import time
from functools import partial
from operator import attrgetter

import click
from redis.exceptions import ConnectionError

from redis_tasks import __version__, scheduler_main, worker_main
from redis_tasks.queue import Queue
from redis_tasks.worker import Worker, WorkerState

red = partial(click.style, fg='red')
green = partial(click.style, fg='green')
yellow = partial(click.style, fg='yellow')


@click.group()
@click.version_option(__version__)
def main():
    """redis_tasks command line tool"""
    pass


@main.command()
@click.option('--all', is_flag=True, help='Empty all queues')
@click.option('--delete', is_flag=True, help='Completely remove queues')
@click.argument('queues', nargs=-1)
def empty(all, delete, queues, **options):
    """Empty selected queues"""

    if all:
        queues = Queue.all()
    elif queues:
        queues = [Queue(q) for q in queues]
    else:
        raise click.UsageError("No queues specified")

    if not queues:
        click.echo('Nothing to do')
        sys.exit(0)

    for queue in queues:
        if delete:
            queue.delete()
        else:
            queue.empty()
        click.echo(f'Queue f{queue.name} emptied')


def configure_logging(verbose, quiet, **options):
    if verbose and quiet:
        raise click.UsageError("flags --verbose and --quiet are mutually exclusive")

    level = (verbose and 'DEBUG') or (quiet and 'WARNING') or 'INFO'
    logging.basicConfig(level=level, style='{', datefmt='%H:%M:%S',
                        format='{asctime} {levelname:5.5} {name:>20}: {message}')


@main.command()
@click.option('--verbose', '-v', is_flag=True, help='Show more output')
@click.option('--quiet', '-q', is_flag=True, help='Show less output')
def scheduler(**options):
    """Run the redis_tasks scheduler"""
    configure_logging(**options)
    try:
        scheduler_main()
    except ConnectionError as e:
        print(e)
        sys.exit(1)


@main.command()
@click.option('--burst', '-b', is_flag=True, help='Run in burst mode (quit after all work is done)')
@click.option('--description', '-d', help='Specify a different description')
@click.option('--verbose', '-v', is_flag=True, help='Show more output')
@click.option('--quiet', '-q', is_flag=True, help='Show less output')
@click.argument('queues', nargs=-1)
def worker(burst, description, queues, **options):
    """Run a redis_tasks worker"""
    configure_logging(**options)
    queues = queues or ['default']
    try:
        worker_main(queues, description=description, burst=burst)
    except ConnectionError as e:
        print(e)
        sys.exit(1)


@main.command()
@click.option('--interval', '-i', type=float, help='Updates stats every N seconds')
@click.option('--by-queue', is_flag=True, help='Shows workers by queue')
@click.argument('queues', nargs=-1)
def info(interval, by_queue, queues, **options):
    """Display information about active queues and workers"""
    if queues:
        queues = [Queue(q) for q in queues]

    def display():
        show_queues(queues)
        click.echo('')
        show_workers(queues, by_queue)
        click.echo('')
    try:
        if interval:
            while True:
                click.clear()
                display()
                click.echo('Updated: %s' % datetime.datetime.now())
                time.sleep(interval)
        else:
            display()
    except ConnectionError as e:
        click.echo(e)
        sys.exit(1)
    except KeyboardInterrupt:
        click.echo()
        sys.exit(0)


def print_separator():
    click.echo("=" * (min(40, click.get_terminal_size()[0])))


def show_queues(queues):
    if not queues:
        queues = Queue.all()

    counts = {q: q.count() for q in queues}

    chart_width = 20
    chart_max = max((1, *counts.values()))
    # Round up to the next fixed marker or the next power of two times 1000
    chart_max = next((x for x in [20, 50, 100, 200, 400, 600, 800, 1000]
                      if x >= chart_max),
                     2 ** math.ceil(math.log2(chart_max / 1000)) * 1000)

    click.secho("Queues", bold=True)
    print_separator()
    for q in sorted(queues, key=attrgetter('name')):
        count = counts[q]
        chart = green('|' + '█' * int(count / chart_max * chart_width))
        click.echo(f'{q.name:<12} {chart} {count}')

    click.echo(f'{len(queues)} queue(s), {sum(counts.values())} task(s) total')


def show_workers(queues, by_queue):
    def state_symbol(state):
        return {
            WorkerState.BUSY: red('busy'),
            WorkerState.IDLE: green('idle')
        }.get(state, state)

    workers = Worker.all()
    all_queues = set(Queue.all()) | {q for w in workers for q in w.queues}
    if queues:
        workers = [w for w in workers
                   if any(set(w.queues) & set(queues))]
    else:
        queues = all_queues

    if not by_queue:
        click.secho("Workers", bold=True)
    else:
        click.secho("Workers per Queue", bold=True)
    print_separator()
    if not by_queue:
        for worker in sorted(workers, key=attrgetter('description')):
            worker_queues = ', '.join(q.name for q in worker.queues)
            click.echo(f'{worker.description} {state_symbol(worker.state)}: {worker_queues}')
    else:
        queue_map = {q: [] for q in all_queues}
        for w in workers:
            for q in w.queues:
                queue_map[q].append(w)
        max_qname = max(len(q.name) for q in queues)
        for queue in sorted(queues, key=attrgetter('name')):
            q_workers = queue_map[queue]
            workers_str = ", ".join(sorted(f'{w.description} {state_symbol(w.state)}'
                                           for w in q_workers))
            if not workers_str:
                workers_str = '–'
            click.echo(f'{queue.name:>{max_qname}}: {workers_str}')

    click.echo(f'{len(workers)} worker(s)')


if __name__ == '__main__':
    main()
