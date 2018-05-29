import random

import pytest

from redis_tasks import Queue, TWorker
from redis_tasks.contrib.graph import GraphMiddleware, TaskGraph, chain


@pytest.fixture
def with_mw(mocker):
    mocker.patch('redis_tasks.conf.task_middleware', new=[GraphMiddleware])
    mocker.patch('redis_tasks.task.task_middleware', new=[GraphMiddleware])


def test_middleware_check(mocker):
    with pytest.raises(Exception):
        TaskGraph().enqueue()

    mocker.patch('redis_tasks.conf.task_middleware', new=[GraphMiddleware])
    TaskGraph().enqueue()


def test_chain(stub, with_mw, mocker):
    def queue_assert(*args):
        assert Queue().count() == 0

    stub.mock.side_effect = queue_assert
    chain([
        dict(func=stub, args=["a"]),
        dict(func=stub, args=["b"]),
        dict(func=stub, args=["c"]),
    ]).enqueue()
    assert Queue().count() == 1
    w = TWorker()
    assert w.run() == 3
    assert stub.mock.call_count == 3
    assert stub.mock.call_args_list == [
        mocker.call('a'), mocker.call("b"), mocker.call("c")]


def test_graph(stub, with_mw, mocker):
    def queue_assert(rnd, queue_size):
        assert Queue().count() == queue_size

    stub.mock.side_effect = queue_assert
    graph = TaskGraph()
    # We construct the following graph:
    #  1  2f
    # /|\/|
    # ||/\|
    # |3  4
    # |/
    # 0
    tasks = [dict(func=stub, args=(random.randrange(10000), queue_size))
             for queue_size in [0, 1, 0, 1, 1]]
    nodes = [graph.add_task(t) for t in tasks]
    edges = [(1, 0), (1, 3), (1, 4), (2, 3), (2, 4), (3, 0)]
    for a, b in edges:
        graph.add_dependency(nodes[a], nodes[b])
    graph.enqueue()
    assert Queue().count() == 2
    w = TWorker()
    assert w.run() == 5
    assert stub.mock.call_args_list == [
        mocker.call(*tasks[i]["args"]) for i in [1, 2, 3, 4, 0]]
