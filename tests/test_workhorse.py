import multiprocessing
import os
import signal

import pytest

from redis_tasks.exceptions import WorkerShutdown
from redis_tasks.task import Task
from redis_tasks.worker_process import PostponeShutdown, WorkHorse
from tests.utils import TaskFactory


def test_postpone_shutdown():
    PostponeShutdown._shutdown_delayed = False
    with pytest.raises(WorkerShutdown):
        PostponeShutdown.trigger_shutdown()

    ps = PostponeShutdown()
    ps.__enter__()
    PostponeShutdown.trigger_shutdown()
    with pytest.raises(WorkerShutdown):
        ps.__exit__()

    PostponeShutdown._shutdown_delayed = False
    ps = PostponeShutdown()
    ps.activate()
    PostponeShutdown.trigger_shutdown()
    with pytest.raises(WorkerShutdown):
        ps.deactivate()

    PostponeShutdown._shutdown_delayed = False
    ps1 = PostponeShutdown()
    ps1.activate()
    with PostponeShutdown():
        PostponeShutdown.trigger_shutdown()
    with pytest.raises(WorkerShutdown):
        ps1.deactivate()

    PostponeShutdown._shutdown_delayed = False
    assert not PostponeShutdown._active


def test_run(mocker):
    task = TaskFactory()
    execute = mocker.patch.object(task, 'execute', return_value=mocker.sentinel.outcome)
    conn1, conn2 = multiprocessing.Pipe()
    WorkHorse(task, conn2).run()
    execute.assert_called_once()
    assert conn1.poll()
    assert conn1.recv() is True
    assert conn1.poll()
    assert conn1.recv().name == 'outcome'
    assert conn1.poll() is False

    # do not leave the PostponeShutdown active
    with execute.call_args[1]["shutdown_cm"]:
        pass


def horsetask():
    raise ArithmeticError("work me")


def test_process():
    task = Task(horsetask)
    horse_conn, conn2 = multiprocessing.Pipe()
    horse = WorkHorse(task, conn2)
    horse.start()
    horse.join()
    assert horse_conn.poll()
    assert horse_conn.recv() is True
    assert horse_conn.poll()
    outcome = horse_conn.recv()
    assert horse_conn.poll() is False
    assert outcome.outcome == 'failure'
    assert outcome.message.splitlines()[-1] == 'ArithmeticError: work me'


def horsetaskwait():
    with multiprocessing.connection.Client(os.environ['RT_TEST_SOCKET']) as conn:
        conn.send("A")
        assert conn.recv() == "B"
        conn.send("C")
        conn.recv()


@pytest.fixture()
def suprocess_socket(tmpdir):
    socket_file = str(tmpdir.join('socket'))
    os.environ['RT_TEST_SOCKET'] = socket_file
    with multiprocessing.connection.Listener(socket_file) as listener:
        yield listener


def test_process_shutdown(suprocess_socket):
    # sanity checks against leftovers from previous tests
    assert not PostponeShutdown._active
    assert not PostponeShutdown._shutdown_delayed

    task = Task(horsetaskwait)
    horse_conn, conn2 = multiprocessing.Pipe()
    horse = WorkHorse(task, conn2)
    horse.start()
    assert horse_conn.poll(1)
    assert horse_conn.recv() is True
    with suprocess_socket.accept() as taskconn:
        assert taskconn.poll(1)
        assert taskconn.recv() == "A"
        # horse ignores SIGTERM
        horse.send_signal(signal.SIGTERM)
        taskconn.send("B")
        assert taskconn.poll(1)
        assert taskconn.recv() == "C"
        # Trigger shutdown
        horse.send_signal(signal.SIGUSR1)
        horse.join(1)
        assert not horse.is_alive()
    assert horse_conn.poll()
    outcome = horse_conn.recv()
    assert not horse_conn.poll()
    assert outcome.outcome == 'failure'
    assert 'Worker shutdown' in outcome.message.splitlines()[-1]
