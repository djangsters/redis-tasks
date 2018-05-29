import multiprocessing
import os
import signal
import time

import pytest

from redis_tasks.exceptions import WorkerShutdown
from redis_tasks.task import TaskOutcome, TaskStatus
from redis_tasks.utils import decode_list
from redis_tasks.worker import WorkerState
from redis_tasks.worker_process import (
    ShutdownRequested, WorkerProcess, WorkHorse, generate_worker_description)
from tests.utils import QueueFactory, TaskFactory, id_list


def test_generate_description():
    assert generate_worker_description()


def dummy_name():
    return "dummy"


def test_init(assert_atomic, settings):
    settings.WORKER_DESCRIPTION_FUNCTION = "tests.test_workerprocess.dummy_name"

    wp = WorkerProcess([QueueFactory])
    assert wp.worker.description == "dummy"


def test_queue_iter(connection, mocker):
    queues = [QueueFactory(), QueueFactory()]
    wp = WorkerProcess(queues)
    wp.worker.startup()
    qi = wp.queue_iter(False)
    task1 = queues[1].enqueue_call()
    task2 = queues[0].enqueue_call()
    assert next(qi).id == task2.id
    assert next(qi).id == task1.id

    def my_await_multi(*args):
        nonlocal await_counter, task3, task4, task5
        await_counter += 1
        if await_counter == 1:
            return None
        elif await_counter == 2:
            task3 = queues[1].enqueue_call()
            return queues[1]
        elif await_counter == 3:
            task4 = queues[1].enqueue_call()
            task5 = queues[1].enqueue_call()
            return queues[0]
        assert False

    await_counter = 0
    task3 = task4 = task5 = None
    mocker.patch('redis_tasks.queue.Queue.await_multi', new=my_await_multi)
    assert next(qi).id == task3.id
    assert wp.worker.current_task_id == task3.id
    assert await_counter == 2
    assert next(qi).id == task4.id
    assert wp.worker.current_task_id == task4.id
    assert await_counter == 3
    assert next(qi).id == task5.id
    assert await_counter == 3

    # Assert that tasks are moved to the worker
    assert (decode_list(connection.lrange(wp.worker.task_key, 0, -1)) ==
            id_list([task5, task4, task3, task1, task2]))

    # Burst mode
    wp = WorkerProcess(queues)
    wp.worker.startup()
    task1 = queues[1].enqueue_call()
    task2 = queues[0].enqueue_call()
    task3 = queues[1].enqueue_call()
    assert id_list(wp.queue_iter(True)) == id_list([task2, task1, task3])


def test_run(settings, mocker, stub):
    tasks = [mocker.sentinel.t1, mocker.sentinel.t2, mocker.sentinel.t3]
    mocker.patch('redis_tasks.worker_process.WorkerProcess.queue_iter', return_value=tasks)

    def my_process(task):
        assert wp.worker.state == WorkerState.IDLE

    process = mocker.patch('redis_tasks.worker_process.WorkerProcess.process_task',
                           side_effect=my_process)
    maintenance = mocker.patch('redis_tasks.worker_process.Maintenance')
    wp = WorkerProcess([QueueFactory()])
    assert wp.run(True) == 3
    assert process.call_args_list == [((x, ),) for x in tasks]
    assert maintenance().run_if_neccessary.call_count == 3
    assert wp.worker.state == WorkerState.DEAD

    settings.WORKER_PRELOAD_FUNCTION = stub.path
    stub.mock.reset_mock()
    wp = WorkerProcess([QueueFactory()])
    wp.run(True)
    assert stub.mock.call_count == 1


def test_run_shutdown(settings, mocker):
    mocker.patch.object(WorkerProcess, 'queue_iter', side_effect=ShutdownRequested)
    wp = WorkerProcess([QueueFactory()])
    with pytest.raises(ShutdownRequested):
        wp.run(False)
    assert wp.worker.started_at
    assert wp.worker.shutdown_at


def test_heartbeats(mocker, stub):
    heartbeat_sent = False

    def send_heartbeat(*args, **kwargs):
        nonlocal heartbeat_sent
        heartbeat_sent = True
    mocker.patch('redis_tasks.registries.WorkerRegistry.heartbeat', side_effect=send_heartbeat)

    def consume_heartbeat(*args, **kwargs):
        nonlocal heartbeat_sent
        assert heartbeat_sent is True
        heartbeat_sent = False
        return mocker.DEFAULT
    maintenance = mocker.patch('redis_tasks.worker_process.Maintenance.run_if_neccessary',
                               side_effect=consume_heartbeat)

    def my_await(*args):
        nonlocal task
        for i in range(3):
            consume_heartbeat()
            print("Awaited!")
            yield None
        task = queue.enqueue_call(stub)
        consume_heartbeat()
        yield queue
        raise ShutdownRequested()
    mock_await = mocker.patch('redis_tasks.queue.Queue.await_multi', side_effect=my_await())

    mocker.patch.object(WorkHorse, 'start', new=WorkHorse.run)
    horse_alive = mocker.patch.object(WorkHorse, 'is_alive', return_value=True)

    def my_join(*args):
        consume_heartbeat()
        yield None
        consume_heartbeat()
        yield None
        consume_heartbeat()
        horse_alive.return_value = False
        yield None
    mock_join = mocker.patch.object(WorkHorse, 'join', side_effect=my_join())

    stub.mock.reset_mock()
    queue = QueueFactory()
    task = None
    wp = WorkerProcess([queue])
    with pytest.raises(ShutdownRequested):
        wp.run(False)
    task.refresh()
    assert task.status == TaskStatus.FINISHED
    assert maintenance.call_count == 1
    assert mock_await.call_count == 5
    assert mock_join.call_count == 3
    assert stub.mock.called


def test_process_task(mocker):
    q = QueueFactory()
    wp = WorkerProcess([q])
    wp.worker.startup()
    q.enqueue_call()
    task = q.dequeue(wp.worker)

    def my_execute(task):
        assert wp.worker.state == WorkerState.BUSY
        assert task.status == TaskStatus.RUNNING
        return TaskOutcome("success")

    execute = mocker.patch.object(WorkerProcess, 'execute_task', side_effect=my_execute)
    wp.process_task(task)
    assert task.status == TaskStatus.FINISHED

    q.enqueue_call()
    task = q.dequeue(wp.worker)
    execute.side_effect = ArithmeticError()
    wp.process_task(task)
    assert task.status == TaskStatus.FAILED
    assert 'ArithmeticError' in task.error_message


def test_execute_task(mocker, settings, time_mocker):
    horse = None

    def my_start(self):
        nonlocal horse
        horse = self
        assert horse.task == task
        horse_alive.return_value = True
        horse.worker_connection.send(True)

    mocker.patch.object(WorkHorse, 'start', new=my_start)
    horse_alive = mocker.patch.object(WorkHorse, 'is_alive', return_value=False)

    # Normal run
    def my_join():
        yield None
        yield None
        horse.worker_connection.send(TaskOutcome("success"))
        horse_alive.return_value = False
        yield None

    mock_join = mocker.patch.object(WorkHorse, 'join', side_effect=my_join())
    wp = WorkerProcess([QueueFactory()])
    wp.worker.startup()
    task = TaskFactory()
    outcome = wp.execute_task(task)
    assert outcome.outcome == "success"
    assert mock_join.call_count == 3

    # Unexpected WorkHorse death
    def dying_join():
        yield None
        horse_alive.return_value = False
        yield None

    mock_join.side_effect = dying_join()
    outcome = wp.execute_task(task)
    assert outcome.outcome == "failure"
    assert "Workhorse died unexpectedly" in outcome.message

    # Shutdown
    shutdown_initiated = False

    def take_usr1(signum):
        nonlocal shutdown_initiated
        shutdown_initiated = True
        assert signum == signal.SIGUSR1

    def shutdown_join():
        yield None
        assert wp.in_interruptible
        yield ShutdownRequested()
        assert shutdown_initiated
        yield None
        horse.worker_connection.send(TaskOutcome("requeue"))
        yield None
        horse_alive.return_value = False
        yield None
    mock_join.side_effect = shutdown_join()
    fake_signal = mocker.patch.object(WorkHorse, 'send_signal', side_effect=take_usr1)
    outcome = wp.execute_task(task)
    assert outcome.outcome == "requeue"

    # Timeout
    def timeout_join():
        yield None
        yield None
        time.step()
        yield None
        yield None

    def take_kill(signum):
        assert signum == signal.SIGKILL
        horse_alive.return_value = False

    mock_join.side_effect = timeout_join()
    settings.DEFAULT_TASK_TIMEOUT = 1
    time = time_mocker("redis_tasks.worker_process.utcnow")
    time.step()
    fake_signal = mocker.patch.object(WorkHorse, 'send_signal', side_effect=take_kill)
    outcome = wp.execute_task(task)
    assert outcome.outcome == "failure"
    assert "Task timeout (1 sec) reached" in outcome.message
    fake_signal.assert_called_once_with(9)


def taskwait():
    with multiprocessing.connection.Client(os.environ['RT_TEST_SOCKET']) as conn:
        conn.send("A")
        try:
            time.sleep(10)
            conn.send("C")
        except WorkerShutdown:
            conn.send("B")
            raise


@pytest.fixture()
def suprocess_socket(tmpdir):
    socket_file = str(tmpdir.join('socket'))
    os.environ['RT_TEST_SOCKET'] = socket_file
    with multiprocessing.connection.Listener(socket_file) as listener:
        yield listener


def test_signal_shutdown_in_task(suprocess_socket):
    queue = QueueFactory()
    task = queue.enqueue_call(taskwait)
    wp = WorkerProcess([queue])
    process = multiprocessing.Process(target=wp.run)
    process.start()

    with suprocess_socket.accept() as taskconn:
        assert taskconn.poll(1)
        assert taskconn.recv() == "A"
        os.kill(process.pid, signal.SIGTERM)
        assert taskconn.poll(1)
        assert taskconn.recv() == "B"
        process.join(1)
        assert not process.is_alive()
    task.refresh()
    assert task.status == TaskStatus.FAILED
    assert 'Worker shutdown' in task.error_message.splitlines()[-1]


def test_signal_shutdown_in_queuewait():
    wp = WorkerProcess([QueueFactory()])
    process = multiprocessing.Process(target=wp.run)
    process.start()
    time.sleep(0.01)
    os.kill(process.pid, signal.SIGTERM)
    process.join(1)
    assert not process.is_alive()
