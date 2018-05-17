import pytest

from redis_tasks import exceptions, utils


def test_import_attribute():
    assert utils.import_attribute("tests.app.import_me.test_attrib") == "foo"


def test_utc():
    now = utils.utcnow()
    assert utils.utcparse(utils.utcformat(now)) == now.replace(microsecond=0)


def test_serialization():
    my_obj = {'a': 'b', 'c': 54.325, 'd': utils.utcnow()}
    assert utils.deserialize(utils.serialize(my_obj)) == my_obj

    with pytest.raises(exceptions.DeserializationError):
        utils.deserialize(b"invalid")


def test_atomic_pipeline(mocker):
    connection = mocker.patch('redis_tasks.conf.connection')

    def f1(pipeline):
        assert pipeline == mock_pipe

    mock_pipe = mocker.sentinel.pipe
    utils.atomic_pipeline(f1)(pipeline=mock_pipe)

    def f2(pipeline):
        assert pipeline == real_pipe

    real_pipe = connection.pipeline().__enter__()
    utils.atomic_pipeline(f2)()
    real_pipe.execute.assert_called_once()

    def f3(pipeline):
        raise ZeroDivisionError()

    real_pipe.execute.reset_mock()
    with pytest.raises(ZeroDivisionError):
        utils.atomic_pipeline(f3)()
    real_pipe.execute.assert_not_called()
