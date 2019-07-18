import asyncio
from dataclasses import dataclass
import datetime as dt
import typing as t

import click
from click import testing
import pytest
import pytest_mock as ptm

from asyncpg_migrate import model


def test_version(cli_runner: testing.CliRunner) -> None:
    from asyncpg_migrate import main
    result = cli_runner.invoke(main.db, 'version')
    assert result.exit_code == 0


def test_db_help(cli_runner: testing.CliRunner) -> None:
    from asyncpg_migrate import main
    result_1 = cli_runner.invoke(main.db)
    result_2 = cli_runner.invoke(main.db, ['--help'])
    assert (result_1.exit_code, result_2.exit_code) == (0, 0)
    assert result_1.output == result_2.output


@pytest.mark.parametrize(
    'invoke_arg',
    [
        ['-v', 'test'],
        ['-vv', 'test'],
        ['-vvv', 'test'],
        ['--verbose', 'test'],
        ['test'],
    ],
)
def test_db_verbosity(
        cli_runner: testing.CliRunner,
        mocker: ptm.MockFixture,
        invoke_arg: t.List[str],
) -> None:
    from loguru import logger
    enable_spy = mocker.spy(logger, name='enable')
    disable_spy = mocker.spy(logger, name='disable')
    add_spy = mocker.spy(logger, name='add')
    _ = mocker.patch('asyncpg_migrate.loader.load_configuration')

    from asyncpg_migrate import main

    @main.db.command(name='test', short_help='test command')
    def test() -> None:
        click.echo(1)

    result = cli_runner.invoke(main.db, invoke_arg)
    assert result.exit_code == 0

    if invoke_arg[0] == '-v' or invoke_arg[0] == '--verbose':
        assert enable_spy.called
        assert not disable_spy.called
        assert add_spy.mock_calls[0].kwargs == {
            'format': '{time} {message}',
            'filter': 'asyncpg-migrate',
            'level': 'INFO',
        }
    elif invoke_arg[0] == '-vv':
        assert enable_spy.called
        assert not disable_spy.called
        assert add_spy.mock_calls[0].kwargs == {
            'format': '{time} {message}',
            'filter': 'asyncpg-migrate',
            'level': 'DEBUG',
        }
    elif invoke_arg[0] == '-vvv':
        assert enable_spy.called
        assert not disable_spy.called
        assert add_spy.mock_calls[0].kwargs == {
            'format': '{time} {message}',
            'filter': 'asyncpg-migrate',
            'level': 'TRACE',
        }
    else:
        assert not enable_spy.called
        assert disable_spy.called
        assert not add_spy.called


@pytest.mark.parametrize('revision', ['head', 'HEAD', '5'])
def test_db_upgrade(
        cli_runner: testing.CliRunner,
        mocker: ptm.MockFixture,
        revision: str,
) -> None:
    @dataclass
    class MockedConfig:
        database_dsn: str

    database_dsn = 'postgres://test:test@test:5432/test'
    mocked_config = MockedConfig(database_dsn)
    db_connection = object()
    _ = mocker.patch(
        'asyncpg_migrate.loader.load_configuration',
        return_value=mocked_config,
    )

    connect_patch = mocker.patch(
        'asyncpg.connect',
        side_effect=asyncio.coroutine(lambda dsn: db_connection),
    )
    upgrade_patch = mocker.patch(
        'asyncpg_migrate.engine.upgrade.run',
        side_effect=asyncio.coroutine(lambda *args, **kwargs: None),
    )

    from asyncpg_migrate import main

    result = cli_runner.invoke(main.db, f'upgrade {revision}')

    assert result.exit_code == 0
    connect_patch.assert_called_once_with(dsn=database_dsn)
    upgrade_patch.assert_called_once_with(
        connection=db_connection,
        config=mocked_config,
        target_revision=revision.upper(),
    )


@pytest.mark.parametrize('revision', ['BASE', 'base', '5', '-4'])
def test_db_downgrade(
        cli_runner: testing.CliRunner,
        mocker: ptm.MockFixture,
        revision: str,
) -> None:
    @dataclass
    class MockedConfig:
        database_dsn: str

    database_dsn = 'postgres://test:test@test:5432/test'
    mocked_config = MockedConfig(database_dsn)
    db_connection = object()
    _ = mocker.patch(
        'asyncpg_migrate.loader.load_configuration',
        return_value=mocked_config,
    )

    connect_patch = mocker.patch(
        'asyncpg.connect',
        side_effect=asyncio.coroutine(lambda dsn: db_connection),
    )
    downgrade_patch = mocker.patch(
        'asyncpg_migrate.engine.downgrade.run',
        side_effect=asyncio.coroutine(lambda *args, **kwargs: None),
    )

    from asyncpg_migrate import main

    result = cli_runner.invoke(main.db, f'downgrade -- {revision}')

    assert result.exit_code == 0
    connect_patch.assert_called_once_with(dsn=database_dsn)
    downgrade_patch.assert_called_once_with(
        connection=db_connection,
        config=mocked_config,
        target_revision=revision.upper(),
    )


@pytest.mark.parametrize('return_revision', [None, 0, 1])
def test_db_revision(
        cli_runner: testing.CliRunner,
        mocker: ptm.MockFixture,
        return_revision: t.Optional[int],
) -> None:
    @dataclass
    class MockedConfig:
        database_dsn: str

    mocker.patch(
        'asyncpg_migrate.loader.load_configuration',
        return_value=MockedConfig('postgres://test:test@test:5432/test'),
    )
    mocker.patch(
        'asyncpg.connect',
        side_effect=asyncio.coroutine(lambda dsn: object()),
    )
    mocker.patch(
        'asyncpg_migrate.engine.migration.latest_revision',
        side_effect=asyncio.coroutine(lambda *args, **kwargs: return_revision),
    )

    from asyncpg_migrate import main

    result = cli_runner.invoke(main.db, 'revision')

    if return_revision is None:
        assert result.exception is not None
        assert result.exit_code == 1
        assert 'No revisions found, you might want to run some migrations first :)' in \
            result.output
    else:
        assert result.exception is None
        assert result.exit_code == 0
        assert f'Current database revision is {return_revision}' in result.output


@pytest.mark.parametrize('entries_count', [0, 1, 3, 7, 10])
def test_db_history(
        cli_runner: testing.CliRunner,
        mocker: ptm.MockFixture,
        entries_count: int,
) -> None:
    @dataclass
    class MockedConfig:
        database_dsn: str
        database_name: str

    entries = model.MigrationHistory([
        model.MigrationHistoryEntry(
            revision=model.Revision(rev),
            timestamp=model.Timestamp(dt.datetime.today()),
            label=mocker.stub(),
            direction=model.MigrationDir.UP if rev % 2 else model.MigrationDir.DOWN,
        ) for rev in range(entries_count)
    ])

    mocker.patch(
        'asyncpg_migrate.loader.load_configuration',
        return_value=MockedConfig(
            'postgres://test:test@test:5432/test',
            'test',
        ),
    )
    mocker.patch(
        'asyncpg.connect',
        side_effect=asyncio.coroutine(lambda dsn: object()),
    )
    mocker.patch(
        'asyncpg_migrate.engine.migration.list',
        side_effect=asyncio.coroutine(lambda *args, **kwargs: entries),
    )

    from asyncpg_migrate import main

    result = cli_runner.invoke(main.db, 'history')
    if not entries_count:
        assert result.exception is not None
        assert result.exit_code == 1
        assert 'No revisions found, you might want to run some migrations first :)' in \
            result.output
    else:
        assert result.exception is None
        assert result.exit_code == 0
        assert len(result.output.split('\n')[3:]) - 1 == entries_count
