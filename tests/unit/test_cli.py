import asyncio
from dataclasses import dataclass
import typing as t

import click
from click import testing
import pytest
import pytest_mock as ptm


def test_version(cli_runner: testing.CliRunner) -> None:
    from asyncpg_migrate import main
    result = cli_runner.invoke(main.version)
    assert result.exit_code == 0
    assert 'asyncpg-migrate: ' in result.output


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
