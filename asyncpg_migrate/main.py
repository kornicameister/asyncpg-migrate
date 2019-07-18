import asyncio
from pathlib import Path
import sys
import typing as t

import asyncpg
import click
from loguru import logger
from tabulate import tabulate

import asyncpg_migrate
from asyncpg_migrate import loader
from asyncpg_migrate import model
from asyncpg_migrate.engine import downgrade
from asyncpg_migrate.engine import migration
from asyncpg_migrate.engine import upgrade

__name__ = 'asyncpg-migrate'

try:
    async_run = asyncio.run
except AttributeError:
    # Python 3.6 compatybility
    _T = t.TypeVar('_T')

    def async_run(coro: t.Awaitable[_T]) -> _T:  # type: ignore
        return asyncio.get_event_loop().run_until_complete(coro)


@click.group()
@click.option(
    '-v',
    '--verbose',
    count=True,
    help='Enables rich output',
)
@click.option(
    '-c',
    '--config',
    type=Path,
    default=Path.cwd() / 'migrations.ini',
)
@click.pass_context
def db(
        ctx: click.Context,
        config: Path,
        verbose: int,
) -> None:
    """DB migration tool for asynpg.
    """

    logger.error(ctx)
    if verbose == 0:
        logger.disable('asyncpg-migrate')
    else:
        logger.enable('asyncpg-migrate')
        verbosity = {
            1: 'INFO',
            2: 'DEBUG',
            3: 'TRACE',
        }
        logger.add(
            sys.stderr,
            format='{time} {message}',
            filter='asyncpg-migrate',
            level=verbosity.get(verbose, 'TRACE'),
        )

    logger.debug(
        'Flags are config={config}, verbose={verbose}',
        config=config,
        verbose=verbose,
    )

    ctx.ensure_object(dict)
    ctx.obj['configuration_file_path'] = config


@db.command(short_help='Prints application version')
def version() -> None:
    version = asyncpg_migrate.__version__
    click.echo(f'v{version}')


@db.command(
    name='upgrade',
    short_help='Upgrades database to specified revision',
)
@click.argument(
    'revision',
    metavar='<revision>',
    required=True,
    type=str.upper,
)
@click.pass_context
def upgrade_cmd(ctx: click.Context, revision: str) -> None:
    async def _runner() -> t.Optional[model.Revision]:
        config = loader.load_configuration(ctx.obj['configuration_file_path'])
        return await upgrade.run(
            config=config,
            target_revision=revision,
            connection=await asyncpg.connect(dsn=config.database_dsn),
        )

    async_run(_runner())


@db.command(
    name='downgrade',
    short_help='Downgrades database to specified revision',
)
@click.argument(
    'revision',
    metavar='<revision>',
    required=True,
    type=str.upper,
)
@click.pass_context
def downgrade_cmd(ctx: click.Context, revision: str) -> None:
    async def _runner() -> t.Optional[model.Revision]:
        config = loader.load_configuration(ctx.obj['configuration_file_path'])
        return await downgrade.run(
            config=config,
            target_revision=revision,
            connection=await asyncpg.connect(dsn=config.database_dsn),
        )

    async_run(_runner())


@db.command(
    name='revision',
    short_help='Prints current revision in remote database',
)
@click.pass_context
def revision_cmd(ctx: click.Context) -> None:
    async def _runner() -> t.Optional[model.Revision]:
        config = loader.load_configuration(ctx.obj['configuration_file_path'])
        return await migration.latest_revision(
            connection=await asyncpg.connect(dsn=config.database_dsn),
        )

    db_revision = async_run(_runner())
    if db_revision is None:
        raise click.ClickException(
            'No revisions found, you might want to run some migrations first :)',
        )
    else:
        click.echo(f'Current database revision is {db_revision}')


@db.command(short_help='Prints migrations history')
@click.pass_context
def history(ctx: click.Context) -> None:
    async def _runner(cfg: model.Config) -> model.MigrationHistory:
        return await migration.list(
            connection=await asyncpg.connect(dsn=cfg.database_dsn),
        )

    config = loader.load_configuration(ctx.obj['configuration_file_path'])
    mig_history = async_run(_runner(config))
    if not mig_history:
        raise click.ClickException(
            'No revisions found, you might want to run some migrations first :)',
        )
    else:
        click.echo(f'Database {config.database_name} history migration')
        click.echo(
            tabulate(
                [[m.revision, m.label, m.timestamp, m.direction] for m in mig_history],
                headers=['No.', 'Revision', 'Lable', 'TS', 'Direction'],
                showindex='1',
            ),
        )


if __name__ == '__main__':
    db(auto_envvar_prefix='APG')
