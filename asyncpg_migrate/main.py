import asyncio
from pathlib import Path
import sys
import typing as t

import asyncpg
import click
from loguru import logger
from tabulate import tabulate

from asyncpg_migrate import loader
from asyncpg_migrate import model
from asyncpg_migrate.engine import downgrade
from asyncpg_migrate.engine import migration
from asyncpg_migrate.engine import upgrade

__name__ = 'asyncpg-migrate'


@click.command(short_help='Prints application version')
def version() -> None:
    version = 'dev'
    click.echo(f'{__name__}: {version}')


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

    ctx.obj = loader.load_configuration(config)

    click.echo('#' * 42)
    click.echo('##\tMigration tool for asyncpg\t##')
    click.echo('#' * 42)
    click.echo('\n')


@db.command(
    name='upgrade',
    short_help='Upgrades database to specified revision',
)
@click.argument(
    'revision',
    metavar='<revision>',
    default='head',
    type=str.upper,
)
@click.pass_context
def upgrade_cmd(ctx: click.Context, revision: str) -> None:
    async def _runner() -> t.Optional[model.Revision]:
        return await upgrade.run(
            config=ctx.obj,
            target_revision=revision,
            connection=await asyncpg.connect(dsn=ctx.obj.database_dsn),
        )

    asyncio.run(_runner())


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
        return await downgrade.run(
            config=ctx.obj,
            target_revision=revision,
            connection=await asyncpg.connect(dsn=ctx.obj.database_dsn),
        )

    asyncio.run(_runner())


@db.command(short_help='Prints current revision in remote database')
@click.pass_context
def revision(ctx: click.Context) -> None:
    async def _runner() -> t.Optional[model.Revision]:
        return await migration.latest_revision(
            connection=await asyncpg.connect(dsn=ctx.obj.database_dsn),
        )

    db_revision = asyncio.run(_runner())
    if db_revision is None:
        raise click.ClickException('No revisions found...')
    else:
        click.echo(f'Current database revision is {db_revision}')


@db.command(short_help='Prints migrations history')
@click.pass_context
def history(ctx: click.Context) -> None:
    async def _runner() -> model.MigrationHistory:
        return await migration.list(
            connection=await asyncpg.connect(dsn=ctx.obj.database_dsn),
        )

    mig_history = asyncio.run(_runner())
    click.echo(f'Database {ctx.obj.database_name} history migration')
    click.echo(
        tabulate(
            [[m.revision, m.label, m.timestamp, m.direction] for m in mig_history],
            headers=['No.', 'Revision', 'Lable', 'TS', 'Direction'],
            showindex='1',
        ),
    )


if __name__ == '__main__':
    db(auto_envvar_prefix='APG')
