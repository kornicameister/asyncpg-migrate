import asyncio
import datetime as dt
import os
from pathlib import Path
import typing as t

import asyncpg
import click
from tabulate import tabulate

from asyncpg_migrate import constants
from asyncpg_migrate import loader
from asyncpg_migrate import model
from asyncpg_migrate.cmd import upgrade as cmd_upgrade

CWD = Path(os.getcwd())


async def _db_revision(ctx: click.Context) -> t.Optional[int]:
    config: model.Config = ctx.obj

    c = await asyncpg.connect(dsn=config.database_dsn)
    await c.reload_schema_state()

    res = await c.fetchrow(
        f"select to_regclass('{constants.MIGRATIONS_SCHEMA}.{constants.MIGRATIONS_TABLE}')",
    )
    if res['to_regclass'] != constants.MIGRATIONS_TABLE:
        raise click.ClickException(
            f'{constants.MIGRATIONS_TABLE} table does not exist, run "upgrade" '
            f'to create migrations first',
        )
    else:
        db_revision = await c.fetchrow(
            f'select revision from {constants.MIGRATIONS_SCHEMA}.{constants.MIGRATIONS_TABLE} '
            f'order by timestamp desc limit 1',
        )
        if not db_revision:
            return None
        return None if db_revision['revision'] is None else int(db_revision['revision'])

    c.terminate()


async def _db_history(ctx: click.Context) -> model.MigrationHistory:
    config: model.Config = ctx.obj
    history = model.MigrationHistory()

    c = await asyncpg.connect(dsn=config.database_dsn)

    await c.reload_schema_state()
    async with c.transaction():
        async for record in c.cursor(f'select revision, label, timestamp, direction from '
                                     f'{constants.MIGRATIONS_SCHEMA}.{constants.MIGRATIONS_TABLE} '
                                     f'order by timestamp asc'):
            history.append(
                model.MigrationHistoryEntry(
                    revision=model.Revision(record['revision']),
                    label=record['label'],
                    timestamp=record['timestamp'],
                    direction=model.MigrationDir(record['direction']),
                ),
            )

    return history


async def _downgrade(ctx: click.Context, target_revision: str) -> None:
    config: model.Config = ctx.obj
    migrations = loader.load_migrations(config)
    to_revision = 1 if target_revision == 'BASE' else int(target_revision)

    await _create_migrations_table_if_not_exists(ctx)
    maybe_db_revision = await _db_revision(ctx)

    if maybe_db_revision is None:
        click.echo('DOWN ::  No migration has ever happened, skipping...')
        return
    elif maybe_db_revision == 0:
        click.echo(
            'DOWN ::  Dowgraded everything there could have been migrated, skipping...,',
        )
        return
    elif not migrations:
        click.echo('DOWN ::  There are no migrations scripts, skipping...')
        return
    else:
        db_revision = maybe_db_revision
        to_revision = 1 if abs(to_revision) == 0 else to_revision

        if to_revision > 0:
            if to_revision > len(migrations):
                click.echo('DOWN ::  Cannot downgrade further than I know scripts for')
                return
        else:
            previous_db_revision = db_revision - (abs(to_revision) - 1)
            if previous_db_revision <= 0:
                to_revision = 1
            else:
                to_revision = previous_db_revision

        if db_revision == 1:
            # special case, we are about to go back to the state as-if no
            # migration has happened, we will remove all the scripts apart
            # from first one
            migrations_to_apply = migrations.slice(start=1, end=1)
        else:
            migrations_to_apply = migrations.slice(start=to_revision, end=db_revision)

        click.echo(
            f'DOWN ::  Applying migrations '
            f'{sorted(migrations_to_apply.keys(), reverse=True)}',
        )

        c = await asyncpg.connect(dsn=config.database_dsn)

        async with c.transaction():
            try:
                with click.progressbar(length=len(migrations_to_apply),
                                       label='DOWN ::') as bar:
                    for migration in migrations_to_apply.downgrade_iterator():
                        click.echo(
                            f'DOWN ::   Applying {migration.revision}/{migration.label}',
                        )

                        await migration.downgrade(c)
                        await c.execute(
                            f'insert into {constants.MIGRATIONS_SCHEMA}.{constants.MIGRATIONS_TABLE}'
                            f'(revision, label, timestamp, direction) '
                            f'values ($1, $2, $3, $4)',
                            migration.revision - 1,
                            migration.label,
                            dt.datetime.today(),
                            model.MigrationDir.DOWN,
                        )

                        await asyncio.sleep(1)
                        bar.update(1)

            except Exception as ex:
                click.echo('UP ::  Failed to dowgrade...')
                raise click.ClickException(str(ex))

        c.terminate()


@click.group()
@click.pass_context
def db(ctx: click.Context) -> None:
    """DB migration tool for asynpg.
    """
    ctx.obj = loader.load_configuration(CWD)

    click.echo('#' * 42)
    click.echo('##\tMigration tool for asyncpg\t##')
    click.echo('#' * 42)
    click.echo('\n')


@db.command(short_help='Upgrades database to specified revision')
@click.argument(
    'revision',
    metavar='<revision>',
    default='head',
    type=str.upper,
)
@click.pass_context
def upgrade(ctx: click.Context, revision: str) -> None:
    asyncio.get_event_loop().run_until_complete(
        cmd_upgrade.run(
            ctx.obj,
            revision,
        ),
    )


@db.command(short_help='Downgrades database to specified revision')
@click.argument(
    'revision',
    metavar='<revision>',
    required=True,
)
@click.pass_context
def downgrade(ctx: click.Context, revision: str) -> None:
    asyncio.get_event_loop().run_until_complete(_downgrade(ctx, revision))


@db.command(short_help='Prints current revision in remote database')
@click.pass_context
def revision(ctx: click.Context) -> None:
    db_revision = asyncio.get_event_loop().run_until_complete(_db_revision(ctx))
    if db_revision is None:
        raise click.ClickException('No revisions found...')
    else:
        click.echo(f'Current database revision is {db_revision}')


@db.command(short_help='Prints migrations history')
@click.pass_context
def history(ctx: click.Context) -> None:
    db_revision = asyncio.get_event_loop().run_until_complete(_db_revision(ctx))
    if db_revision is None:
        raise click.ClickException('No revisions found...')
    else:
        config: model.Config = ctx.obj
        history = asyncio.get_event_loop().run_until_complete(_db_history(ctx))
        click.echo(f'Database {config.database_name} history migration')
        click.echo(
            tabulate(
                [[m.revision, m.label, m.timestamp, m.direction] for m in history],
                headers=['No.', 'model.Revision', 'Lable', 'TS', 'Direction'],
                showindex='No',
            ),
        )


if __name__ == '__main__':
    db()
