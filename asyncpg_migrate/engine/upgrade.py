import asyncio
import datetime as dt
import typing as t

import asyncpg
from loguru import logger

from asyncpg_migrate import constants
from asyncpg_migrate import loader
from asyncpg_migrate import model
from asyncpg_migrate.commands import _helper

LOG = logger.configure(extra={'command': 'upgrade'})


@LOG.catch
async def run(
        config: model.Config,
        target_revision: t.Union[str, model.Revision],
) -> t.AsyncIterator[int]:
    """Executes the UP migration.

    Algorithm:
    1. Check if everything is on order
    2. Ensure that migration table is created
    3. Get latest migration that has been applied from DB
    4. Compute the next migration from which to start new one
    5. Be happy :)
    """

    migrations = loader.load_migrations(config)
    to_revision = len(migrations) if target_revision == 'HEAD' else int(target_revision)

    if not migrations:
        logger.trace('There are no migrations scripts, skipping...')
        raise RuntimeError('No migrations to upgrade')
    elif to_revision == 0:
        logger.trace('I can not migrate to revision 0, skipping...')
        raise RuntimeError('Cannot migrate to revision 0')
    elif to_revision > len(migrations):
        logger.trace(
            f'I can not migrate to revision {to_revision} '
            f'knowing only {len(migrations)} revision(s), '
            f'skipping...',
        )
        return

    await _helper.migrations_table_create(
        config=config,
        migrations_table_schema=constants.MIGRATIONS_SCHEMA,
        migrations_table_name=constants.MIGRATIONS_TABLE,
    )
    maybe_db_revision = await _helper.latest_migration_revision(
        config=config,
        migrations_table_schema=constants.MIGRATIONS_SCHEMA,
        migrations_table_name=constants.MIGRATIONS_TABLE,
    )

    if maybe_db_revision is None:
        start_from_db_revision = 1
        logger.trace('Looks like we will run migration for first time')
    elif maybe_db_revision == to_revision:
        logger.trace(f'Already at {to_revision} (latest), skipping...')
        return
    else:
        start_from_db_revision = maybe_db_revision + 1
        if start_from_db_revision > to_revision:
            logger.trace(
                f'Current revision is {maybe_db_revision} and you '
                f'want to migrate to {to_revision}.\n'
                f'Cannot go backward when you want me to go UP, sorry :(',
            )
            return

    migrations_to_apply = migrations.slice(
        start=start_from_db_revision,
        end=to_revision,
    )
    logger.trace(f'Applying migrations {sorted(migrations_to_apply.keys())}')

    c = await asyncpg.connect(dsn=config.database_dsn)

    async with c.transaction():
        try:
            for migration in migrations_to_apply.upgrade_iterator():
                logger.trace(f'Applying {migration.revision}/{migration.label}')

                await migration.upgrade(c)
                await c.execute(
                    f'insert into '
                    f'{constants.MIGRATIONS_SCHEMA}.'
                    f'{constants.MIGRATIONS_TABLE}'
                    f' (revision, label, timestamp, direction)'
                    f' values ($1, $2, $3, $4)',
                    migration.revision,
                    migration.label,
                    dt.datetime.today(),
                    model.MigrationDir.UP,
                )

                yield 1
                await asyncio.sleep(1)
        except Exception as ex:
            logger.trace('Failed to upgrade...')
            raise RuntimeError(str(ex))

    c.terminate()
