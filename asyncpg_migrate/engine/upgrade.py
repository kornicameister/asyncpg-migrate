import asyncio
import datetime as dt
import typing as t

import asyncpg
from loguru import logger

from asyncpg_migrate import constants
from asyncpg_migrate import loader
from asyncpg_migrate import model
from asyncpg_migrate.engine import migration

LOG = logger.opt(record=True)


async def run(
        config: model.Config,
        target_revision: t.Union[str, int],
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
    if not migrations:
        LOG.info('There are no migrations scripts, skipping...')
        yield -1
        return

    to_revision = model.Revision.decode(
        target_revision,
        list(migrations.keys()),
    )

    await migration.create_table(
        config=config,
        table_schema=constants.MIGRATIONS_SCHEMA,
        table_name=constants.MIGRATIONS_TABLE,
    )
    maybe_db_revision = await migration.latest_revision(
        config=config,
        table_schema=constants.MIGRATIONS_SCHEMA,
        table_name=constants.MIGRATIONS_TABLE,
    )

    if maybe_db_revision is None:
        start_from_db_revision = 1
        LOG.debug('Looks like we will run migration for first time')
    elif maybe_db_revision == to_revision:
        LOG.trace(f'Already at {to_revision} (latest), skipping...')
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
            for m in migrations_to_apply.upgrade_iterator():
                LOG.trace(f'Applying {m.revision}/{m.label}')

                await m.upgrade(c)
                await c.execute(
                    f'insert into '
                    f'{constants.MIGRATIONS_SCHEMA}.'
                    f'{constants.MIGRATIONS_TABLE}'
                    f' (revision, label, timestamp, direction)'
                    f' values ($1, $2, $3, $4)',
                    m.revision,
                    m.label,
                    dt.datetime.today(),
                    model.MigrationDir.UP,
                )

                yield 1
                await asyncio.sleep(1)
        except Exception as ex:
            LOG.trace('Failed to upgrade...')
            raise RuntimeError(str(ex))

    c.terminate()
