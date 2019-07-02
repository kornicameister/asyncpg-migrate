from asyncpg_migrate import constants

def test_migration_table_location() -> None:
    assert constants.MIGRATIONS_TABLE == '_migrations_'
    assert constants.MIGRATIONS_SCHEMA == 'public'
