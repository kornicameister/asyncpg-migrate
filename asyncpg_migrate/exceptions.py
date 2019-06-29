class MigrationLoadError(Exception):
    """MigrationLoadError happens if there is something wrong with migration.

    This exception will be thrown whenever a bad thing about a migration file
    is discovered by tool.

    """
    ...
