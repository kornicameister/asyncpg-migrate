import copy
from dataclasses import dataclass, field
import datetime as dt
import enum
from pathlib import Path
import typing as t

import asyncpg

Revision = t.NewType('Revision', int)
Timestamp = t.NewType('Timestamp', dt.datetime)
MigrationCallable = t.Callable[[asyncpg.Connection],
                               t.Callable[[asyncpg.Connection],
                                          t.Coroutine[t.Any,
                                                      t.Any,
                                                      None,
                                                      ],
                                          ],
                               ]


class MigrationDir(str, enum.Enum):
    UP = 'UP'
    DOWN = 'DOWN'


class Migrations(t.Dict[Revision, 'Migration']):
    def slice(self, start: int, end: t.Optional[int] = None) -> 'Migrations':
        real_end = len(self) if end is None else end

        if start > real_end:
            raise ValueError(f'Cannot slice if end={end} < start={start}')

        if start == 1 and real_end == len(self):
            return copy.deepcopy(self)
        else:
            new_migrations = Migrations()

            # have to add 1 to real_end because range is not non-inclusive at
            # the end
            for r in range(start, real_end + 1, 1):
                revision = Revision(r)
                new_migrations[revision] = self[revision]
            return new_migrations

    def upgrade_iterator(self) -> t.Iterator['Migration']:
        return iter([self[revision] for revision in sorted(self.keys())])

    def downgrade_iterator(self) -> t.Iterator['Migration']:
        return iter([self[revision] for revision in sorted(self.keys(), reverse=True)])


@dataclass(frozen=True)
class Migration:
    revision: Revision
    label: str
    path: Path
    upgrade: MigrationCallable = field(
        hash=False,
        compare=False,
    )
    downgrade: MigrationCallable = field(
        hash=False,
        compare=False,
    )


@dataclass(frozen=True)
class MigrationHistoryEntry:
    revision: Revision = field(hash=True, compare=True)
    timestamp: Timestamp = field(hash=True, compare=True)
    direction: MigrationDir = field(hash=True, compare=True)
    label: str = field(hash=False, compare=False)


class MigrationHistory(t.Set[MigrationHistoryEntry]):
    ...


@dataclass(frozen=True)
class Config:
    script_location: Path
    database_dsn: str
    database_name: str
