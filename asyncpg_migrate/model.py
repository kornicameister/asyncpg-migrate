import copy
from dataclasses import dataclass, field
import datetime as dt
import enum
from pathlib import Path
import typing as t

import asyncpg

Timestamp = t.NewType('Timestamp', dt.datetime)
MigrationCallable = t.Callable[[asyncpg.Connection],
                               t.Callable[[asyncpg.Connection],
                                          t.Coroutine[t.Any,
                                                      t.Any,
                                                      None,
                                                      ],
                                          ],
                               ]


class Revision(int):
    @classmethod
    def decode(
            cls,
            rev: t.Union[str, int, 'Revision'],
            all_revisions: t.Sequence['Revision'] = None,
    ) -> 'Revision':
        if isinstance(rev, Revision):
            return rev
        elif isinstance(rev, int):
            if rev >= 0:
                return Revision(rev)
            else:
                raise ValueError('Decoding from negative value is not possible')
        else:
            try:
                return cls.decode(int(rev))
            except ValueError:
                if not all_revisions:
                    raise ValueError(
                        'Decoding from "head" or "base" require knowing all revisions',
                    )
                if rev.lower() == 'head':
                    return all_revisions[-1]
                elif rev.lower() == 'base':
                    return all_revisions[0]
                else:
                    raise ValueError(
                        f'{rev} is neither "base" nor "head" and'
                        f' thus cannot be converted from string',
                    )


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
        return iter([self[rev] for rev in sorted(self)])

    def downgrade_iterator(self) -> t.Iterator['Migration']:
        return iter([self[rev] for rev in sorted(self, reverse=True)])

    def revisions(self) -> t.Sequence[Revision]:
        return sorted(self.keys())


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


class MigrationHistory(t.List[MigrationHistoryEntry]):
    ...


@dataclass(frozen=True)
class Config:
    script_location: Path
    database_dsn: str = field(repr=False)
    database_name: str
