import typing as t

import pytest

from asyncpg_migrate import model


@pytest.mark.parametrize(
    'test_revision,expected_revision,all_revisions',
    [
        ('HEAD', 2, [1, 2]),
        ('HEAD', ValueError, []),
        ('base', 1, [1, 2, 3, 4, 5]),
        ('base', ValueError, []),
        ('head', 3, [1, 2, 3]),
        ('BASE', 1, [1]),
        (1, 1, [1, 2]),
        (2, 2, [1, 2]),
        (-1, ValueError, [1, 2]),
    ],
)
def test_revision_decoding(
        test_revision: t.Union[str, int],
        expected_revision: t.Union[model.Revision, Exception],
        all_revisions: t.Sequence[model.Revision],
) -> None:
    if type(expected_revision) == int:
        assert model.Revision.decode(
            test_revision,
            all_revisions,
        ) == expected_revision
    else:
        with pytest.raises(expected_revision):
            model.Revision.decode(
                test_revision,
                all_revisions,
            )
