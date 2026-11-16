"""Whether the manifest of a split PDF is being indexed, and under which generation.

Its own module so the transformer pipeline and the vector store can read it without importing the split machinery.
"""
from __future__ import annotations

from contextvars import ContextVar

_split_parent_gen: ContextVar[str | None] = ContextVar("split_parent_gen", default=None)


def split_parent_active() -> bool:
    """True while a split parent's manifest is being indexed: no classify or entities stage is
    dispatched for it, because the roll-up of its parts drives them."""
    return _split_parent_gen.get() is not None


def split_parent_gen() -> str | None:
    """The generation the manifest being indexed belongs to; its points are stamped with it."""
    return _split_parent_gen.get()


def begin_split_parent(gen: str) -> None:
    if not gen:
        raise ValueError("a split parent needs the generation its points are stamped with")
    _split_parent_gen.set(gen)


def end_split_parent() -> None:
    # Set back rather than reset by token: an async generator closed from another context cannot reset.
    _split_parent_gen.set(None)
