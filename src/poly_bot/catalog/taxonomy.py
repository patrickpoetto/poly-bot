"""Manual taxonomy overrides — load / save / lookup."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import structlog
from sqlmodel import Session, select

from poly_bot.persistence.schema import TaxonomyOverride

logger = structlog.get_logger(__name__)

OverrideKind = Literal["tag", "condition_id", "slug_prefix"]
_VALID_KINDS: tuple[str, ...] = ("tag", "condition_id", "slug_prefix")


@dataclass(frozen=True)
class OverrideHit:
    domain: str | None
    structure: str | None
    resolution_source: str | None


class TaxonomyOverrideStore:
    """In-memory snapshot of overrides for fast classification."""

    def __init__(self) -> None:
        self._by_condition: dict[str, OverrideHit] = {}
        self._by_tag: dict[str, OverrideHit] = {}
        self._by_slug_prefix: list[tuple[str, OverrideHit]] = []

    @classmethod
    def load(cls, session: Session) -> TaxonomyOverrideStore:
        store = cls()
        rows = session.exec(select(TaxonomyOverride)).all()
        for row in rows:
            store._index(row)
        return store

    def _index(self, row: TaxonomyOverride) -> None:
        hit = OverrideHit(
            domain=row.domain,
            structure=row.structure,
            resolution_source=row.resolution_source,
        )
        key = row.key.lower()
        if row.kind == "condition_id":
            self._by_condition[row.key] = hit
        elif row.kind == "tag":
            self._by_tag[key] = hit
        elif row.kind == "slug_prefix":
            self._by_slug_prefix.append((key, hit))

    def lookup(
        self,
        *,
        condition_id: str | None = None,
        slug: str | None = None,
        tags: list[str] | None = None,
    ) -> OverrideHit | None:
        if condition_id and condition_id in self._by_condition:
            return self._by_condition[condition_id]
        slug_l = (slug or "").lower()
        for prefix, hit in self._by_slug_prefix:
            if slug_l.startswith(prefix):
                return hit
        for tag in tags or []:
            tag_l = tag.lower()
            if tag_l in self._by_tag:
                return self._by_tag[tag_l]
        return None


def list_overrides(session: Session) -> list[TaxonomyOverride]:
    stmt = select(TaxonomyOverride).order_by(TaxonomyOverride.id.desc())  # type: ignore[union-attr]
    return list(session.exec(stmt).all())


def upsert_override(
    session: Session,
    *,
    kind: OverrideKind,
    key: str,
    domain: str | None = None,
    structure: str | None = None,
    resolution_source: str | None = None,
    note: str = "",
) -> TaxonomyOverride:
    if kind not in _VALID_KINDS:
        msg = f"invalid override kind: {kind}"
        raise ValueError(msg)
    if not key:
        msg = "override key required"
        raise ValueError(msg)
    if not (domain or structure or resolution_source):
        msg = "override must set at least one of domain/structure/resolution_source"
        raise ValueError(msg)

    row = session.exec(
        select(TaxonomyOverride).where(
            TaxonomyOverride.kind == kind,
            TaxonomyOverride.key == key,
        )
    ).first()
    if row is None:
        row = TaxonomyOverride(kind=kind, key=key)
    row.domain = domain or row.domain
    row.structure = structure or row.structure
    row.resolution_source = resolution_source or row.resolution_source
    row.note = note or row.note
    session.add(row)
    session.commit()
    session.refresh(row)
    logger.info("taxonomy_override_upsert", kind=kind, key=key, domain=domain)
    return row


def delete_override(session: Session, override_id: int) -> bool:
    row = session.get(TaxonomyOverride, override_id)
    if row is None:
        return False
    session.delete(row)
    session.commit()
    return True
