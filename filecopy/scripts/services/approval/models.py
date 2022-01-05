from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy.engine import CursorResult


class EntityType(str, Enum):
    FOLDER = 'folder'
    FILE = 'file'


class ReviewStatus(str, Enum):
    DENIED = 'denied'
    PENDING = 'pending'
    APPROVED = 'approved'


class CopyStatus(str, Enum):
    PENDING = 'pending'
    COPIED = 'copied'


class ApprovalEntity(BaseModel):
    """Model to represent one approval entity."""

    id: UUID
    request_id: Optional[UUID]
    entity_geid: Optional[str]
    entity_type: Optional[EntityType]
    review_status: Optional[ReviewStatus]
    parent_geid: Optional[str]
    copy_status: Optional[CopyStatus]
    name: str

    class Config:
        orm_mode = True

    @property
    def is_approved_for_copy(self) -> bool:
        return (
            self.entity_type == EntityType.FILE
            and self.review_status == ReviewStatus.APPROVED
            and self.copy_status == CopyStatus.PENDING
        )


class ApprovalRequest(BaseModel):
    """Model to represent one approval request."""

    id: UUID
    destination_geid: str
    source_geid: str
    destination_path: str
    source_path: str

    class Config:
        orm_mode = True


class ApprovalEntityPath(list):
    """Store path (folders only) to one approval entity considering parents."""

    def __str__(self) -> str:
        return '/'.join([node.name for node in self])


class ApprovedApprovalEntities(dict):
    """Store only approved approval entities using entity geid as a key."""


class ApprovalEntities(dict):
    """Store multiple approval entities from one request using entity geid as a key."""

    @classmethod
    def from_cursor(cls, result: CursorResult):
        """Load approval entities from sqlalchemy cursor result."""

        instance = cls()
        for entity in result:
            approval_entity = ApprovalEntity.from_orm(entity)
            instance[approval_entity.entity_geid] = approval_entity

        return instance

    def get_approved(self) -> ApprovedApprovalEntities:
        """Return only approved file entities with pending copy status."""

        approved_entities = ApprovedApprovalEntities()

        for entity_geid, entity in self.items():
            if entity.is_approved_for_copy:
                approved_entities[entity.entity_geid] = entity

        return approved_entities

    def get_top_parent_geid(self, entity_geid: str) -> str:
        """Return top most folder geid among all entities."""

        try:
            entity = self[entity_geid]
            assert entity.entity_type == EntityType.FOLDER
            parent_geid = entity.parent_geid
        except (KeyError, AssertionError):
            parent_geid = None

        if parent_geid is None:
            return entity_geid

        return self.get_top_parent_geid(parent_geid)

    def get_path_until_top_parent(self, approval_entity: ApprovalEntity) -> ApprovalEntityPath:
        """Return path to one approval entity."""

        current = approval_entity
        entity_full_path = ApprovalEntityPath()

        while current.parent_geid:
            current = self[current.parent_geid]
            entity_full_path.insert(0, current)

        return entity_full_path
