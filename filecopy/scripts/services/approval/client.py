from typing import Optional
from uuid import uuid4

from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy import update
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.future import Engine

from config import ConfigClass
from services.approval.models import ApprovalEntity
from services.approval.models import ApprovedEntities
from services.approval.models import CopyStatus
from services.approval.models import EntityType
from services.approval.models import ReviewStatus


class ApprovalEntityClient:
    """Get information about approval entities for copy request."""

    def __init__(self, engine: Optional[Engine] = None, metadata: Optional[MetaData] = None):
        if engine is None:
            engine = create_engine(url=ConfigClass.RDS_DB_URI, future=True)
        self.engine = engine

        if metadata is None:
            metadata = MetaData(schema=ConfigClass.RDS_SCHEMA_DEFAULT)
        self.metadata = metadata

        self.approval_entity = Table(
            'approval_entity',
            self.metadata,
            Column('id', UUID(as_uuid=True), unique=True, primary_key=True, default=uuid4),
            keep_existing=True,
            autoload_with=self.engine,
        )

    def get_approved_entities(self, request_id: str) -> ApprovedEntities:
        """Return only approved file entities related to request id with pending copy status."""

        statement = select(self.approval_entity).filter_by(
            request_id=request_id,
            entity_type=EntityType.FILE,
            review_status=ReviewStatus.APPROVED,
            copy_status=CopyStatus.PENDING,
        )

        approved_entities = ApprovedEntities()

        result = self.engine.connect().execute(statement)
        for entity in result:
            approval_entity = ApprovalEntity.from_orm(entity)
            approved_entities[approval_entity.entity_geid] = approval_entity

        return approved_entities

    def update_copy_status(self, approval_entity: ApprovalEntity, copy_status: CopyStatus) -> None:
        """Update copy status field for approval entity."""

        statement = (
            update(self.approval_entity)
            .where(self.approval_entity.columns.id == approval_entity.id)
            .values(copy_status=copy_status)
        )

        with self.engine.begin() as connection:
            connection.execute(statement)
