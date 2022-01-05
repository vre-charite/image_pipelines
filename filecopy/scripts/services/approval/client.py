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
from services.approval.models import ApprovalRequest
from services.approval.models import CopyStatus
from services.approval.models import ApprovalEntities


class ApprovalServiceClient:
    """Get information about approval request or entities for copy request."""

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
        self.approval_request = Table(
            'approval_request',
            self.metadata,
            Column('id', UUID(as_uuid=True), unique=True, primary_key=True, default=uuid4),
            keep_existing=True,
            autoload_with=self.engine,
        )

    def get_approval_request(self, request_id: str) -> ApprovalRequest:
        """Return approval request by id."""

        statement = select(self.approval_request).filter_by(id=request_id)
        cursor = self.engine.connect().execute(statement)

        approval_request = ApprovalRequest.from_orm(cursor.fetchone())

        return approval_request

    def get_approval_entities(self, request_id: str) -> ApprovalEntities:
        """Return all approval entities related to request id."""

        statement = select(self.approval_entity).filter_by(request_id=request_id)
        cursor = self.engine.connect().execute(statement)

        request_approval_entities = ApprovalEntities.from_cursor(cursor)

        return request_approval_entities

    def update_copy_status(self, approval_entity: ApprovalEntity, copy_status: CopyStatus) -> None:
        """Update copy status field for approval entity."""

        statement = (
            update(self.approval_entity)
            .where(self.approval_entity.columns.id == approval_entity.id)
            .values(copy_status=copy_status)
        )

        with self.engine.begin() as connection:
            connection.execute(statement)
