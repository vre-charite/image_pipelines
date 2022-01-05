from uuid import uuid4

import pytest
from sqlalchemy import Column
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy.future import create_engine

from services.approval.client import ApprovalServiceClient
from services.approval.models import ApprovalEntities


@pytest.fixture
def inmemory_engine():
    yield create_engine('sqlite:///:memory:', future=True)


@pytest.fixture
def metadata(inmemory_engine):
    metadata = MetaData()

    Table(
        'approval_entity',
        metadata,
        Column('id', String(), unique=True, primary_key=True, default=uuid4),
        Column('request_id', String()),
        Column('entity_type', String()),
        Column('review_status', String()),
    )
    Table(
        'approval_request',
        metadata,
        Column('id', String(), unique=True, primary_key=True, default=uuid4),
        Column('destination_geid', String()),
        Column('source_geid', String()),
        Column('destination_path', String()),
        Column('source_path', String()),
    )
    metadata.create_all(inmemory_engine)

    with inmemory_engine.connect() as connection:
        with connection.begin():
            metadata.create_all(connection)

    yield metadata


@pytest.fixture
def approval_service_client(inmemory_engine, metadata):
    yield ApprovalServiceClient(inmemory_engine, metadata)


class TestApprovalServiceClient:
    def test_get_approval_entities_returns_instance_of_approval_entities(self, approval_service_client, faker):
        request_id = faker.uuid4()

        result = approval_service_client.get_approval_entities(request_id)

        assert isinstance(result, ApprovalEntities)
