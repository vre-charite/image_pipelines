from uuid import uuid4

import pytest
from sqlalchemy import Column
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy.future import create_engine

from services.approval.client import ApprovalEntityClient
from services.approval.models import ApprovedEntities


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
    metadata.create_all(inmemory_engine)

    with inmemory_engine.connect() as connection:
        with connection.begin():
            metadata.create_all(connection)

    yield metadata


@pytest.fixture
def approval_entity_client(inmemory_engine, metadata):
    yield ApprovalEntityClient(inmemory_engine, metadata)


class TestApprovalEntityClient:
    def test_get_approved_entities_returns_instance_of_approved_entities(self, approval_entity_client, faker):
        request_id = faker.uuid4()

        result = approval_entity_client.get_approved_entities(request_id)

        assert isinstance(result, ApprovedEntities)
