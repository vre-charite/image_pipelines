import random
import uuid
from typing import Callable

import pytest

from folder_copy import DuplicatedFileNames
from models import get_timestamp
from models import Node
from models import ResourceType
from neo4j_helper import Neo4jPathCheck


@pytest.fixture
def generate_node(faker) -> Callable[..., Node]:
    def _generate_node(global_entity_id=None, name=None, labels=None, archived=None) -> Node:
        if global_entity_id is None:
            global_entity_id = f'{uuid.uuid4()}-{get_timestamp()}'

        if name is None:
            name = faker.word()

        if labels is None:
            labels = [random.choice(list(ResourceType))]

        if archived is None:
            archived = faker.pybool()

        return Node(
            {
                'global_entity_id': global_entity_id,
                'name': name,
                'labels': labels,
                'archived': archived,
            }
        )

    return _generate_node


@pytest.fixture
def duplicated_files():
    yield DuplicatedFileNames()


@pytest.fixture
def path_check():
    yield Neo4jPathCheck('zone')
