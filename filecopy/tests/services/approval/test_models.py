import random

from services.approval.models import ApprovalEntity
from services.approval.models import CopyStatus
from services.approval.models import EntityType
from services.approval.models import ReviewStatus


class TestApprovalEntity:
    def test_model_creates_successfully(self, faker):
        ApprovalEntity(
            id=faker.uuid4(),
            request_id=faker.uuid4(),
            entity_geid=faker.uuid4(),
            entity_type=random.choice(list(EntityType)),
            review_status=random.choice(list(ReviewStatus)),
            parent_geid=faker.uuid4(),
            copy_status=random.choice(list(CopyStatus)),
            name=faker.word(),
        )
