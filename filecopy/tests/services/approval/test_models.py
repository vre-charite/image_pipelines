# Copyright 2022 Indoc Research
# 
# Licensed under the EUPL, Version 1.2 or – as soon they
# will be approved by the European Commission - subsequent
# versions of the EUPL (the "Licence");
# You may not use this work except in compliance with the
# Licence.
# You may obtain a copy of the Licence at:
# 
# https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12
# 
# Unless required by applicable law or agreed to in
# writing, software distributed under the Licence is
# distributed on an "AS IS" basis,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied.
# See the Licence for the specific language governing
# permissions and limitations under the Licence.
# 

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
