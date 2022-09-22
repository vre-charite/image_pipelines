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

import pytest
from scripts.models import Node
from scripts.models import NodeList
from scripts.models import ResourceType
from scripts.models import append_suffix_to_filepath


@pytest.mark.parametrize(
    'filepath,suffix,expected',
    [
        ('file.tar.gz', 'suffix', 'file_suffix.tar.gz'),
        ('/path/to/file.tar.gz', 'outdated', '/path/to/file_outdated.tar.gz'),
        ('folder.tar.gz/file.tar.gz', 'tar', 'folder.tar.gz/file_tar.tar.gz'),
        ('image.jpg', 1638552343, 'image_1638552343.jpg'),
        ('file', 'suffix', 'file_suffix'),
        ('path/to/file', 'suffix', 'path/to/file_suffix'),
    ],
)
def test_append_suffix_to_filename_appends_suffix_as_expected(filepath, suffix, expected):
    assert append_suffix_to_filepath(filepath, suffix) == expected


class TestNode:
    def test_is_folder_returns_true_if_labels_contain_folder_type(self, create_node):
        node = create_node(labels=[ResourceType.FOLDER])

        assert node.is_folder is True

    def test_is_folder_returns_false_if_labels_does_not_contain_folder_type(self, create_node):
        node = create_node(labels=[ResourceType.FILE])

        assert node.is_folder is False

    def test_is_file_returns_true_if_labels_contain_file_type(self, create_node):
        node = create_node(labels=[ResourceType.FILE])

        assert node.is_file is True

    def test_is_file_returns_false_if_labels_does_not_contain_file_type(self, create_node):
        node = create_node(labels=[ResourceType.FOLDER])

        assert node.is_file is False

    def test_is_archived_returns_true_if_archived_property_is_true(self, create_node):
        node = create_node(archived=True)

        assert node.is_archived is True

    def test_is_archived_returns_false_if_archived_property_is_false(self, create_node):
        node = create_node(archived=False)

        assert node.is_archived is False

    def test_geid_returns_global_entity_id(self, create_node):
        node = create_node()
        expected_geid = node['global_entity_id']

        assert node.geid == expected_geid

    def test_name_returns_name(self, create_node):
        node = create_node()
        expected_name = node['name']

        assert node.name == expected_name


class TestNodeList:
    def test_new_instance_converts_list_values_into_node_instances(self):
        nodes = NodeList([{'key': 'value'}])

        assert isinstance(nodes[0], Node)
