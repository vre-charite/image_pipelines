import pytest
from models import Node
from models import NodeList
from models import ResourceType
from models import append_suffix_to_filepath


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
    def test_is_folder_returns_true_if_labels_contain_folder_type(self, generate_node):
        node = generate_node(labels=[ResourceType.FOLDER])

        assert node.is_folder is True

    def test_is_folder_returns_false_if_labels_does_not_contain_folder_type(self, generate_node):
        node = generate_node(labels=[ResourceType.FILE])

        assert node.is_folder is False

    def test_is_file_returns_true_if_labels_contain_file_type(self, generate_node):
        node = generate_node(labels=[ResourceType.FILE])

        assert node.is_file is True

    def test_is_file_returns_false_if_labels_does_not_contain_file_type(self, generate_node):
        node = generate_node(labels=[ResourceType.FOLDER])

        assert node.is_file is False

    def test_is_archived_returns_true_if_archived_property_is_true(self, generate_node):
        node = generate_node(archived=True)

        assert node.is_archived is True

    def test_is_archived_returns_false_if_archived_property_is_false(self, generate_node):
        node = generate_node(archived=False)

        assert node.is_archived is False

    def test_geid_returns_global_entity_id(self, generate_node):
        node = generate_node()
        expected_geid = node['global_entity_id']

        assert node.geid == expected_geid

    def test_name_returns_name(self, generate_node):
        node = generate_node()
        expected_name = node['name']

        assert node.name == expected_name


class TestNodeList:
    def test_new_instance_converts_list_values_into_node_instances(self):
        nodes = NodeList([{'key': 'value'}])

        assert isinstance(nodes[0], Node)
