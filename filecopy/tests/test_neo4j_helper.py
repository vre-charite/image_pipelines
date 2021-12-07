class TestNeo4jPathCheck:
    def test_is_file_exists_returns_true_if_node_exists(self, path_check, mocker, generate_node):
        mocker.patch.object(path_check, '_get_node', return_value=generate_node())

        received_response = path_check.is_file_exists('code', 'path')

        assert received_response is True

    def test_is_file_exists_returns_false_if_node_does_not_exist(self, path_check, mocker):
        mocker.patch.object(path_check, '_get_node', return_value=None)

        received_response = path_check.is_file_exists('code', 'path')

        assert received_response is False

    def test_is_folder_exists_returns_true_if_node_exists(self, path_check, mocker, generate_node):
        mocker.patch.object(path_check, '_get_node', return_value=generate_node())

        received_response = path_check.is_folder_exists('code', 'path')

        assert received_response is True

    def test_is_folder_exists_returns_false_if_node_does_not_exist(self, path_check, mocker):
        mocker.patch.object(path_check, '_get_node', return_value=None)

        received_response = path_check.is_folder_exists('code', 'path')

        assert received_response is False

    def test_get_folder_exists_returns_node_when_folder_is_available(self, path_check, mocker, generate_node):
        expected_response = generate_node()
        mocker.patch.object(path_check, '_get_node', return_value=expected_response)

        received_response = path_check.get_folder('code', 'path')

        assert received_response == expected_response

    def test_get_folder_exists_returns_none_when_folder_is_not_available(self, path_check, mocker):
        mocker.patch.object(path_check, '_get_node', return_value=None)

        received_response = path_check.get_folder('code', 'path')

        assert received_response is None

    def test_get_file_exists_returns_node_when_file_is_available(self, path_check, mocker, generate_node):
        expected_response = generate_node()
        mocker.patch.object(path_check, '_get_node', return_value=expected_response)

        received_response = path_check.get_file('code', 'path')

        assert received_response == expected_response

    def test_get_file_exists_returns_none_when_file_is_not_available(self, path_check, mocker):
        mocker.patch.object(path_check, '_get_node', return_value=None)

        received_response = path_check.get_file('code', 'path')

        assert received_response is None
