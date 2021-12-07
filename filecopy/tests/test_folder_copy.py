from pathlib import Path


class TestDuplicatedFileNames:
    def test_add_stores_filename_with_timestamp_for_given_filepath(self, duplicated_files, faker):
        filepath = faker.file_path(depth=3)
        expected_filename = f'{Path(filepath).stem}_{duplicated_files.filename_timestamp}{Path(filepath).suffix}'

        received_filename = duplicated_files.add(filepath)

        assert received_filename == expected_filename

    def test_get_returns_existing_filename_by_filepath(self, duplicated_files, faker):
        filepath = faker.file_path(depth=3)
        expected_filename = duplicated_files.add(filepath)

        received_filename = duplicated_files.get(filepath)

        assert received_filename == expected_filename

    def test_get_returns_default_value_if_filename_does_not_exist(self, duplicated_files, faker):
        filepath = faker.file_path(depth=3)
        expected_filename = faker.pystr()

        received_filename = duplicated_files.get(filepath, expected_filename)

        assert received_filename == expected_filename
