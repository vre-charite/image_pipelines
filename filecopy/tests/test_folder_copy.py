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
