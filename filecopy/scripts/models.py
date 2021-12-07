import time
from enum import Enum
from enum import unique
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Union


def get_timestamp() -> int:
    """Return current timestamp."""

    return round(time.time())


def append_suffix_to_filepath(filepath: str, suffix: Union[str, int], separator: str = '_') -> str:
    """Append suffix to filepath before extension."""

    path = Path(filepath)

    current_extension = ''.join(path.suffixes)
    new_extension = f'{separator}{suffix}{current_extension}'

    filename_parts = [path.name, '']
    if current_extension:
        filename_parts = path.name.rsplit(current_extension, 1)

    filename = new_extension.join(filename_parts)

    filepath = str(path.parent / filename)

    return filepath


@unique
class ResourceType(str, Enum):
    FOLDER = 'Folder'
    FILE = 'File'
    TRASH_FILE = 'TrashFile'
    CONTAINER = 'Container'


class Node(dict):
    """Store information about one node."""

    @property
    def is_folder(self) -> bool:
        return ResourceType.FOLDER in self['labels']

    @property
    def is_file(self) -> bool:
        return ResourceType.FILE in self['labels']

    @property
    def is_archived(self) -> bool:
        return self['archived'] is True

    @property
    def geid(self) -> str:
        return self['global_entity_id']

    @property
    def name(self) -> str:
        return self['name']


class NodeList(list):
    """Store list of Nodes."""

    def __init__(self, nodes: List[Dict[str, Any]]) -> None:
        super().__init__([Node(node) for node in nodes])
