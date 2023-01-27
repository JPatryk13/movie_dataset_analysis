from typing import Mapping


def str_map(mapping: Mapping[str, str], _str: str) -> str:
    """
    Replaces parts of the string based on mapping dict i.e. applies _str.replace(key, value) for each key-value pair

    :param mapping: dict-type with str-type key-value pairs
    :param _str: input string
    :return: output string with mapped elements
    """
    for k, v in mapping.items():
        _str = _str.replace(k, v)

    return _str
