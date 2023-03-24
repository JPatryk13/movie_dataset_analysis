import unittest
from src.cleaning_data.tools import str_map


class TestStrMap(unittest.TestCase):
    def setUp(self) -> None:
        self.map_with_single_word = {
            "word": "foo"
        }
        self.map_with_two_words = {
            "word": "bar",
            " a ": " foo "
        }

        self.str_with_word = {
            "original": "This is a word",
            "replaced_one": "This is a foo",
            "replaced_two": "This is foo bar"
        }
        self.str_with_multiple_word = {
            "original": "This is a word within a word",
            "replaced_one": "This is a foo within a foo",
            "replaced_two": "This is foo bar within foo bar"
        }

    def test_str_map_raise_type_error(self) -> None:
        self.map_with_int = {"word": 1}

        with self.assertRaises(TypeError) as context:
            str_map(self.map_with_int, self.str_with_word["original"])

        self.assertTrue("must be str, not int" in str(context.exception))

    def test_str_map_replace_single_word_once(self) -> None:
        expected = self.str_with_word["replaced_one"]
        actual = str_map(self.map_with_single_word, self.str_with_word["original"])
        self.assertEqual(expected, actual)

    def test_str_map_replace_single_word_twice(self) -> None:
        expected = self.str_with_multiple_word["replaced_one"]
        actual = str_map(self.map_with_single_word, self.str_with_multiple_word["original"])
        self.assertEqual(expected, actual)

    def test_str_map_replace_two_words_once(self) -> None:
        expected = self.str_with_word["replaced_two"]
        actual = str_map(self.map_with_two_words, self.str_with_word["original"])
        self.assertEqual(expected, actual)

    def test_str_map_replace_two_words_twice(self) -> None:
        expected = self.str_with_multiple_word["replaced_two"]
        actual = str_map(self.map_with_two_words, self.str_with_multiple_word["original"])
        self.assertEqual(expected, actual)


if __name__ == "__main__":
    unittest.main()
