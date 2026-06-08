import unittest

from selection_ml.run_selection_ml import (
    CurrentRow,
    build_output_values,
    is_archive_spreadsheet,
    model_text,
    parse_target_sheets,
)


class SelectionMlTest(unittest.TestCase):
    def test_model_text_uses_efgi(self):
        row = [""] * 9
        row[4] = "E"
        row[5] = "F"
        row[6] = "G"
        row[8] = "I"
        self.assertEqual(model_text(row), "E\nF\nG\nI")

    def test_parse_target_sheets_deduplicates(self):
        self.assertEqual(parse_target_sheets("prod, dev,prod"), ["prod", "dev"])

    def test_parse_target_sheets_rejects_unknown_sheet(self):
        with self.assertRaises(RuntimeError):
            parse_target_sheets("prod,archive_prod")

    def test_archive_file_name_must_start_with_prod_prefix(self):
        self.assertTrue(is_archive_spreadsheet({"name": "prod_2026-05"}))
        self.assertFalse(is_archive_spreadsheet({"name": "dev_2026-05"}))
        self.assertFalse(is_archive_spreadsheet({"name": "archive_prod_2026-05"}))

    def test_output_keeps_blank_rows_aligned(self):
        rows = [CurrentRow(row_index=3, text="article")]
        values = build_output_values(rows, 3, [0.74], 10, 100)
        self.assertEqual(values[0], ["", "", ""])
        self.assertEqual(values[1][0], 74)
        self.assertEqual(values[2], ["", "", ""])


if __name__ == "__main__":
    unittest.main()
