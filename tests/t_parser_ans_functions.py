__author__ = 'eliazarinelli'

import unittest

import os
import sys
sys.path.insert(0, os.path.abspath('..'))

import dana.parsers.parser_ans_functions as paf

class Test_trade_extract_dict(unittest.TestCase):

	def test_standard_case(self):

		# actual input
		dict_in = {
			'key_in_0': 'val_in_0',
			'key_in_1': 'val_in_1'
		}

		dict_mapping = {'key_out_1': 'key_in_1'}

		# expected output
		expected_output = {'key_out_1': 'val_in_1'}

		# actual output
		actual_output = paf._trade_extract_dict(dict_in, dict_mapping)

		self.assertDictEqual(expected_output, actual_output)