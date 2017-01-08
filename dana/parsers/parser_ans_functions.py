__author__ = 'eliazarinelli'

def _trade_extract_dict(dict_in, name_mapping):

	dict_out = {}

	for k_out, k_in in name_mapping.items():
		dict_out[k_out] = dict_in[k_in]

	return dict_out