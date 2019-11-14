from collections import defaultdict

datasets=('source',)

def analysis(sliceno):
	return set(datasets.source.iterate_chain(sliceno, ('model', 'serial_number')))

def synthesis(analysis_res):
	res = analysis_res.merge_auto()
	h = defaultdict(set)
	for model, serial in res:
		h[serial].add(model)
	return tuple((s, v) for s, v in h.items() if len(v) > 1)
