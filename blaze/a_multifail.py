from collections import defaultdict

datasets = ('source',)

def analysis(sliceno):
	c = defaultdict(set)
	for m, s, f, ts in datasets.source.iterate_chain(sliceno, ('model', 'serial_number', 'failure', 'date')):
		key = '\0'.join((m, s))
		if f:
			c[key].add(ts)
	return c

def synthesis(analysis_res):
	c = analysis_res.merge_auto()
	ret = {}
	for item, fails in sorted(c.items()):
		if len(fails) > 1:
			ret[item] = fails
	return ret
