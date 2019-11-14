from collections import defaultdict, Counter
from datetime import date

"""
Compute number of running drives per model per day in format
{ts: {model: count, }, }
"""

datasets = ('source',)

def analysis(sliceno):
	c = defaultdict(lambda: defaultdict(set))
	for m, s, ts in datasets.source.iterate_chain(sliceno, ('model', 'serial_number', 'date')):
		c[ts][m].add(s)

	d = defaultdict(dict)
	for ts, data in c.items():
		for m, s in data.items():
			d[ts][m] = len(s)
	return d

def synthesis(analysis_res):
	c = defaultdict(Counter)
	for res in analysis_res:
		for ts, data in res.items():
			for model, count in data.items():
				c[ts][model] += count
	return c
