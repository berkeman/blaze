from collections import Counter, defaultdict
from datetime import date

"""
Return number of failing drives per model per day in format
{ts: {model: count, }, }
"""

datasets = ('source',)

def analysis(sliceno):
	c = defaultdict(Counter)
	for m, f, ts in datasets.source.iterate_chain(sliceno, ('model', 'failure', 'date')):
		if f:
			c[ts][m] += 1
	return c

def synthesis(analysis_res):
	return analysis_res.merge_auto()
