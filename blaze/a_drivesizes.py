from collections import Counter, defaultdict

datasets = ('source',)

"""
  Return dict {model: size, ...}.
  In a few cases, sizes are reported as -1, so we return
  the most common value and hope for the best.
"""


def analysis(sliceno):
	c = defaultdict(Counter)
	for m, size in datasets.source.iterate_chain(sliceno, ('model', 'capacity_bytes')):
		c[m][size] += 1
	return c

def synthesis(analysis_res):
	res = Counter()
	c = analysis_res.merge_auto()
	for model, data in c.items():
		res[model] = data.most_common()[0][0]
	return res, c
