""" Generate a file for plotting with number of active drives per day and model """

def main(urd):

	imp = urd.peek_latest('imp/import').joblist[-1]

	# compute {ts: {model: count, }, } for running drives
	jid_dpd = urd.build('drivesperday', datasets=dict(source=imp))
	day2model2count = jid_dpd.load()

	# find all drives
	drives = set.union(*(set(x.keys()) for x in day2model2count.values()))

	with open('activedrives.txt', 'wt') as fh:
		fh.write('# ' + ' '.join(drives) + '\n')
		for ts, data in sorted(day2model2count.items()):
			fh.write(' '.join((str(ts),) + tuple(str(data.get(model, 0)) for model in drives)) + '\n')
