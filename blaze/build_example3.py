import os
from collections import defaultdict

def main(urd):

	PATH = '/home/ab/blaze.acc'

	files = [
		('bb_20190401.csv.gz', '2019-04-01'),
		('bb_20190401.csv.gz', '2019-04-02'),
		('bb_20190401.csv.gz', '2019-04-03'),
	]

	column2type = dict(
		capacity_bytes="int64_10",
		date="date:%Y-%m-%d",
		failure="strbool",
		model="ascii",
		serial_number="ascii",
	)

	previous = defaultdict(lambda :None)

	for file, date in files:
		urd.begin('import', date)
		jid = urd.build_chained("csvimport", name='import',
			options=dict(filename=os.path.join(PATH, file)),)
		jid = urd.build_chained("dataset_type", name='type',
			options=dict(column2type=column2type),
			datasets=dict(source=jid),)
		urd.finish('import')

