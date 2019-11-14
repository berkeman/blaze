def main(urd):

	print('-' * 80)
	print('First, we list all items since beginning of time stored in the urd list \'imp/import\':')
	print()
	print(urd.since('imp/import', 0))

	print('-' * 80)
	print('Extract the latest item in the list and briefly examine it:')
	urdresp = urd.peek_latest('imp/import')
	print()
	print('  user     ', urdresp.user)
	print('  build    ', urdresp.build)
	print('  timestamp', urdresp.timestamp)

	print('-' * 80)
	print('A closer look at the item\'s .joblist:')
	print()
	joblist = urdresp.joblist
	# do a pretty print
	print(joblist.pretty)
	# and print execution times
	total, perjob = joblist.profile
	print('  total exec time', total)
	print('  exec time per job')
	for name, xtime in perjob.items():
		print('    %-20s %9f' % (name, xtime))

	print('-' * 80)
	print('Turn attention to the last job in the joblist')
	print()
	job = joblist[-1]
	# one can also use .get()
	assert job == joblist.get('dataset_sort')
	print('  last job in joblist ', job)
	print('  method              ', job.method)
	print('  datasets in job     ', job.datasets())

	print('-' * 80)
	print('A closer look at the job\'s dataset')
	print()
	ds = job.datasets()[0]
	print('  num lines', ds.lines)
	print(' some columns')
	for ix, (name, data) in enumerate(ds.columns.items()):
		print('   %-20s %10s %20s' % (name, data.type, data.max))
		if ix == 5:
			break
	print('-' * 80)

	print()

	print("""Note, this is only scratching the surface, see documentation for more information on the
    - UrdResponse,
    - Joblist,
    - Job, and
    - Dataset
classes.""")
	
