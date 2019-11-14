""" Verify hypotheses etc """

def main(urd):

	# Get latest import
	imp = urd.peek_latest('imp/import').joblist[-1]

	# See if any serial_number is shared between models
	job = urd.build('validatehashinghypo', datasets=dict(source=imp))
	print('\nSerial_number with more than one model:')
	for item in job.load():
		print(item)

	# See if all models have one unique size
	jid_sizes = urd.build('drivesizes', datasets=dict(source=imp))
	_, model2sizes = jid_sizes.load()
	print('\nDrives with more than one size')
	for model, v in model2sizes.items():
		if len(v) > 1:
			print(model)
			for item in v.most_common():
				print('    {:30n} {:9d}'.format(*item))

	# See if drives fail more than once
	jid_multifail = urd.build('multifail', datasets=dict(source=imp))
	print('\nDrives failing more than once!')
	for item, failv in jid_multifail.load().items():
		model, serial = item.split('\0')
		print("%20s %20s %s" % (model, serial, str(list(x.strftime("%Y-%m-%d") for x in sorted(failv)))))
