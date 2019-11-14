
def main(urd):

	print(urd.list())

	# Import all files stored in the .zip-file into separate, chained datasets
	jid = urd.build("csvimport_zip", options=dict(filename="data_Q2_2019.zip", exclude_re=r"(__MACOSX|\.DS_Store)", chaining="by_filename", strip_dirs=True,),)

	# Set the urd list to beginning of time
	urd.truncate('testo_import', 0)

	# Since we will use the import job over and over again for different tasks,
	# we store a reference to it in the Urd database.
	urd.begin('testo_import', "2019-06-30")
	# Do the same csvimport_zip-job again.  Note that since it has been run
	# previously, the Accelerator will just look it up and return a reference to it.
	jid = urd.build("csvimport_zip", options=dict(filename="data_Q2_2019.zip", exclude_re=r"(__MACOSX|\.DS_Store)", chaining="by_filename", strip_dirs=True,),)
	urd.finish("testo_import")


	# Assume we do not know any details about the import above, we just want to
	# do some processing on the latest imported file, so
	# Find all items in import list
	timestamps = urd.since('testo_import', 0)
	# Extract latest one
	timestamp = timestamps[-1]
	# Actually, instead we could
	urdobj = urd.peek_latest('testo_import')
	print(urdobj.joblist.pretty)
	print(urdobj.timestamp)


	# Create new job that uses the last import job we just found.
	# We create a new Urd list for it, too
	urd.begin('testo_process', timestamp)
	urdresp = urd.get('testo_import', timestamp)
	job = urdresp.joblist[0]
	jid = urd.build("dataset_rehash", options=dict(hashlabel='serial_number', length=-1,), datasets=dict(source=job, previous=None,),)
	urd.finish('testo_process')


	# Let's examine what Urd knows about the processing job
	urdresp = urd.peek('testo_process', timestamp)
	print(urdresp.joblist)
	print(urdresp.deps)
