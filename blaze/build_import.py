from collections import defaultdict

from accelerator.dataset import Dataset
from accelerator.build import profile_jobs
from accelerator.extras import job_post
def main(urd):

	urd.set_workdir('import')

	files = [
		("data_2013.zip", "2013-12-31"),
		("data_2014.zip", "2014-12-31"),
		("data_2015.zip", "2015-12-31"),
		("data_Q1_2016.zip", "2016-03-31"),
		("data_Q2_2016.zip", "2016-06-30"),
		("data_Q3_2016.zip", "2016-09-31"),
		("data_Q4_2016.zip", "2016-12-31"),
		("data_Q1_2017.zip", "2017-03-31"),
		("data_Q2_2017.zip", "2017-06-30"),
		("data_Q3_2017.zip", "2017-09-31"),
		("data_Q4_2017.zip", "2017-12-31"),
		("data_Q1_2018.zip", "2018-03-31"),
		("data_Q2_2018.zip", "2018-06-30"),
		("data_Q3_2018.zip", "2018-09-31"),
		("data_Q4_2018.zip", "2018-12-31"),
		("data_Q1_2019.zip", "2019-03-31"),
		("data_Q2_2019.zip", "2019-06-30"),
	]

	defaults = {}
	column2type = dict(
		capacity_bytes="int64_10",
		date="date:%Y-%m-%d",
		failure="strbool",
		model="ascii",
		serial_number="ascii",
	)
	smart_attrs = set(range(1, 6)) | set(range(7, 14)) | {15, 184, 187} | set(range(187, 202)) | {223, 225, 240, 241, 242, 250, 251, 252, 254, 255}
	# at the end also has
	# {16, 17, 22, 23, 24, 168, 170, 173, 174, 177, 179, 181, 182, 183, 218, 220, 222, 224, 226, 231, 232, 233, 235}
	# haven't looked more than that
	for attr in smart_attrs:
		column2type["smart_%d_normalized" % (attr,)] = "int32_10"
		column2type["smart_%d_raw" % (attr,)] = "int64_10"
		defaults["smart_%d_normalized" % (attr,)] = None
		defaults["smart_%d_raw" % (attr,)] = None

	previous = defaultdict(lambda :None)

	t = defaultdict(int)

	for file, date in files:
		urd.begin('import', date)
		jid = urd.build(
			"csvimport_zip",
			options=dict(
				filename=file,
				# The zips have some extra Mac gunk in them which we don't want
				exclude_re=r"(__MACOSX|\.DS_Store)",
				chaining="by_filename",
				strip_dirs=True,
			),
			datasets=dict(previous=previous['import']),
		)
		previous['import'] = jid

		jid = urd.build("dataset_rehash",
			options=dict(
				hashlabel='serial_number',
				length=-1,
			),
			datasets=dict(
				source=jid,
				previous=previous['hash'],
			),
		)
		previous['hash'] = jid

		jid = urd.build("dataset_type",
			options=dict(
				column2type=column2type,
				defaults=defaults),
			datasets=dict(
				source=jid,
				previous=previous['type'],
			),
		)
		previous['type'] = jid

		jid = urd.build("dataset_sort",
			options=dict(
				sort_columns='date',
			),
			datasets=dict(
				source=jid,
				previous=previous['sort'],
			),
		)
		previous['sort'] = jid

		for item in urd.joblist:
			x = job_post(item).profile.get('prepare') + job_post(item).profile.get('analysis') + job_post(item).profile.get('synthesis')
			t[item.method] += x


		urd.finish('import')

	for key, val in t.items():
		print(key, val)
