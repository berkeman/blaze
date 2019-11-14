from collections import Counter, defaultdict
from datetime import date
"""
  Reconstruct table from [[https://www.backblaze.com/blog/hard-drive-stats-q2-2019]]
  - We get number of drives correct if we sample on 2019-06-30
  - We get almost correct number of fails if looking in window 2019-04-01 - 2019-06-30
  - Drive days seems correct if counting drive days only in window 2019-04-01 - 2019-06-30
"""

backblaze_fails = { # as reported in [[https://www.backblaze.com/blog/hard-drive-stats-q2-2019/]]
	'TOSHIBA MG07ACA14TA': 0,   'ST12000NM0007': 247,       'HGST HUH721212ALN604': 5,
	'HGST HUH721212ALE600': 1,  'ST10000NM0086': 2,         'ST8000DM002': 25,
	'ST8000NM0055': 64,         'HGST HUH728080ALE600': 2,  'ST6000DX000': 4,
	'HGST HMS5C4040BLE640': 15, 'ST4000DM000': 104,         'HGST HMS5C4040ALE640': 5,
	'TOSHIBA MD04ABA400V': 0,
}

datelow = date(2019, 4, 1)
datehigh = date(2019, 6, 30)
mindrivecnt = 60

def main(urd):

	# Get latest import
	imp = urd.peek_latest('imp/import').joblist[-1]

	# compute {ts: {model: count, }, } for running drives
	jid_dpd = urd.build('drivesperday', datasets=dict(source=imp))
	day2model2count = jid_dpd.load()

	# compute {ts: {model: count, }, } for failing drives
	jid_fails = urd.build('drivefails', datasets=dict(source=imp))
	day2model2fails = jid_fails.load()

	# compute {model: size, }
	jid_sizes = urd.build('drivesizes', datasets=dict(source=imp))
	model2size, _ = jid_sizes.load()

	def sumrange(x):
		# input x = {ts: {model: count, ...}, ...}
		# sum over ts, return    {model: count, ...}
		c = Counter()
		for ts, data in x.items():
			if datelow <= ts <= datehigh:
				for model, cnt in data.items():
					c[model] += cnt
		return c

	model2days = sumrange(day2model2count)
	model2fails = sumrange(day2model2fails)

	print()
	print("model                     size   #drives #drivedays   #fails        AFR      diff")
	for model, cnt in sorted(day2model2count[date(2019, 6, 30)].items(), key=lambda x: (-model2size[x[0]], x[0])):
		if cnt >= mindrivecnt:
			size = model2size[model]/1e12
			if size > 1:
				size = str(int(size)) + 'TB'
			else:
				size = str(int(1000*size)) + 'GB'
			diffs = ''
			if model in backblaze_fails:
				diff = model2fails[model] - backblaze_fails.get(model)
				if diff:
					diffs = "(%d)" % (diff,)
			print("%-20s %9s %9d %9d %9d %9.2f%% %9s" % (model, size, cnt, model2days[model], model2fails[model], 100*365*model2fails[model]/model2days[model], diffs))
