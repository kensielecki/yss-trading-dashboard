"""Prune pipeline logs older than 8 weeks from output/logs/."""
import os
import glob
from datetime import datetime, timedelta

LOG_DIR = "output/logs"
cutoff = datetime.utcnow() - timedelta(weeks=8)

pruned = 0
for path in glob.glob(os.path.join(LOG_DIR, "??????_????_pipeline.log")):
    try:
        file_date = datetime.strptime(os.path.basename(path)[:6], "%y%m%d")
        if file_date < cutoff:
            os.remove(path)
            print(f"Pruned: {path}")
            pruned += 1
    except ValueError:
        pass

print(f"Pruned {pruned} log file(s).")
