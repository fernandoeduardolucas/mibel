import argparse
import subprocess
from datetime import date, timedelta

def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    p.add_argument("--lookback_days", type=int, default=7)
    p.add_argument("--workflow", default="shopnow_kpis_v1")
    p.add_argument("--module", default="shopnow_flyte.py")
    p.add_argument("--remote", action="store_true")
    args = p.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    for d in daterange(start, end):
        cmd = ["pyflyte", "run"]
        if args.remote:
            cmd.append("--remote")
        cmd += [args.module, args.workflow,
                "--logical_date", d.isoformat(),
                "--lookback_days", str(args.lookback_days)]
        print("Running:", " ".join(cmd))
        subprocess.run(cmd, check=True)

if __name__ == "__main__":
    main()