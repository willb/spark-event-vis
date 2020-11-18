import papermill as pm
import argparse
import os
import sys
import re
import sqlite3

parser = parser = argparse.ArgumentParser()
parser.add_argument('--master', help='Spark master URL (default: "local[*]")', default="local[*]")
parser.add_argument('--outdir', help='where to store postprocessed notebooks and per-app databases (default: "outputs")', default="outputs")
parser.add_argument('--per-app-db', help='use a separate output database for each event log', action='store_true', default=False)
parser.add_argument('--fail-fast', help='terminate if processing a single log fails', action='store_true', default=False)
parser.add_argument('--db', help='database file to store/append aggregated postprocessed events to (default="wide-output.db")', default="wide-output.db")
parser.add_argument('--config', metavar="KEY=VAL", help="add KEY=VAL to Spark's configuration", action='append', default=[], dest='config')
parser.add_argument('files', metavar="FILE", nargs="+")

def vacuum_analyze(dbfile):
    conn = sqlite3.Connection(dbfile)
    conn.execute("vacuum")
    conn.execute("analyze")
    conn.close()

if __name__ == '__main__':
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    failed = []

    for log in args.files:
        rendered_notebook = os.path.join(args.outdir, '%s-rendered.ipynb' % os.path.basename(log))
        stdoutfile = os.path.join(args.outdir, '%s.out' % os.path.basename(log))
        stderrfile = os.path.join(args.outdir, '%s.err' % os.path.basename(log))
        dbfile = os.path.join(args.outdir, '%s.db' % os.path.basename(log))

        print("processing %s --> %s " % (log, rendered_notebook))
        
        with open(stdoutfile, "w") as stdout:
            with open(stderrfile, "w") as stderr:
                the_db = args.per_app_db and dbfile or args.db
                try:
                    pm.execute_notebook(
                        'metrics.ipynb',
                        rendered_notebook,
                        parameters=dict(metrics_file=log, output_file=the_db, wide_output_file=args.db),
                        log_output=False,
                        stdout_file=stdout,
                        stderr_file=stderr,
                        request_save_on_cell_execute=True
                    )

                    if args.per_app_db:
                        print("optimizing per-app db...")
                        vacuum_analyze(the_db)
                except:
                    failed.append(log)
                    print("failed to process file %s" % log)
                    print(sys.exc_info()[0])
                    if args.fail_fast:
                        raise
    
    print("optimizing wide database...")
    vacuum_analyze(args.db)

    print("completed %d of %d logs successfully" % (len(args.files) - len(failed), len(args.files)))
    for failure in failed:
        print("failed to process file %s" % failure)                