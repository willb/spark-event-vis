import papermill as pm
import argparse
import os
import re

parser = parser = argparse.ArgumentParser()
parser.add_argument('--master', help='Spark master URL (default: "local[*]")', default="local[*]")
parser.add_argument('--outdir', help='where to store postprocessed notebooks (default: "outputs")', default="outputs")
parser.add_argument('--db', help='database file to store/append postprocessed events to (default="output.db")', default="output.db")
parser.add_argument('--config', metavar="KEY=VAL", help="add KEY=VAL to Spark's configuration", action='append', default=[], dest='config')
parser.add_argument('files', metavar="FILE", nargs="+")

if __name__ == '__main__':
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    for log in args.files:
        rendered_notebook = os.path.join(args.outdir, '%s-rendered.ipynb' % os.path.basename(log))
        stdoutfile = os.path.join(args.outdir, '%s.out' % os.path.basename(log))
        stderrfile = os.path.join(args.outdir, '%s.err' % os.path.basename(log))

        print("processing %s --> %s " % (log, rendered_notebook))
        
        with open(stdoutfile, "w") as stdout:
            with open(stderrfile, "w") as stderr:
                pm.execute_notebook(
                    'metrics.ipynb',
                    rendered_notebook,
                    parameters=dict(metrics_file=log, output_file=args.db),
                    log_output=False,
                    stdout_file=stdout,
                    stderr_file=stderr,
                    request_save_on_cell_execute=True
                )