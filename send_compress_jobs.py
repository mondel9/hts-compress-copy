import sys
import pandas as pd
from celery import group

from compress import optipng_files


def send_jobs(arglist):
    jobs = group(optipng_files.s(arglist.iloc[i].values[0], arglist.iloc[i].values[1]) \
                 for i in range(arglist.shape[0]))
    result = jobs.apply_async()

def main():
    arglist = pd.read_csv(sys.argv[1])
    send_jobs(arglist)

    

if __name__ == "__main__":
    main()
