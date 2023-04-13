"""Compress and Copy HTS Experiment Data

This script allows the user to compress HTS tiff files using OptiPNG and
saves the compressed data to a storage location. Celery is used to
run compression tasks in parallel.

This tool accepts comma separated value files (.csv).

This script requires that `celery` be installed within the Python
environment you are running this script in.

This file can also be imported as a module and contains the following
functions:

    * optipng_files - runs optipng compression command on input files
    * main - the main function of the script
    
"""
import os
import argparse
from celery import Celery
from dotenv import load_dotenv


load_dotenv()

BROKER = os.getenv('BROKER')
app = Celery('compress', broker=BROKER)


def check_for_files(directory):
    """Return any filenames in directory."""
    if os.path.exists(directory):
        if len(os.listdir((directory))) > 0:
            return(os.listdir((directory)))

@app.task
def optipng_files(source_dir, target_dir, overwrite=False):
    """Run OptiPNG compression on images.
    
    TODO: include behavior for overwrite=True.
    
    Args:
        source_dir: directory containing all plate files for a dataset
        target_dir: directory where files should be transeferred to
    """
    # get full path for each file
    source_files = [ os.path.join(source_dir, f) for f in os.listdir(source_dir) \
                    if os.path.isfile(os.path.join(source_dir, f))]

    # if target does not exist, create it
    if not os.path.exists(target_dir):
        os.mkdir(target_dir)
    
    for file_path in source_files:
        file_name = os.path.basename(file_path)
        
        if not os.path.isfile(os.path.join(target_dir, file_name.replace('.tif','.png'))) and not overwrite:
            # check extension and exclude Thumbnail images
            if file_name[-4:] == '.tif' and file_name[0:5] != 'Thumb':
                docall = f'optipng -zs=2 -f0 -nx -nz -quiet {file_path} -dir {target_dir}'
            else:
                 docall = f'cp {file_path} {target_dir}/.'
            
            os.system(docall)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("source", help="source directory path")
    parser.add_argument("target", help="target directory path")
    parser.add_argument("-o","--overwrite", help="overwrite if file found in target dir", 
                        dest='overwrite', action='store_true', default=False)
    args = parser.parse_args()

    source_dir = args.source
    target_dir = args.target
    overwrite = args.overwrite
    
    optipng_files(source_dir, target_dir, overwrite)
    
    
    
if __name__ == '__main__':
    main()