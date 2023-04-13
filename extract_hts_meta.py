"""Extract and Compress Tiff Metadata.

This script allows the user to extract metdata from tiff files using PIL
with the option of running a compression script to compress and
copy the data for storage.

This script requires that `PIL` and `pandas` be installed within the Python
environment you are running this script in.

This file can also be imported as a module and contains the following
functions:

    * optipng_files - runs optipng compression command on input files
    * main - the main function of the script
    
An example of command-line usage is:
python extract_hts_meta.py -s /source/directory -t /target/directory -c
    
"""
import os
import argparse
import json
import time
import datetime
import pandas as pd
from PIL import Image, TiffTags

from send_compress_jobs import send_jobs


def get_dataset_metadata(image):
    """Extract metadata tags common for all images.
    
    Args:
        image: str or path-like object to one tiff image in dataset.
    
    Returns:
        `dataset_meta` struct mapping keys to extracted metadata.
    """
    dataset_meta = {}  

    meta_keys = ['ImageWidth', 'ImageLength', 'BitsPerSample', 'Compression',
            'PhotometricInterpretation', 'FillOrder', 'XResolution',
            'YResolution', 'PlanarConfiguration', 'ResolutionUnit',
            'Software', 'Predictor', 'NewSubfileType', 'SamplesPerPixel',
            'RowsPerStrip', 'StripByteCounts']
    
    with Image.open(image) as img:      
        for key in img.tag.keys():
            tag = TiffTags.lookup(key)
            if tag.name in meta_keys:
                 dataset_meta[TiffTags.TAGS[key]] = img.tag[key]
                    
    return dataset_meta

def get_img_metadata(images):
    """Extract metadat unique to each tiff image.
    
    Args:
        images: list of str or path-like objects for each image in a plate.
        
    Returns:
        `all_images` struct mapping keys to extracted metadata for every 
        image in a plate.
    """
    all_images = {}
    for img_path in images:
        with Image.open(img_path) as img:
            metadata = {}       
            for key in img.tag.keys():
                tag = TiffTags.lookup(key)
                if tag.name == 'unknown':
                    metadata[tag.name] = img.tag[key]
                elif tag.name == 'ImageDescription':
                    # parse into clean format
                    desc = img.tag[key]
                    cleaned = desc[0].split('\r\n')[0:-1]
                    metadata[TiffTags.TAGS[key]] = cleaned
                elif tag.name == 'DateTime':
                    metadata[TiffTags.TAGS[key]] = img.tag[key]
        all_images[os.path.basename(img_path)] = metadata
        
    return all_images

def save_metadata(source_dir, target_dir):
    """Extract and save metadata for an HTS dataset.
    
    Saves generated ouput files to given location.
    
    Args:
        source_dir: root directory of dataset to extract metadata from.
        target_dir: directory to save metdata and compressed images.
    
    Returns:
        Task arguments dataframe containing source and target directories
        to pass to `send_jobs` if `compress` is `True`.
    """
    dataset = os.path.basename(source_dir)
    data = {}
    data['DatasetName'] = dataset
    img_path = os.path.join(source_dir, 'images') 
    plate_dirs = os.listdir(img_path)
    
    source = []
    target = []
    i = 0
    print('Extracting metadata...')

    for plate in plate_dirs:
        if plate == '.DS_Store': 
            continue
        else:
            source.append(os.path.join(img_path, plate))
            target.append(os.path.join(target_dir, plate))
            
            images = os.listdir(os.path.join(img_path, plate))
            file_names = [os.path.join(img_path, plate, f) for f in images if ".tif" in f]
            
            if i == 0:
                # get one instance of constant data for entire dataset
                data['DatasetInfo'] = get_dataset_metadata(file_names[0])
            
            data[plate] = get_img_metadata(file_names)
            i += 1
    
    # save task arguments for compressAndCopy
    taskargs = pd.DataFrame({'source_dir': source,
                             'target_dir': target})
    
    print('Saving metadata..')
    # ta_file = f'{target_dir}/{dataset}_compress_copy.csv'
    taskargs.to_csv(f'{target_dir}/{dataset}_compress_copy.csv', index=False)
    
    with open(f"{target_dir}/{dataset}_metadata.json", "w") as outfile:
        json.dump(data, outfile)
        
    return taskargs

def main():
    argParser = argparse.ArgumentParser()
    argParser.add_argument("-s", "--source", help="root directory of dataset to extract metadata from")
    argParser.add_argument("-t", "--target", help="directory to save metdata and compressed images")
    argParser.add_argument("-c", "--compress", default=False, action="store_true")

    args = argParser.parse_args()

    arglist = save_metadata(args.source, args.target)
    
    if args.compress:
        print('Starting compress and copy...')
        send_jobs(arglist)


if __name__ == "__main__":
    main()
        
