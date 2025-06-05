# buncha shit
import multiprocessing as mp
import osgeo.gdal as gdal
import numpy as np
from tqdm import tqdm
from functools import partial
import argparse

# idk what this does
gdal.DontUseExceptions()
    
# calculate volume for a pixel and return a set containing the volume and elevation
def process_pixel(pixel_elevation, pixel_width, floor):

    # -9999.0 is a placeholder for empty data; so, ignore
    if float(pixel_elevation) == -9999.0:
        return (0, float('inf'))  # ignore no-data pixels
    
    # calculate volume for pixel if it's elevation is higher than the specified floor
    if float(pixel_elevation) > float(floor):
        height_from_floor = float(pixel_elevation) - float(floor) # calculate height
        pixel_volume = height_from_floor * pixel_width * pixel_width # calculate volume
    
    # set volume to zero if below the floor so it doesn't get added to the total
    else:
        pixel_volume = 0

    return (float(pixel_volume), float(pixel_elevation))

# wrapper function to allow multiple arguments for pool.map
# it takes smaller chunks of the full image pixel list; still lists
def wrapper(section, pixel_width, floor):
    return [process_pixel(pixel, pixel_width, floor) for pixel in section]

# run all the logic for pixel volume processing with multiprocessing 
def run_map(file, floor, threads):
    
    # set correct variable types
    file = str(file)
    floor = float(floor)
    threads = int(threads)

    # set max cache size per process to ~536 MB
    gdal.SetCacheMax(512 * 1024 * 1024) 

    # open the dem file, store elevation data, store pixel width, and close file
    ds = gdal.OpenEx(file)
    all_elevation_data = ds.ReadAsArray()
    gt = ds.GetGeoTransform()
    pixel_width = abs(gt[1])
    ds = None

    # flatten data (NumPy function) for easier parsing; filter out unneeded pixels (crucial to make multiprocessing work, apparently) 
    flat_data = all_elevation_data.flatten()
    filtered_data = flat_data[(flat_data != -9999.0) & (flat_data > floor)]

    # split filtered data into a 2D array with smaller lists based on the threads requested
    sections = np.array_split(filtered_data, threads * 4)

    # create 'function' to use in pool.map to bypass inability of accepting multiple arguments
    wrapped = partial(wrapper, pixel_width=pixel_width, floor=floor)

    # set placeholders for total volume and minimum point elevation
    total_volume = 0
    minimum_point = float('inf')

    # create seperate processes for each call of process_pixel() using specified thread amount to calculate total volume and minimum elevation
    # imap_unordered() allows for faster processing since pixel order doesn't matter
    # create progress bar with tqdm; total is equal to threads * 4
    with mp.Pool(threads) as pool:
        for result_section in tqdm(pool.imap_unordered(wrapped, sections), total=len(sections)):
            for volume, elevation in result_section:
                total_volume += volume
                if elevation < minimum_point:
                    minimum_point = elevation


    # show all of our hard work    
    print(f"Minimum Point: {minimum_point}")
    print(f"Total Volume: {total_volume}")

# thing that does the thing
if __name__ == '__main__':

    # logic for accepting command line arguments
    parser = argparse.ArgumentParser(description="DEM Volume Calculation")
    parser.add_argument('-d', '--dem', help='Enter a path to a RAW DEM file. Should generally be a DSM and not a DTM.', required=True, dest="dem")
    parser.add_argument('-f', '--floor', help='Enter a value (in meters) to be used as a floor for volume calculation.', required=True, dest="floor")
    parser.add_argument('-t', '--threads', help='Enter the number of threads to use in parallel processing.', required=True, dest="threads")
    args = parser.parse_args()

    # run function with given arguments
    run_map(args.dem, args.floor, args.threads)
