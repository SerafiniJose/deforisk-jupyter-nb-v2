import rasterio


def unmask_raster(input_file, output_file):
    """
    Unmasks pixel values in a raster file by setting existing no-data values to 0
    and updating the metadata to reflect that the new no-data value is 255.
    Parameters:
        input_file (str): Path to the input raster file.
        output_file (str): Path to the output raster file.
    """
    # Open the input raster file
    with rasterio.open(input_file) as src:
        # Read the raster data into a NumPy array
        data = src.read(1)  # Assuming single-band raster

        # Get the current no-data value from metadata
        nodata = src.nodata

        if nodata is not None:
            # Create a mask for existing no-data values
            no_data_mask = data == nodata
            # Set the no-data value to 0
            data[no_data_mask] = 0

        # Define the metadata for the output file
        meta = src.meta.copy()
        meta["nodata"] = 255  # Update the no-data value in metadata

    # Write the updated data and metadata to a new raster file
    with rasterio.open(output_file, "w", **meta) as dst:
        dst.write(data, 1)
