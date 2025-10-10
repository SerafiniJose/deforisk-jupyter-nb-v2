from pathlib import Path
import os


def list_files_by_extension_os(folder_path, file_extensions):  # -> list:
    """
    List all files with specified extensions in the given folder.

    Parameters:
    folder_path (str): The path to the folder where you want to search for files.
    file_extensions (list of str): A list of file extensions to search for (e.g., ['.shp', '.tif']).

    Returns:
    list: A list of file paths with the specified extensions.
    """
    matching_files = []
    try:
        # Check if the provided path is a directory
        if os.path.isdir(folder_path):
            # Iterate over all files in the directory
            for filename in os.listdir(folder_path):
                # Construct full file path
                file_path = os.path.join(folder_path, filename)
                # Check if the file has any of the specified extensions
                if any(filename.lower().endswith(ext) for ext in file_extensions):
                    matching_files.append(file_path)
        else:
            print(f"The provided path '{folder_path}' is not a directory.")
    except Exception as e:
        print(f"An error occurred: {e}")
    return matching_files


def list_files_by_extension(folder_path, file_extensions, recursive=False):
    """
    List all files with specified extensions in the given folder.
    Parameters:
    folder_path (str or Path): The path to the folder where you want to search for files.
    file_extensions (list of str): A list of file extensions to search for (e.g., ['.shp', '.tif']).
    recursive (bool): Whether to recursively search through subdirectories or not.
    Returns:
    list: A list of file paths with the specified extensions.
    """
    matching_files = []
    try:
        # Convert folder_path to Path object if it's a string
        folder_path = Path(folder_path)

        # Check if the provided path is a directory
        if folder_path.is_dir():
            for entry in folder_path.iterdir():
                if entry.is_file() and any(
                    entry.suffix.lower() == ext.lower() for ext in file_extensions
                ):
                    matching_files.append(str(entry))
                elif recursive and entry.is_dir():
                    # Recursively search subdirectories
                    matching_files.extend(
                        list_files_by_extension(entry, file_extensions, recursive)
                    )
        else:
            print(f"The provided path '{folder_path}' is not a directory.")
    except Exception as e:
        print(f"An error occurred: {e}")
    return matching_files


def filter_file_os(input_files, filter_words, exclude_words=None):
    """
    Filters a list of files based on include and exclude words.

    Parameters:
        input_files (list): List of file paths to be filtered.
        filter_words (list): Words that must be present in the filenames for inclusion.
        exclude_words (list, optional): Words that must not be present in the filenames for exclusion. Defaults to None.

    Returns:
        list: Filtered list of files.
    """
    # Ensure all words are lowercase for case-insensitive comparison
    filter_words = [word.lower() for word in filter_words]
    exclude_words = [word.lower() for word in (exclude_words or [])]

    filtered_files = [
        file
        for file in input_files
        if all(word in os.path.basename(file).lower() for word in filter_words)
        and not any(
            exclude_word in os.path.basename(file).lower()
            for exclude_word in exclude_words
        )
    ]

    return filtered_files


from pathlib import Path


def filter_files(input_files, filter_words, exclude_words=None):
    """
    Filters a list of files based on include and exclude words.

    Parameters:
        input_files (list): List of file paths to be filtered.
        filter_words (list): Words that must be present in the filenames for inclusion.
        exclude_words (list, optional): Words that must not be present in the filenames for exclusion. Defaults to None.

    Returns:
        list: Filtered list of files.
    """
    # Ensure all words are lowercase for case-insensitive comparison
    filter_words = [word.lower() for word in filter_words]
    exclude_words = [word.lower() for word in (exclude_words or [])]

    filtered_files = [
        file
        for file in input_files
        if all(word in Path(file).name.lower() for word in filter_words)
        and not any(
            exclude_word in Path(file).name.lower() for exclude_word in exclude_words
        )
    ]

    return filtered_files
