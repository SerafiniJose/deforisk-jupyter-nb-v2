from pathlib import Path
from typing import List, Optional, Union


def list_files_by_extension(
    folder_path: str, file_extensions: Union[str, List[str]]
) -> List[Path]:
    """
    List all files with specified extensions in the given folder.

    Parameters:
    folder_path (str): The path to the folder where you want to search for files.
    file_extensions (str or list of str): A single file extension or list of file extensions to search for
        (e.g., '.shp', '.tif' or ['.shp', '.tif']).

    Returns:
    List[Path]: A list of Path objects for files with the specified extensions.

    Example:
        >>> list_files_by_extension('/path/to/folder', ['.shp', '.tif'])
        [PosixPath('/path/to/folder/data.shp'), PosixPath('/path/to/folder/imagery.tif')]
    """
    try:
        # Convert folder_path to Path object
        folder = Path(folder_path)

        # Check if the provided path is a directory
        if not folder.is_dir():
            raise NotADirectoryError(
                f"The provided path '{folder_path}' is not a directory."
            )

        # Normalize extensions to include the dot prefix
        if isinstance(file_extensions, str):
            file_extensions = [file_extensions]

        normalized_extensions = [
            ext if ext.startswith(".") else f".{ext}" for ext in file_extensions
        ]

        # Get all files in the directory and filter by extension
        matching_files = [
            file
            for file in folder.iterdir()
            if file.is_file()
            and file.suffix.lower() in [ext.lower() for ext in normalized_extensions]
        ]

        return matching_files

    except Exception as e:
        print(f"An error occurred: {e}")
        return []


def filter_files_by_extension(
    files: List[Path], extensions: Union[str, List[str]]
) -> List[Path]:
    """
    Filter files by their extensions.

    Args:
        files: List of file paths to filter
        extensions: Single extension string or list of extension strings (e.g., '.txt' or ['.txt', '.py'])

    Returns:
        List of Path objects that match the criteria
    """
    if isinstance(extensions, str):
        extensions = [extensions]

    # Normalize extensions to include the dot prefix
    normalized_extensions = [
        ext if ext.startswith(".") else f".{ext}" for ext in extensions
    ]

    # Match any of the extensions
    return [
        f
        for f in files
        if f.suffix.lower() in [ext.lower() for ext in normalized_extensions]
    ]


def filter_files_by_include_keywords(
    files: List[Path], keywords: List[str], match_any: bool = True
) -> List[Path]:
    """
    Filter files by including keywords in their names.

    Args:
        files: List of file paths to filter
        keywords: List of keywords that must be present in the filename
        match_any: If True, matches any keyword; if False, matches all keywords

    Returns:
        List of Path objects that match the criteria
    """
    # Normalize keywords to lowercase for case-insensitive matching
    normalized_keywords = [kw.lower() for kw in keywords]

    def matches_criteria(file_path: Path) -> bool:
        filename = file_path.name.lower()

        if match_any:
            # At least one keyword must be present
            return any(keyword in filename for keyword in normalized_keywords)
        else:
            # All keywords must be present
            return all(keyword in filename for keyword in normalized_keywords)

    return [f for f in files if matches_criteria(f)]


def filter_files_by_exclude_keywords(
    files: List[Path], keywords: List[str], match_any: bool = True
) -> List[Path]:
    """
    Filter files by excluding keywords from their names.

    Args:
        files: List of file paths to filter
        keywords: List of keywords that must NOT be present in the filename
        match_any: If True, excludes file if any keyword is present; if False, excludes file only if all keywords are present

    Returns:
        List of Path objects that match the criteria
    """
    # Normalize keywords to lowercase for case-insensitive matching
    normalized_keywords = [kw.lower() for kw in keywords]

    def matches_criteria(file_path: Path) -> bool:
        filename = file_path.name.lower()

        if match_any:
            # Exclude file if any keyword is present
            return not any(keyword in filename for keyword in normalized_keywords)
        else:
            # Exclude file only if all keywords are present
            return not all(keyword in filename for keyword in normalized_keywords)

    return [f for f in files if matches_criteria(f)]


def filter_files_by_keywords(
    files: List[Path],
    include_keywords: Optional[List[str]] = None,
    match_any_include: bool = True,
    exclude_keywords: Optional[List[str]] = None,
    match_any_exclude: bool = True,
) -> List[Path]:
    """
    Filter files by including and excluding keywords in their names.

    Args:
        files: List of file paths to filter
        include_keywords: List of keywords that must be present in the filename (optional)
        match_any_include: If True, matches any include keyword; if False, matches all include keywords
        exclude_keywords: List of keywords that must NOT be present in the filename (optional)
        match_any_exclude: If True, excludes file if any exclude keyword is present;
                           if False, excludes file only if all exclude keywords are present

    Returns:
        List of Path objects that match the criteria
    """
    # Normalize keywords to lowercase for case-insensitive matching
    normalized_include_keywords = (
        [kw.lower() for kw in include_keywords] if include_keywords else []
    )
    normalized_exclude_keywords = (
        [kw.lower() for kw in exclude_keywords] if exclude_keywords else []
    )

    def matches_criteria(file_path: Path) -> bool:
        filename = file_path.name.lower()

        # Check include criteria
        include_match = True
        if include_keywords:
            if match_any_include:
                # At least one include keyword must be present
                include_match = any(
                    keyword in filename for keyword in normalized_include_keywords
                )
            else:
                # All include keywords must be present
                include_match = all(
                    keyword in filename for keyword in normalized_include_keywords
                )

        # Check exclude criteria
        exclude_match = True
        if exclude_keywords:
            if match_any_exclude:
                # Exclude file if any exclude keyword is present
                exclude_match = not any(
                    keyword in filename for keyword in normalized_exclude_keywords
                )
            else:
                # Exclude file only if all exclude keywords are present
                exclude_match = not all(
                    keyword in filename for keyword in normalized_exclude_keywords
                )

        # Return True only if both include and exclude criteria are satisfied
        return include_match and exclude_match

    return [f for f in files if matches_criteria(f)]


def filter_folders_by_include_keywords(
    folders: List[Path], keywords: List[str], match_any: bool = True
) -> List[Path]:
    """
    Filter folders by including keywords in their names.

    Args:
        folders: List of folder paths to filter
        keywords: List of keywords that must be present in the folder name
        match_any: If True, matches any keyword; if False, matches all keywords

    Returns:
        List of Path objects that match the criteria
    """
    # Normalize keywords to lowercase for case-insensitive matching
    normalized_keywords = [kw.lower() for kw in keywords]

    def matches_criteria(folder_path: Path) -> bool:
        folder_name = folder_path.name.lower()

        if match_any:
            # At least one keyword must be present
            return any(keyword in folder_name for keyword in normalized_keywords)
        else:
            # All keywords must be present
            return all(keyword in folder_name for keyword in normalized_keywords)

    return [f for f in folders if matches_criteria(f)]


def filter_folders_by_exclude_keywords(
    folders: List[Path], keywords: List[str], match_any: bool = True
) -> List[Path]:
    """
    Filter folders by excluding keywords from their names.

    Args:
        folders: List of folder paths to filter
        keywords: List of keywords that must NOT be present in the folder name
        match_any: If True, excludes folder if any keyword is present; if False, excludes folder only if all keywords are present

    Returns:
        List of Path objects that match the criteria
    """
    # Normalize keywords to lowercase for case-insensitive matching
    normalized_keywords = [kw.lower() for kw in keywords]

    def matches_criteria(folder_path: Path) -> bool:
        folder_name = folder_path.name.lower()

        if match_any:
            # Exclude folder if any keyword is present
            return not any(keyword in folder_name for keyword in normalized_keywords)
        else:
            # Exclude folder only if all keywords are present
            return not all(keyword in folder_name for keyword in normalized_keywords)

    return [f for f in folders if matches_criteria(f)]


# Example usage:
if __name__ == "__main__":
    # Example of how to use the filtering functions with FolderManager
    from component.script.file_management import FolderManager

    # Create a FolderManager instance
    folder_manager = FolderManager("test_folder")

    # Get files from the folder
    files = folder_manager.list_files()

    # Filter files by extension
    txt_files = filter_files_by_extension(files, [".txt"])
    py_files = filter_files_by_extension(files, [".py", ".js"])

    # Filter files by include keywords
    important_files = filter_files_by_include_keywords(files, ["important"])
    exclude_temp_files = filter_files_by_exclude_keywords(files, ["temp"])

    # Combined filtering example - files that are .txt or .py AND contain "important"
    txt_or_py_files = filter_files_by_extension(files, [".txt", ".py"])
    important_txt_py_files = filter_files_by_include_keywords(
        txt_or_py_files, ["important"], match_any=True
    )
