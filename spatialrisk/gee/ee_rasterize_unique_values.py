import ee
from typing import Any


def gee_rasterize_unique_values(
    feature_collection: ee.FeatureCollection, property_name: str
) -> ee.Image:
    """
    Assign consecutive numbers to unique values of a specified column in a FeatureCollection.

    Args:
        feature_collection (ee.FeatureCollection): The input FeatureCollection.
        property_name (str): The name of the column containing unique values.

    Returns:
        ee.Image: An image where each pixel represents the consecutive number assigned to the unique values.
    """
    # Get the unique values of the specified column
    unique_values = feature_collection.aggregate_array(property_name).distinct()

    # Create a mapping from unique values to consecutive numbers
    value_to_number_map = ee.Dictionary.fromLists(
        unique_values, ee.List.sequence(1, unique_values.length())
    )

    # Function to assign the consecutive number to each feature
    def assign_number(feature: ee.Feature) -> ee.Feature:
        return feature.set(
            "consecutive_number", value_to_number_map.get(feature.get(property_name))
        )

    # Map the function over the FeatureCollection
    updated_features = feature_collection.map(assign_number)

    # Convert the updated FeatureCollection to an ee.Image
    image = updated_features.reduceToImage(["consecutive_number"], ee.Reducer.first())

    return image
