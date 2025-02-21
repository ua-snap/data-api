from flask import render_template
from luts import nodata_mappings


def nullify_nodata_value(value, endpoint):
    """Return None if a nodata value is detected, otherwise return the original value.

    Args:
        value: Original value
    Returns:
        value: The original value or None if a nodata value was detected
    """
    if str(value) in map(str, nodata_mappings[endpoint]):
        return None
    return value


def nullify_nodata(data, endpoint):
    """Traverse data dict recursively to convert nodata values to None.

    Args:
        data (dict): Results dict
    Returns:
        nullified (dict): The same results dict with nodata values set to None
    """
    if isinstance(data, list):
        return list(map(lambda x: nullify_nodata(x, endpoint), data))
    if isinstance(data, tuple):
        return tuple(map(lambda x: nullify_nodata(x, endpoint), data))
    if isinstance(data, dict):
        return dict(map(lambda x: nullify_nodata(x, endpoint), data.items()))

    nullified = nullify_nodata_value(data, endpoint)
    return nullified


def prune_nodata_dict(data):
    """Traverse dict recursively and prune empty or None branches.

    Args:
        data (dict): Dict with nodata values set to None
    Returns:
        pruned (dict): The same dict with empty and None branches pruned
    """
    pruned = {}
    for key, value in data.items():
        pruned[key] = prune_nodata(value)

    if any(value is not None for value in pruned.values()):
        return pruned

    return None


def prune_nodata_list(data):
    """Traverse list recursively and prune empty or None branches.

    Args:
        data (list): List with nodata values set to None
    Returns:
        pruned (list): The same list with empty and None branches pruned
    """
    pruned = []
    for value in data:
        if type(value) in [list, dict, tuple]:
            pruned_value = prune_nodata(value)
            if len(pruned_value) > 0:
                pruned.append(pruned_value)

    return pruned


def prune_nulls_with_max_intensity(data):
    """
    Recursively remove all None values from dicts and remove any empty dicts that remain.

    In practice, this will trim keys with `null` values from the API response even in the case when a sibling key does have data.
    """
    if isinstance(data, dict):
        return {
            k: v
            for k, v in (
                (k, prune_nulls_with_max_intensity(v)) for k, v in data.items()
            )
            if v is not None
        }
    else:
        return data


def prune_nodata(data):
    """Traverse data structure recursively and prune empty or None branches.

    Args:
        data (dict, list): Data structure with nodata values set to None
    Returns:
        (dict): The same data with empty and None branches pruned
    """
    if isinstance(data, dict):
        return prune_nodata_dict(data)

    if isinstance(data, list):
        return prune_nodata_list(data)

    return data


def nullify_and_prune(data, endpoint):
    """Filter nodata values, prune empty branches, and return data"""
    nullified_data = nullify_nodata(data, endpoint)
    pruned_data = prune_nodata(nullified_data)
    return pruned_data


def postprocess(data, endpoint, titles=None):
    """Nullify and prune data, add titles, and return 404 if appropriate"""
    pruned_data = nullify_and_prune(data, endpoint)
    if pruned_data in [{}, None, 0]:
        return render_template("404/no_data.html"), 404

    if titles is not None:
        pruned_data = add_titles(pruned_data, titles)

    return pruned_data


def recursive_rounding(keys, values):
    to_return = {}
    for key, value in zip(keys, values):
        if isinstance(value, dict):
            rounded_value = recursive_rounding(value.keys(), value.values())
        elif isinstance(value, (tuple, list)):
            rounded_value = [round_by_type(x) for x in value]
        else:
            rounded_value = round_by_type(value)
        to_return[round_by_type(key)] = rounded_value
    return to_return


def round_by_type(to_round, round_amount=7):
    if isinstance(to_round, (int, float)):
        return round(to_round, round_amount)
    elif isinstance(to_round, (list, tuple)):
        return [round_by_type(x) for x in to_round]
    return to_round


def add_titles(packaged_data, titles):
    """
    Adds title fields to a JSONlike data package and returns it.
    Args:
        packaged_data (json): JSONlike data package output
            from the run_fetch_* and run_aggregate_* functions
        titles (list, str): title or list of titles to add to the data package

    Returns:
        data package with titles added
    """
    if titles is not None:
        if isinstance(titles, str):
            packaged_data["title"] = titles
        else:
            for key in titles.keys():
                if key in packaged_data:
                    if packaged_data[key] is not None:
                        packaged_data[key]["title"] = titles[key]
    return packaged_data
