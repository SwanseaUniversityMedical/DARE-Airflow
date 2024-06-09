import re


def escape_column(column):
    return re.sub(r'[^a-zA-Z0-9]+', '_', column).strip("_")


def validate_column(column):
    assert column == escape_column(column)
    return column


def escape_dataset(dataset):
    # TODO make this more sensible
    return escape_column(dataset)


def validate_identifier(identifier):
    # Validate the identifier is strictly one or more dot separated identifiers
    assert re.match(
        r'^(?:[a-z](?:[_a-z0-9]*[a-z0-9]|[a-z0-9]*))'
        r'(?:\.[a-z0-9](?:[_a-z0-9]*[a-z0-9]|[a-z0-9]*))*$',
        identifier,
        flags=re.IGNORECASE
    )

    return identifier
