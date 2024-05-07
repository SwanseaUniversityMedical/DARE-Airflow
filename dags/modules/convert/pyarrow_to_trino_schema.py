import logging


def pyarrow_to_trino_schema(schema):
    trino_schema = []

    for field in schema:
        # Extract field name and data type
        field_name = field.name
        field_type = str(field.logical_type)
        logging.info(f"pyarrow schema: {field_name} is {field_type} ({field.physical_type})")

        # Convert PyArrow data type to Trino-compatible data type
        if field_type == 'int64':
            trino_type = 'BIGINT'
        elif field_type.startswith('Int'):
            if field.physical_type == 'INT64':
                trino_type = 'BIGINT'
            else:
                trino_type = 'INTEGER'
        elif field_type.startswith('Float'):
            trino_type = 'DOUBLE'
        elif field_type.startswith('String'):
            trino_type = 'VARCHAR'
        elif field_type == 'Object':
            trino_type = 'VARCHAR'
        elif field_type.startswith('Bool'):
            trino_type = 'BOOLEAN'
        elif field_type.startswith('Timestamp'):
            trino_type = 'TIMESTAMP'
        elif field_type.startswith('Date'):
            trino_type = 'DATE'
        elif field_type == 'None':
            trino_type = field.physical_type
        else:
            # Use VARCHAR as default for unsupported types
            trino_type = 'VARCHAR'

        # Append field definition to Trino schema
        trino_schema.append(f'{field_name} {trino_type}')

    return ',\n'.join(trino_schema)