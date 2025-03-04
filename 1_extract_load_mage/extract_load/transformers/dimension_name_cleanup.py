import psutil
import pandas as pd
from datetime import datetime
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        df: pandas df with correct column names 
    """
    kwarg_logger = kwargs.get('logger')

    # change all col names to snake syntax
    kwarg_logger.info(f"need to change the following columns to snake case: {', '.join(data.columns[data.columns.str.contains('(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z]{1}[a-z])', regex = True)])}")

    data.columns = data.columns\
                .str.replace('(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z]{1}[a-z])', '_', regex=True)\
                .str.lower()

    kwarg_logger.info(f"env memory stats: {psutil.virtual_memory().total} (total memory), {psutil.virtual_memory().available} (available), {psutil.virtual_memory().percent} (percent), {psutil.virtual_memory().used} (used), {psutil.virtual_memory().free} (free)")

    kwarg_logger.info(f"exporting a df with {data.shape[0]} records, with the following dimension names: {', '.join(data.columns)}")
    
    return data

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

@test 
def test_camel_col_name(output, *args) -> None:
    assert len(output.columns[output.columns.str.contains('(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z]{1}[a-z])', regex = True)]) == 0, 'The column names are not in snake format'