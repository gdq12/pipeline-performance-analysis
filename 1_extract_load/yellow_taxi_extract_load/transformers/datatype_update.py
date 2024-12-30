import re
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
        df: pandas df with the data types that are specified in utils.helper files
    """
    kwarg_logger = kwargs.get('logger')

    # create new column for partitioning in next step 
    pickup_col = data.columns[data.columns.str.contains('pickup', regex = True)][0]
    col_name = re.sub('time', '', pickup_col)
    data[col_name] = data[pickup_col].dt.date
    
    # convert everything to string for better loading to BigQuery tables 
    df = data.astype(str)

    kwarg_logger.info(f"env memory stats: {psutil.virtual_memory().total} (total memory), {psutil.virtual_memory().available} (available), {psutil.virtual_memory().percent} (percent), {psutil.virtual_memory().used} (used), {psutil.virtual_memory().free} (free)")
    kwarg_logger.info("updates data types as all string")

    return df