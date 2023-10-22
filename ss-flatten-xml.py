# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
import snowflake.snowpark.files as files
from snowflake.snowpark.functions import col
from collections import OrderedDict
from datetime import datetime
import xmltodict
import pandas as pd

# Function used to flatten the XML
# Attribution URL: https://stackoverflow.com/questions/38852822/how-to-flatten-xml-file-in-python
def flatten_dict(d):
    def items():
        for key, value in d.items():
            if isinstance(value, dict):
                for subkey, subvalue in flatten_dict(value).items():
                    yield key + "." + subkey, subvalue
            else:
                yield key, value

    return OrderedDict(items())

    
def main(session: snowpark.Session): 

    the_time = datetime.now()
    the_date_suffix = the_time.strftime("%m%d%Y_%H_%M_%S")
    stage_name = "@XMLTEST"
    input_file_name = "books-sample.xml"
    output_table_name = "XML_TEST_OUT_" + the_date_suffix
    with files.SnowflakeFile.open("{}/{}".format(stage_name, input_file_name), 'rb', require_scoped_url = False) as f:
        xmlcontent = xmltodict.parse(f)
    
    flattened_xml = flatten_dict(xmlcontent)

    #
    # Create a pandas dataframe from the dictionary
    s = pd.DataFrame(flattened_xml)

    # Rename the column because Snowflake reserves the '.' as a seperator between objects
    s.rename(columns={"catalog.book": "CATALOG_BOOK"}, inplace=True)

    # Create a Snowpark dataframe as we'll use this to create a Snowflake table
    my_df = session.createDataFrame(s)

    # Create a Snowflake table in order to persist the RAW data
    my_df.write.mode("overwrite").save_as_table("{}".format(output_table_name))

    # Query the newly created table, creating another Snowpark dataframe consisting of the fields with which we want to work
    my_df2 = session.sql("SELECT {} FROM {}".format("CATALOG_BOOK:author as AUTHOR, CATALOG_BOOK:description AS DESCRIPTION", output_table_name))

    session.sql("DROP TABLE {}".format(output_table_name))
    
    return my_df2
