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
    input_file_name = "meter-readings.xml"
    output_table_name = "XML_TEST_OUT_" + the_date_suffix
    with files.SnowflakeFile.open("{}/{}".format(stage_name, input_file_name), 'rb', require_scoped_url = False) as f:
        xmlcontent = xmltodict.parse(f)
    
    flattened_xml = flatten_dict(xmlcontent)

    # Create a pandas dataframe representing an initial flattened version of the XML input---more work to do
    s = pd.DataFrame(flattened_xml)

    # As Snowflake reserves the "." as a separator between object names, let's rename the columns in the pandas dataframe
    s.rename(columns={"Channels.Channel": "CHANNELS_CHANNEL"}, inplace=True)

    # Create a Snowpark dataframe from the pandas dataframe
    my_df = session.createDataFrame(s)

    # Save the Snowpark dataframe to a Snowflake table
    my_df.write.mode("overwrite").save_as_table("{}".format(output_table_name))

    # Let's craft some SQL with an eye toward creating a table that is a flattened representation of the XML.  Up to now, the Snowflake tables use a single VARIANT datatype, which embeds JSON
    the_select_string = "SELECT {} FROM {}".format("CHANNELS_CHANNEL['@StartDate']::TIMESTAMP as READING_STARTDATE, CHANNELS_CHANNEL['@EndDate']::TIMESTAMP as READING_ENDDATE, CHANNELS_CHANNEL['@TimeZone']::VARCHAR as CH_TIMEZONE, CHANNELS_CHANNEL['ChannelID']['@ServicePointChannelID']::VARCHAR as SERVICEPOINTCHANNELID, CHANNELS_CHANNEL['ContiguousIntervalSets'] as contig_interval_sets, CHANNELS_CHANNEL['ContiguousIntervalSets']['ContiguousIntervalSet'] as contig_interval_set, CHANNELS_CHANNEL['ContiguousIntervalSets']['ContiguousIntervalSet']['Readings'], CHANNELS_CHANNEL['ContiguousIntervalSets']['ContiguousIntervalSet']['Readings']['Reading']", output_table_name)

    # my_df.show()
    my_df2 = session.sql(the_select_string)
    # my_df2 = my_df.select(col("CHANNELS_CHANNEL:['@StartDate']").as_("READING_STARTDATE"))
    # my_df2.show()

    # Save a table representing the flattend version of the XML imput
    my_df2.write.mode("overwrite").save_as_table("{}".format(output_table_name + "_flat"))

    # Now, we need to further flatten the VARIANT column that contains the channel readings.
    the_select_string_morph = "SELECT {} FROM {} a, LATERAL FLATTEN (input => a.contig_interval_set) b, LATERAL FLATTEN (input => a.\"{}\") p, LATERAL FLATTEN (input => p.value) q WHERE the_measure < 2 and mon_start is not null".format("reading_startdate, reading_enddate, ch_timezone, servicepointchannelid, contig_interval_sets, contig_interval_set, q.value::float as the_measure, b.value['@StartTime']::TIMESTAMP as MON_START, b.value['@EndTime']::TIMESTAMP as MON_END", output_table_name + "_flat", "CHANNELS_CHANNEL['CONTIGUOUSINTERVALSETS']['CONTIGUOUSINTERVALSET']['READINGS']['READING']")

    my_df3 = session.sql(the_select_string_morph)
    # my_df3 = my_df2.select(col("READING_STARTDATE"), col("READING_ENDDATE"), col("CH_TIMEZONE"), col("SERVICEPOINTCHANNELID"), col("contig_interval_sets"), col("contig_interval_set"))

    # Save a GOLD version of the table, which will have all pertinent data elements represented by individual columns
    my_df3.write.mode("overwrite").save_as_table("{}".format(output_table_name + "_stage"))

    the_select_string_stage = "SELECT {} FROM {}".format("ch_timezone, servicepointchannelid, the_measure, mon_start, mon_end", output_table_name + "_stage")

    my_df4 = session.sql(the_select_string_stage)

    result = session.sql("DROP TABLE {}".format(output_table_name)).collect()
    for x in range(len(result)):
        print("{}\n".format(result[x]))
    result = session.sql("DROP TABLE {}".format(output_table_name + "_flat")).collect()
    for x in range(len(result)):
        print("{}\n".format(result[x]))

    return my_df4

