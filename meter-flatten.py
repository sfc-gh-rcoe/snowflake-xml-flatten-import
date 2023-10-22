from snowflake.snowpark import Session
import snowflake.snowpark.files as files
from dotenv import load_dotenv
from snowflake.snowpark.functions import col
from collections import OrderedDict
from datetime import datetime
import xmltodict
import pandas as pd
import os
import sys



load_dotenv()
my_creds = {
	"account": os.environ["account_name"],
	"user": os.environ["account_user"],
	"password": os.environ["account_password"],
	"role": os.environ["account_role"],
	"schema": os.environ["account_schema"],
	"database": os.environ["account_database"],
	"warehouse": os.environ["account_warehouse"]
}
#
# Function from Stackoverflow for flattening an XML document
# 
# Attribution URL: https://stackoverflow.com/questions/38852822/how-to-flatten-xml-file-in-python
#
def flatten_dict(d):
	def items():
		for key, value in d.items():
			if isinstance(value, dict):
				for subkey, subvalue in flatten_dict(value).items():
					yield key + "." + subkey, subvalue
			else:
				yield key, value

	return OrderedDict(items())

    
def main(stage_name = sys.argv[1], input_file_name = sys.argv[2], output_table_name = sys.argv[3]): 

	#
	# Before running the following line, a determination must be made for how to populate the 'my_creds' dictionary with authentication credentials
	# Could be a JSON bundle, could use the dotenv module, or retreive creds from Azure Key Vault
	#
    session = Session.builder.configs(my_creds).create()


    the_time = datetime.now()
    the_date_suffix = the_time.strftime("%m%d%Y_%H_%M_%S")
	# stage_name = "@XMLTEST"
	# input_file_name = "books-sample.xml"
    output_table_name = output_table_name + "_" + the_date_suffix
    file_url = "{}".format(input_file_name)
    with open(file_url, 'r', encoding='utf-8') as f:
        xmlcontent_read = f.read()

    xmlcontent = xmltodict.parse(xmlcontent_read)

    flattended_xml = flatten_dict(xmlcontent)

    # Create a pandas dataframe representing an initial flattened version of the XML input---more work to do
    s = pd.DataFrame(flattended_xml)

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

    my_df4.show(10)
    
    result = session.sql("DROP TABLE {}".format(output_table_name)).collect()
    for x in range(len(result)):
        print("{}\n".format(result[x]))
    result = session.sql("DROP TABLE {}".format(output_table_name + "_flat")).collect()
    for x in range(len(result)):
        print("{}\n".format(result[x]))
main()
