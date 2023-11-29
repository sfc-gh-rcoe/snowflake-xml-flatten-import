from snowflake.snowpark import Session
from snowflake.snowpark.types import StructType, StringType, TimestampType, FloatType, StructField, VariantType
import snowflake.snowpark.files as files
import snowflake.snowpark.functions as F
from dotenv import load_dotenv
from collections import OrderedDict
from datetime import datetime
import xmltodict
import pandas as pd
import os
import sys
import streamlit as st
from io import StringIO



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

    
def write_xml_to_table(input_file_name): 

	#
	# Before running the following line, a determination must be made for how to populate the 'my_creds' dictionary with authentication credentials
	# Could be a JSON bundle, could use the dotenv module, or retreive creds from Azure Key Vault
	#
    session = Session.builder.configs(my_creds).create()


    the_time = datetime.now()
    the_date_suffix = the_time.strftime("%m%d%Y_%H_%M_%S")
	# stage_name = "@XMLTEST"
	# input_file_name = "books-sample.xml"
    output_table_name = "METER_READINGS"
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
    # my_df.write.mode("overwrite").save_as_table("{}".format(output_table_name))

    # Let's craft some SQL with an eye toward creating a table that is a flattened representation of the XML.  Up to now, the Snowflake tables use a single VARIANT datatype, which embeds JSON

    my_df2 = my_df.select(F.col("CHANNELS_CHANNEL")["@StartDate"].as_("READING_STARTDATE"),\
            F.col("CHANNELS_CHANNEL")["@EndDate"].as_("READING_ENDDATE"),\
            F.col("CHANNELS_CHANNEL")["@TimeZone"].as_("CH_TIMEZONE"),\
            F.col("CHANNELS_CHANNEL")["ChannelID"]["@ServicePointChannelID"].as_("SERVICEPOINTCHANNELID"),\
            F.col("CHANNELS_CHANNEL")["ContiguousIntervalSets"].as_("contig_interval_sets"),\
            F.col("CHANNELS_CHANNEL")["ContiguousIntervalSets"]["ContiguousIntervalSet"].as_("contig_interval_set"),\
            F.col("CHANNELS_CHANNEL")["ContiguousIntervalSets"]["ContiguousIntervalSet"]["Readings"].as_("readings"),\
            F.col("CHANNELS_CHANNEL")["ContiguousIntervalSets"]["ContiguousIntervalSet"]["Readings"]["Reading"].as_("reading")
            )
    # my_df2.show()

    # Save a table representing the flattend version of the XML imput
    # my_df2.write.mode("overwrite").save_as_table("{}".format(output_table_name + "_flat"))
   
    # Now, we need to further flatten the VARIANT column that contains the channel readings.

    my_df3 = my_df2.select(F.col("READING_STARTDATE").cast(TimestampType()).as_("READING_STARTDATE"),\
           F.col("READING_ENDDATE").cast(TimestampType()).as_("READING_ENDDATE"),\
            F.col("CH_TIMEZONE").cast(StringType()).as_("CH_TIMEZONE"),\
            F.col("SERVICEPOINTCHANNELID").cast(StringType()).as_("SERVICEPOINTCHANNELID"),\
            F.col("contig_interval_sets").cast(VariantType()).as_("contig_interval_sets"),\
            F.col("contig_interval_set").cast(VariantType()).as_("contig_interval_set"),\
            F.col("readings").cast(VariantType()).as_("readings"),\
            F.col("reading").cast(VariantType()).as_("reading"),\
            F.col("reading")[1]["@VALUE"].cast(StringType()).as_("M_VALUE"))

    split_to_table = F.table_function("split_to_table")
    ## my_df3 = my_df3.join_table_function(split_to_table(F.col("reading")[1]["@VALUE"], F.lit(" ")))
    my_df3 = my_df3.join_table_function("flatten", my_df3["reading"]).drop(["SEQ", "PATH", "INDEX", "THIS"])
    my_df4 = my_df3.select(F.col("READING_STARTDATE"),\
            F.col("READING_ENDDATE"),\
            F.col("CH_TIMEZONE"),\
            F.col("SERVICEPOINTCHANNELID"),\
            # F.col("contig_interval_set")["TimePeriod"]["@StartTime"].cast(TimestampType()).as_("MON_START"),\
            F.col("contig_interval_set")["TimePerod"]["@StartTime"].cast(TimestampType()).as_("MON_START"),\
            # F.col("contig_interval_set")["TimePeriod"]["@EndTime"].cast(TimestampType()).as_("MON_END"),\
            F.col("contig_interval_set")["TimePerod"]["@EndTime"].cast(TimestampType()).as_("MON_END"),\
            F.col("VALUE")["@VALUE"].cast(FloatType()).as_("THE_MEASURE")).filter(F.col("READING_STARTDATE").isNotNull())
    # Save a GOLD version of the table, which will have all pertinent data elements represented by individual columns
    # my_df3.write.mode("overwrite").save_as_table("{}".format(output_table_name + "_stage"))


    # my_df4.write.mode("overwrite").save_as_table("{}".format(output_table_name + "_stage"))
    my_df5 = my_df4.write.mode("overwrite").save_as_table(table_name=output_table_name, table_type='transient')
    return output_table_name

st.title("###XML Importer")


m_session1 = Session.builder.configs(my_creds).create()

record_count = 0

uploaded_files = st.file_uploader(label = "Select the file(s) against which to match", accept_multiple_files=True, type = ['xml'])
t_df = m_session1.create_dataframe(data=[[1,2]], schema=['a', 'b'])
if ((uploaded_files)):
    l_df = m_session1.create_dataframe(data=[[1,2]], schema=['a', 'b'])
    for t_file in uploaded_files:
        if t_file is not None:
            amt_of_data = t_file.getvalue()
            t_name = write_xml_to_table(t_file.name)
            t_df = m_session1.table(t_name)
    st.dataframe(t_df.limit(25).toPandas())







