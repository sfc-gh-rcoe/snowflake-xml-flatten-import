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
    with open(file_url, 'rb') as f:
        xmlcontent = xmltodict.parse(f)

    flattended_xml = flatten_dict(xmlcontent)

    s = pd.DataFrame(flattended_xml)
    s.rename(columns={"portfolio.stock": "PORTFOLIO_STOCK", "portfolio.@xmlns:dt": "PORTFOLIO_NAMESPACE"}, inplace=True)
    my_df = session.createDataFrame(s)

    my_df.write.mode("overwrite").save_as_table("{}".format(output_table_name))

    # An additional spin on the SQL here as the column name for \/ id has a special character.  Note how we handle
    the_select_string = "SELECT {} FROM {}".format("PORTFOLIO_STOCK['@exchange']::VARCHAR as EXCHANGE, PORTFOLIO_STOCK:name::VARCHAR as NAME, PORTFOLIO_STOCK:price['#text']::FLOAT as PRICE", output_table_name)

    my_df2 = session.sql(the_select_string)

    my_df2.write.mode("overwrite").save_as_table("{}".format(output_table_name + "_flat"))

    my_df2.show(12)


main()
