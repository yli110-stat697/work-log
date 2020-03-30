import pandas as pd 
import pymysql as MySQLdb
import os

connection_dict = {"QMPX_HOST": 'qmpx-prd-meta1.sf.quinstreet.net',
				   "QMPX_PORT": 3307,
				   "CCMP_HOST": 'qmpx-ccmp-prd1.sf.quinstreet.net',
				   "CCMP_PORT": 3301}

user_choice = input("Please enter the database: QMPX or CCMP?")
HOST = user_choice + '_HOST'
PORT = user_choice + '_PORT'

conn = MySQLdb.connect(host=connection_dict.get(HOST),
							   port=connection_dict.get(PORT),
							   user=os.getenv('DB_USER'),
							   password=os.getenv('DB_PSWD'))

client_list = """
SELECT client_code, name
FROM meta.client;
"""

client = pd.read_sql(client_list, conn)
print("Check the client list below:")
print(client)
client_choice = tuple(input("Enter your client_code, separated by comma:").split(","))
start_date = input("Enter the beginning date of your report in the format of yyyy-mm-dd:")
start_date = "'"+start_date+"'"
end_date = input("Enter the endding date of your report in the format of yyyy-mm-dd:")
end_date = "'"+end_date+"'"

if len(client_choice) == 1:
	sql_query = """
	SELECT lc.leadcapture_key, lc.email_address, lc.first_name, lc.last_name, lc.client_code, lc.campaign_code, 
		{conditional_field} lc.aggregator_code, lc.aggregator_lead_key, json_extract(lc.lead_info, '$.otherAttributes.leadIDToken') as leadid_token,
		lc.createddate, ls.name as lead_status, isr.name as inquiry_status_reason,
        json_extract(lc.debug_info, '$.validationDetails') as validation_detailes
	FROM ods.leadcapture lc
	LEFT JOIN meta.inquiry_status_reason isr ON lc.key2inquiry_status_reason = isr.inquiry_status_reason_key
	LEFT JOIN meta.lead_status ls ON lc.key2lead_status = ls.lead_status_key
	WHERE client_code = '{client}' AND lc.createddate BETWEEN {start} AND {end}
	ORDER BY createddate;
	""".format(conditional_field=["","json_extract(lc.delivered_lead_info, '$.src1') as src1,"]['BRKLY' in client_choice[0]], client=client_choice[0], 
		   start=start_date, end=end_date)
else:
	sql_query = """
	SELECT lc.leadcapture_key, lc.email_address, lc.first_name, lc.last_name, lc.client_code, lc.campaign_code, 
			{conditional_field} lc.aggregator_code, lc.aggregator_lead_key, json_extract(lc.lead_info, '$.otherAttributes.leadIDToken') as leadid_token,
			lc.createddate, ls.name as lead_status, isr.name as inquiry_status_reason,
	        json_extract(lc.debug_info, '$.validationDetails') as validation_detailes
	FROM ods.leadcapture lc
	LEFT JOIN meta.inquiry_status_reason isr ON lc.key2inquiry_status_reason = isr.inquiry_status_reason_key
	LEFT JOIN meta.lead_status ls ON lc.key2lead_status = ls.lead_status_key
	WHERE client_code in {client} AND lc.createddate BETWEEN {start} AND {end}
	ORDER BY createddate;
	""".format(conditional_field=["","json_extract(lc.delivered_lead_info, '$.src1') as src1,"]['BRKLY' in client_choice[0]], client=client_choice, 
			   start=start_date, end=end_date)

df = pd.read_sql(sql_query, conn)
print("There are {} rows in this report.".format(df.shape[0]))

df.to_csv("LeadReport_" + client_choice[0]+ "_between_" + start_date + "_and_" + end_date + '.csv')
print("Your report has been outputted.")

