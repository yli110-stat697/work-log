import pandas as pd 
import mysql.connector 
import requests
import json
import os

platform = input("Please enter the platform that you want to repost: QMPX or CCMP?")

if platform == 'CCMP':
	url = 'https://leadipapi.cloudcontrol.media/postData'
	conn1 = mysql.connector.connect(host='qmpx-ccmp-prd1.sf.quinstreet.net',
									port=3301,
									user=os.getenv('DB_USER'),
									password=os.getenv('DB_PSWD'))
	conn2 = conn1
elif platform == 'QMPX':
	url = 'https://leadipapi.quinstreet.com/postData'
	conn1 = mysql.connector.connect(host='qmpx-prd-meta1.sf.quinstreet.net',
									port=3307,
									user=os.getenv('DB_USER'),
									password=os.getenv('DB_PSWD'))
	conn2 = mysql.connector.connect(host='qmpx-prd-logging1.sf.quinstreet.net',
									port=3306,
									user=os.getenv('DB_USER'),
									password=os.getenv('DB_PSWD'))
else:
	print("Wrong platform. Please restart the program.")


stuck_query = """
select lc.leadcapture_key, json_extract(lc.lead_info, '$.otherAttributes.txn_id') as txn_id,
        lc.email_address, lc.createddate, lc.aggregator_code, lc.aggregator_lead_key, ls.name as lead_status,
        lc.client_code, isr.name as isr, json_extract(lc.debug_info, '$.validationDetails') as validation_details
from ods.leadcapture lc
left join meta.inquiry_status_reason isr on isr.inquiry_status_reason_key = lc.key2inquiry_status_reason
left join meta.lead_status ls on ls.lead_status_key = lc.key2lead_status
where ls.name in ('STUCK','VALID_PENDING_CLIENT_CONFIRMATION')
		and lc.createddate > NOW() - interval 6 day ;
"""
stuck_leads = pd.read_sql(stuck_query, conn1)
stuck_leads.to_csv('stuck_leads.csv')
print("The dataframe about stuck leads has been outputted.")
print("There are {} stuck leads. Is that correct?".format(stuck_leads.shape[0]))
decision = input("Type Y for yes and N for no: ")
if decision == "Y":
	stuck_leads_LCK = stuck_leads[['leadcapture_key', 'txn_id']].copy()
	stuck_leads_LCK.txn_id = stuck_leads_LCK.txn_id.str.replace('"',"")
	txn_id_tuple = tuple(stuck_leads_LCK.txn_id)

	if len(txn_id_tuple) == 1:
		tracking_sql = """
		select *
		from logging.tracking_info
		where txn_id = '{}' and txn_type2code = 199;
		""".format(txn_id_tuple[0])
	else:
		tracking_sql = """
		select *
		from logging.tracking_info
		where txn_id in {} and txn_type2code = 199;
		""".format(txn_id_tuple)

	txn = pd.read_sql(tracking_sql, conn2)
	df = txn.merge(stuck_leads_LCK, on = 'txn_id')
	df['auth_code'] = df.input_payload.str.extract(r'(Basic .*\|)')
	df['auth_code'] = df.auth_code.str.replace('|', "")
	df['body'] = df.input_payload.str.extract(r'({.*})')
	df = df.iloc[:, -4:]
	df['API body'] = df['body'].str[:-1]+ ',"previousLeadCaptureKey":"' + df.leadcapture_key.astype(str) + '"}' 
	df.to_csv('leads_to_be_reposted.csv')
	print("The leads that will be reposted is outputted.")

	new_df = pd.DataFrame(columns=['TransactionId', 'LeadCaptureKey', 'Status', 'ResponseCode', 'Details'])

	for index, row in df.iterrows():
	    repost = requests.post(url
	                          ,headers={'Content-Type':'application/json'
	                                   ,'Authorization': row['auth_code']}
	                          ,json = json.loads(row['API body']))
	    print(repost.status_code)
	    print(repost.text)
	    new_df = new_df.append(json.loads(repost.text), ignore_index=True)

	new_df.to_csv('repost_responses.csv')
	print("The responses of those reposted are outputted.")

else:
	print("Please rerun the script.")