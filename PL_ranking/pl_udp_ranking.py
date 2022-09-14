"""
Requirements:
1. psycopg2
2. pandas

"""

import psycopg2
import pandas as pd

# Set the address of csv file accordingly
data = pd.read_csv('scripts/PL_ranking/PL UDP Ranking - Sheet1.csv')
df = pd.DataFrame(data, columns=['UDP_ID', 'SKU_ID', 'Suggested UDP Rank'])

conn = psycopg2.connect(
   database="data_service_db", user='data_service_user', password='data_service_pass', host='52.66.130.251', port= '5431'
)
conn.autocommit = True
cursor = conn.cursor()

for row in df.itertuples():
   cursor.execute("SELECT id, page_id FROM udp WHERE id = {}".format(row[1],))
   res = cursor.fetchone()

   #For handling the case with no page_id for given udp_id
   if res is None:
      #print("No page_id for udp {}".format(row[1]))
      continue

   #For handling entries with missing sku_id
   try:
      cursor.execute("SELECT * FROM pages_sku_mapping WHERE page_id={} AND sku_id={}".format(res[1], row[2]))
   except:
      #print("SKU_ID missing for udp {}".format(res[1]))
      continue
   
   dt = cursor.fetchone()

   #For handling the foreign key constraint 
   try:
      if dt is None:
         cursor.execute("INSERT INTO pages_sku_mapping (page_id, sku_id, custom_position) VALUES({}, {}, {})".format(res[1], row[2], row[3]))
      else:
         cursor.execute("UPDATE pages_sku_mapping SET custom_position={} WHERE page_id={} AND sku_id={}".format(row[3], res[1], row[2]))
   except:
      #print("Foreign Key constraint violated for udp {} and sku {}".format(res[1], row[2]))
      continue


conn.close()