import streamlit
import pandas as pd
import snowflake.connector
  
def get_demo_table_list():
  with my_cnx.cursor() as my_cur:
      my_cur.execute("SELECT * FROM DEMO_TABLE")
      return my_cur.fetchall()

  
my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
back_from_function = get_demo_table_list()
my_cnx.close()

df = pd.dataframe(back_from_function, columns=['First', 'Last', 'Age'])
streamlit.dataframe(df)


streamlit.table(df)
