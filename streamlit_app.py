import streamlit
import pandas as pd
import snowflake.connector
  
def get_demo_table_list():
  with my_cnx.cursor() as my_cur:
      my_cur.execute("SELECT * FROM DEMO_TABLE")
      return my_cur.fetchall()

def get_demo_transaction_list():
  with my_cnx.cursor() as my_cur_transactions:
      my_cur_transactions.execute("SELECT *, YEAR(t_date) as transactionYear, MONTH(t_date) as transactionMonth FROM TRANSACTION_HISTORY")
      return my_cur_transactions.fetchall()


my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
back_from_function = get_demo_table_list()
# my_cnx.close()

df = pd.DataFrame(back_from_function, columns=['First', 'Last', 'Age'])
streamlit.dataframe(df)


streamlit.table(df)

back_from_transactions = get_demo_transaction_list()
my_cnx.close()



df_transactions = pd.DataFrame(back_from_transactions, columns=['transactionDate', 'transactionDescription', 'transactionYear', 'transactionMonth'])
# df_transactions['year'] = df_transactions['transactionDate'].dt.to_period('M')
streamlit.table(df_transactions)


df_m_rep = pd.DataFrame(df_transactions['transactionMonth'].unique().tolist())

streamlit.write(df_m_rep.values.tolist())

filt = (df_m_rep.values.tolist())
df_months_represented = df_transactions[filt]

streamlit.table(df_months_represented)