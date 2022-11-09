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


df_m_rep = pd.DataFrame(df_transactions['transactionMonth'].unique().tolist(), columns = ['transactionMonth'])

df_y_rep = pd.DataFrame(df_transactions['transactionYear'].unique().tolist(), columns = ['transactionYear'])

filt_m = (df_transactions['transactionMonth'].isin(df_m_rep['transactionMonth'].values.tolist()))
streamlit.write(filt_m)

filt_y = (df_transactions['transactionYear'].isin(df_y_rep['transactionYear'].values.tolist()))
streamlit.write(filt_y)

df_months_represented = pd.DataFrame(df_transactions[filt_m], columns=['transactionDate', 'transactionDescription', 'transactionYear', 'transactionMonth'])

streamlit.table(df_months_represented.drop_duplicates(subset='transactionMonth'))

streamlit.table(df_months_represented.drop_duplicates(subset='transactionYear'))

df_sl_years = df_months_represented['transactionYear'].drop_duplicates()
df_sl_years.set_index(['transactionYear', inplace=true)

streamlit.table(df_sl_years)

t_years = df_sl_years['transactionYear'].values.tolist()

streamlit.write(t_years)

# streamlit.slider("The Year: ", value=df_sl_years['transactionYear'])