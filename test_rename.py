import pandas as pd
import re

master_df = pd.DataFrame()
s = pd.Series([1,2,3]).to_frame()
s.columns = [f"DA_Price_{str(c).replace(' ', '_')}" for c in s.columns]
master_df = master_df.join(s, how='right')

clean_columns = {col: re.sub(r'\s*_0$', '', str(col)) for col in master_df.columns}
master_df.rename(columns=clean_columns, inplace=True)
print(master_df.columns)
