
from processor.cleaning_utils import load_csv, clean_df

df = load_csv("data/raw/medical_appointments.csv")
print("Before:", df.shape)
df_clean = clean_df(df)
print("After: ", df_clean.shape)
print(df_clean.head())
