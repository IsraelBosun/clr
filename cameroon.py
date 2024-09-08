from fastapi import UploadFile
import pandas as pd
from io import BytesIO

async def process_Cameroon_file_logic(file: UploadFile):
  contents = await file.read()
  df = pd.read_excel(BytesIO(contents), sheet_name='CLR')

  # Normalize column names and apply the data processing logic
  def process_dataframe(df):
      # Normalize column names
      df.columns = [col.strip().replace('\n', ' ').replace('\r', ' ') for col in df.columns]

      # Aggregate data and perform calculations
      aggregateed_data = df.groupby('CUSTOMER_NAME')[['SECTOR', 'FACILITY_ TYPE', 'APPROVED AMOUNT USD', 'OUTSANDING BALANCE USD', 'IFRS_ CLASSIFICATION', 'PRUDENTIAL_ CLASSIFICATION']].sum().reset_index()
      top5_customers = aggregateed_data.sort_values(by='OUTSANDING BALANCE USD', ascending=False).head(5)

      sumof_top5 = top5_customers['OUTSANDING BALANCE USD'].sum()
      sumof_all = df['OUTSANDING BALANCE USD'].sum()

      ccy = df[df['CURRENCY_TYPE'] == 'FCY']
      direct_exp = df[df['EXPOSURE_ TYPE'].str.contains('DIRECT', case=False, na=False)]
      contingent_exp = df[df['EXPOSURE_ TYPE'].str.contains('INDIRECT', case=False, na=False)]

      missed_repayments = df['UNPAID AMOUNT (USD)'].sum()
      fcy_total = ccy['OUTSANDING BALANCE USD'].sum()
      fcy_direct = direct_exp[direct_exp['CURRENCY_TYPE'] == 'FCY']['OUTSANDING BALANCE USD'].sum()
      sumof_direct = direct_exp['OUTSANDING BALANCE USD'].sum()
      sumof_contingent = contingent_exp['OUTSANDING BALANCE USD'].sum()
      sumof_stage1 = direct_exp[direct_exp['IFRS_ CLASSIFICATION'] == 'STAGE 1']['OUTSANDING BALANCE USD'].sum()
      sumof_stage2 = direct_exp[direct_exp['IFRS_ CLASSIFICATION'] == 'STAGE 2']['OUTSANDING BALANCE USD'].sum()
      sumof_stage3 = direct_exp[direct_exp['IFRS_ CLASSIFICATION'] == 'STAGE 3']['OUTSANDING BALANCE USD'].sum()

      # Calculating percentages
      fcy_direct_percentage = (fcy_direct / sumof_direct) * 100 if sumof_direct else 0
      fcy_total_percentage = (fcy_total / sumof_all) * 100 if sumof_all else 0
      percentageof_top5 = (sumof_top5 / sumof_all) * 100 if sumof_all else 0
      ppl = (sumof_stage1 / sumof_direct) * 100 if sumof_direct else 0
      wpl = (sumof_stage2 / sumof_direct) * 100 if sumof_direct else 0
      npl = (sumof_stage3 / sumof_direct) * 100 if sumof_direct else 0
      mrr = (missed_repayments / sumof_direct) * 100 if sumof_direct else 0

      # Create the result dictionary
      result = {
          "PPL": f"{ppl:.2f}%",
          "WPL": f"{wpl:.2f}%",
          "NPL": f"{npl:.2f}%",
          "Stage1_Loans": f"{sumof_stage1:,.2f}",
          "Stage2_Loans": f"{sumof_stage2:,.2f}",
          "Stage3_Loans": f"{sumof_stage3:,.2f}",
          "Direct_Exposure": f"{sumof_direct:,.2f}",
          "Contingent_Exposure": f"{sumof_contingent:,.2f}",
          "Total_Outstanding_Exposure": f"{sumof_all:,.2f}",
          "Sum_of_Top_5": f"{sumof_top5:,.2f}",
          "FCY_Direct_Percentage": f"{fcy_direct_percentage:.2f}%",
          "FCY_Total_Percentage": f"{fcy_total_percentage:.2f}%",
          "Missed_Repayments": f"{missed_repayments:,.2f}",
          "Missed_Repayments_Percentage": f"{mrr:.2f}%",
          "Top_5_Percentage": f"{percentageof_top5:.2f}%"
      }

      return result
    
  result = process_dataframe(df)
  return result