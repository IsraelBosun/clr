from fastapi import UploadFile
import pandas as pd
from io import BytesIO


async def process_Congo_file_logic(file: UploadFile):
  content = await file.read()
  df = pd.read_excel(BytesIO(content), sheet_name='CLR', skiprows = 7)
  
  df['TOTAL UNPAID'] = pd.to_numeric(df['TOTAL UNPAID'], errors='coerce')
  
  # Aggregation for top 5 customers
  aggregateed_data = df.groupby('CUSTOMER_NAME')[['SECTOR ', 'FACILITY  TYPE', 'APPROVED AMOUNT', 'TOTAL AMOUNT', 'IFRS  CLASSIFICATION', 'BANK CLASSIFICATION  AS AT TODAY ']].sum().reset_index()
  top5_customers = aggregateed_data.sort_values(by='TOTAL AMOUNT', ascending=False).head(5)
  
  # Calculate sums and percentages
  ccy = df[df['CURRENCY  TYPE'] == 'FCY']
  direct_exp = df[df['EXPOSURE  TYPE'] == 'DIRECT']
  contingent_exp = df[df['EXPOSURE  TYPE'] == 'CONTINGENT']
  missed_repayments = df['TOTAL UNPAID'].sum()
  sumof_all = df['TOTAL AMOUNT'].sum()
  fcy_total = ccy['TOTAL AMOUNT'].sum()
  fcy_direct = direct_exp[direct_exp['CURRENCY  TYPE'] == 'FCY']['TOTAL AMOUNT'].sum()
  sumof_direct = direct_exp['TOTAL AMOUNT'].sum()
  sumof_contingent = contingent_exp['TOTAL AMOUNT'].sum()
  sumof_stage1 = direct_exp[direct_exp['IFRS  CLASSIFICATION'] == 'STAGE 1']['TOTAL AMOUNT'].sum()
  sumof_stage2 = direct_exp[direct_exp['IFRS  CLASSIFICATION'] == 'STAGE 2']['TOTAL AMOUNT'].sum()
  sumof_stage3 = direct_exp[direct_exp['IFRS  CLASSIFICATION'] == 'STAGE 3']['TOTAL AMOUNT'].sum()
  
  fcy_direct_percentage = (fcy_direct / sumof_direct) * 100
  fcy_total_percentage = (fcy_total / sumof_all) * 100
  sumof_top5 = top5_customers['TOTAL AMOUNT'].sum()
  percentageof_top5 = (sumof_top5 / sumof_all) * 100
  ppl = (sumof_stage1 / sumof_direct) * 100
  wpl = (sumof_stage2 / sumof_direct) * 100
  npl = (sumof_stage3 / sumof_direct) * 100
  mrr = (missed_repayments / sumof_direct) * 100
  
  # Aggregated data for missed repayments
  aggregateed_missed = df.groupby('CUSTOMER_NAME')[['SECTOR ', 'APPROVED AMOUNT', 'TOTAL AMOUNT', 'TOTAL UNPAID']].sum().reset_index()
  missed_repayments_data = aggregateed_missed.sort_values(by='TOTAL UNPAID', ascending=False).head(20)
  
  # Aggregated data for Stage 2
  stage2_df = df[df['IFRS  CLASSIFICATION'] == 'STAGE 2']
  stage2_grouped = stage2_df.groupby('CUSTOMER_NAME')[['APPROVED AMOUNT', 'TOTAL AMOUNT']].sum().reset_index()
  stage2_sorted = stage2_grouped.sort_values(by='TOTAL AMOUNT', ascending=False)
  top_20_stage2 = stage2_sorted.head(20)
  
  # Aggregated data by sector
  aggregateed_sector = df.groupby('SECTOR ')[['APPROVED AMOUNT', 'TOTAL AMOUNT']].sum().reset_index()
  sector_data = aggregateed_sector.sort_values(by='TOTAL AMOUNT', ascending=False)
  
  # Prepare the result dictionary
  result = {
      "top5_customers": top5_customers.to_dict(orient='records'),
      "missed_repayments_data": missed_repayments_data.to_dict(orient='records'),
      "top_20_stage2": top_20_stage2.to_dict(orient='records'),
      "sector_data": sector_data.to_dict(orient='records'),
      "fcy_direct_percentage": fcy_direct_percentage,
      "fcy_total_percentage": fcy_total_percentage,
      "stage1_loans": sumof_stage1,
      "stage2_loans": sumof_stage2,
      "stage3_loans": sumof_stage3,
      "direct_exposure": sumof_direct,
      "contingent_exposure": sumof_contingent,
      "total_exposure": sumof_all,
      "missed_repayments": missed_repayments,
      "ppl": ppl,
      "wpl": wpl,
      "npl": npl,
      "fcy_direct": fcy_direct,
      "fcy_total": fcy_total,
      "mrr": mrr,
      "percentage_of_top5": percentageof_top5,
  }
  
  return result