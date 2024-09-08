from fastapi import UploadFile
import pandas as pd
from io import BytesIO


async def process_Gambia_file_logic(file: UploadFile):
  content = await file.read()
  df = pd.read_excel(BytesIO(content), sheet_name='CLR')
  
  # Aggregation for top 5 customers
  aggregateed_data = df.groupby('CUSTOMER_NAME')[['SECTOR', 'FACILITY_DESCRIPTION', 'APPROVED AMOUNT', 'OUTSTANDING BALANCE', 'IFRS_CLASSIFICATION', 'PRUDENTIAL_CLASSIFICATION']].sum().reset_index()
  top5_customers = aggregateed_data.sort_values(by='OUTSTANDING BALANCE', ascending=False).head(5)
  
  # Calculate sums and percentages
  ccy = df[df['CURRENCY_TYPE'] == 'FCY']
  direct_exp = df[df['EXPOSURE_TYPE'] == 'DIRECT']
  contingent_exp = df[df['EXPOSURE_TYPE'] == 'CONTINGENT']
  missed_repayments = df['UNPAID AMOUNT (USD)'].sum()
  sumof_all = df['OUTSTANDING BALANCE'].sum()
  fcy_total = ccy['OUTSTANDING BALANCE'].sum()
  fcy_direct = direct_exp[direct_exp['CURRENCY_TYPE'] == 'FCY']['OUTSTANDING BALANCE'].sum()
  sumof_direct = direct_exp['OUTSTANDING BALANCE'].sum()
  sumof_contingent = contingent_exp['OUTSTANDING BALANCE'].sum()
  sumof_stage1 = direct_exp[direct_exp['IFRS_CLASSIFICATION'] == 'STAGE 1']['OUTSTANDING BALANCE'].sum()
  sumof_stage2 = direct_exp[direct_exp['IFRS_CLASSIFICATION'] == 'STAGE 2']['OUTSTANDING BALANCE'].sum()
  sumof_stage3 = direct_exp[direct_exp['IFRS_CLASSIFICATION'] == 'STAGE 3']['OUTSTANDING BALANCE'].sum()
  
  fcy_direct_percentage = (fcy_direct / sumof_direct) * 100
  fcy_total_percentage = (fcy_total / sumof_all) * 100
  sumof_top5 = top5_customers['OUTSTANDING BALANCE'].sum()
  percentageof_top5 = (sumof_top5 / sumof_all) * 100
  ppl = (sumof_stage1 / sumof_direct) * 100
  wpl = (sumof_stage2 / sumof_direct) * 100
  npl = (sumof_stage3 / sumof_direct) * 100
  mrr = (missed_repayments / sumof_direct) * 100
  
  # Aggregated data for missed repayments
  aggregateed_missed = df.groupby('CUSTOMER_NAME')[['SECTOR', 'APPROVED AMOUNT', 'OUTSTANDING BALANCE', 'UNPAID AMOUNT (USD)']].sum().reset_index()
  missed_repayments_data = aggregateed_missed.sort_values(by='UNPAID AMOUNT (USD)', ascending=False).head(20)
  
  # Aggregated data for Stage 2
  stage2_df = df[df['IFRS_CLASSIFICATION'] == 'STAGE 2']
  stage2_grouped = stage2_df.groupby('CUSTOMER_NAME')[['APPROVED AMOUNT', 'OUTSTANDING BALANCE']].sum().reset_index()
  stage2_sorted = stage2_grouped.sort_values(by='OUTSTANDING BALANCE', ascending=False)
  top_20_stage2 = stage2_sorted.head(20)
  
  # Aggregated data by sector
  aggregateed_sector = df.groupby('SECTOR')[['APPROVED AMOUNT', 'OUTSTANDING BALANCE']].sum().reset_index()
  sector_data = aggregateed_sector.sort_values(by='OUTSTANDING BALANCE', ascending=False)
  
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