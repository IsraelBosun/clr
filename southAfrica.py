from fastapi import UploadFile
import pandas as pd
from io import BytesIO


async def process_SouthAfrica_file_logic(file: UploadFile):
  content = await file.read()
  df = pd.read_excel(BytesIO(content), sheet_name='CLR')
  
  df['OUTSTANDING BALANCE (USD)'] = df['OUTSTANDING BALANCE (USD)'].abs()
  
  # Aggregation for top 5 customers
  aggregateed_data = df.groupby('CUST_ID')[['SECTOR', 'FACILITY_DISCRIPTION', 'APPROVED AMOUNT (USD)', 'OUTSTANDING BALANCE (USD)', 'GROUP IFRS_CLASSIFICATION PER 31.12.2021 NPL Stage 3 ', 'PRUDENTIAL_CLASSIFICATION- IN WORDS WATCHLIST ETC']].sum().reset_index()
  top5_customers = aggregateed_data.sort_values(by='OUTSTANDING BALANCE (USD)', ascending=False).head(5)
  
  # Calculate sums and percentages
  ccy = df[df['CCY'] == 'FCY']
  direct_exp = df[df['EXPOSURE_TYPE'] == 'DIRECT']
  contingent_exp = df[df['EXPOSURE_TYPE'] == 'INDIRECT']
  missed_repayments = df['UNPAID AMOUNT (USD)'].sum()
  sumof_all = df['OUTSTANDING BALANCE (USD)'].sum()
  fcy_total = ccy['OUTSTANDING BALANCE (USD)'].sum()
  fcy_direct = direct_exp[direct_exp['CCY'] == 'FCY']['OUTSTANDING BALANCE (USD)'].sum()
  sumof_direct = direct_exp['OUTSTANDING BALANCE (USD)'].sum()
  sumof_contingent = contingent_exp['OUTSTANDING BALANCE (USD)'].sum()
  sumof_stage1 = direct_exp[direct_exp['GROUP IFRS_CLASSIFICATION PER 31.12.2021 NPL Stage 3 '] == 'STAGE 1']['OUTSTANDING BALANCE (USD)'].sum()
  sumof_stage2 = direct_exp[direct_exp['GROUP IFRS_CLASSIFICATION PER 31.12.2021 NPL Stage 3 '] == 'STAGE 2']['OUTSTANDING BALANCE (USD)'].sum()
  sumof_stage3 = direct_exp[direct_exp['GROUP IFRS_CLASSIFICATION PER 31.12.2021 NPL Stage 3 '] == 'STAGE 3']['OUTSTANDING BALANCE (USD)'].sum()
  
  fcy_direct_percentage = (fcy_direct / sumof_direct) * 100
  fcy_total_percentage = (fcy_total / sumof_all) * 100
  sumof_top5 = top5_customers['OUTSTANDING BALANCE (USD)'].sum()
  percentageof_top5 = (sumof_top5 / sumof_all) * 100
  ppl = (sumof_stage1 / sumof_direct) * 100
  wpl = (sumof_stage2 / sumof_direct) * 100
  npl = (sumof_stage3 / sumof_direct) * 100
  mrr = (missed_repayments / sumof_direct) * 100
  
  # Aggregated data for missed repayments
  aggregateed_missed = df.groupby('CUST_ID')[['SECTOR', 'APPROVED AMOUNT (USD)', 'OUTSTANDING BALANCE (USD)', 'UNPAID AMOUNT (USD)']].sum().reset_index()
  missed_repayments_data = aggregateed_missed.sort_values(by='UNPAID AMOUNT (USD)', ascending=False).head(20)
  
  # Aggregated data for Stage 2
  stage2_df = df[df['GROUP IFRS_CLASSIFICATION PER 31.12.2021 NPL Stage 3 '] == 'STAGE 2']
  stage2_grouped = stage2_df.groupby('CUST_ID')[['APPROVED AMOUNT (USD)', 'OUTSTANDING BALANCE (USD)']].sum().reset_index()
  stage2_sorted = stage2_grouped.sort_values(by='OUTSTANDING BALANCE (USD)', ascending=False)
  top_20_stage2 = stage2_sorted.head(20)
  
  # Aggregated data by sector
  aggregateed_sector = df.groupby('SECTOR')[['APPROVED AMOUNT (USD)', 'OUTSTANDING BALANCE (USD)']].sum().reset_index()
  sector_data = aggregateed_sector.sort_values(by='OUTSTANDING BALANCE (USD)', ascending=False)
  
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