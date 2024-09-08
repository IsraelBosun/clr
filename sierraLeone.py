from fastapi import UploadFile
import pandas as pd
from io import BytesIO

async def process_SierraLeone_file_logic(file: UploadFile):
  content = await file.read()
  df = pd.read_excel(BytesIO(content), sheet_name='CLR')
  
  # Set pandas display options
  pd.set_option('display.width', 1000)
  pd.options.display.float_format = '{:,.2f}'.format
  
  # Aggregate data
  aggregateed_data = df.groupby('CUSTOMER NAME')[['SECTOR', 'FACILITY TYPE', 'APPROVED AMOUNT (USD)', 'OUTSTANDING BALANCE (USD)', 'IFRS_CLASSIFICATION', 'PRUDENTIAL_CLASSIFICATION']].sum().reset_index()
  top5_customers = aggregateed_data.sort_values(by='OUTSTANDING BALANCE (USD)', ascending=False).head(5)
  
  sumof_top5 = top5_customers['OUTSTANDING BALANCE (USD)'].sum()
  sumof_all = df['OUTSTANDING BALANCE (USD)'].sum()
  
  ccy = df[df['CURRENCY_TYPE'] == 'FCY']
  direct_exp = df[df['EXPOSURE TYPE'] == 'DIRECT']
  contingent_exp = df[df['EXPOSURE TYPE'] == 'CONTINGENT']
  
  fcy_total = ccy['OUTSTANDING BALANCE (USD)'].sum()
  fcy_direct = direct_exp[direct_exp['CURRENCY_TYPE'] == 'FCY']['OUTSTANDING BALANCE (USD)'].sum()
  sumof_direct = direct_exp['OUTSTANDING BALANCE (USD)'].sum()
  sumof_contingent = contingent_exp['OUTSTANDING BALANCE (USD)'].sum()
  sumof_stage1 = direct_exp[direct_exp['IFRS_CLASSIFICATION'] == 'STAGE 1']['OUTSTANDING BALANCE (USD)'].sum()
  sumof_stage2 = direct_exp[direct_exp['IFRS_CLASSIFICATION'] == 'STAGE 2']['OUTSTANDING BALANCE (USD)'].sum()
  sumof_stage3 = direct_exp[direct_exp['IFRS_CLASSIFICATION'] == 'STAGE 3']['OUTSTANDING BALANCE (USD)'].sum()
  
  fcy_direct_percentage = (fcy_direct / sumof_direct) * 100 if sumof_direct != 0 else 0
  fcy_total_percentage = (fcy_total / sumof_all) * 100 if sumof_all != 0 else 0
  percentageof_top5 = (sumof_top5 / sumof_all) * 100 if sumof_all != 0 else 0
  ppl = (sumof_stage1 / sumof_direct) * 100 if sumof_direct != 0 else 0
  wpl = (sumof_stage2 / sumof_direct) * 100 if sumof_direct != 0 else 0
  npl = (sumof_stage3 / sumof_direct) * 100 if sumof_direct != 0 else 0
  
  # Aggregated data for Stage 2
  stage2_df = df[df['IFRS_CLASSIFICATION'] == 'STAGE 2']
  stage2_grouped = stage2_df.groupby('CUSTOMER NAME')[['APPROVED AMOUNT (USD)', 'OUTSTANDING BALANCE (USD)']].sum().reset_index()
  stage2_sorted = stage2_grouped.sort_values(by='OUTSTANDING BALANCE (USD)', ascending=False)
  top_20_stage2 = stage2_sorted.head(20)
  
  # Aggregated data by sector
  aggregateed_sector = df.groupby('SECTOR')[['APPROVED AMOUNT (USD)', 'OUTSTANDING BALANCE (USD)']].sum().reset_index()
  sector = aggregateed_sector.sort_values(by='OUTSTANDING BALANCE (USD)', ascending=False)
  
  # Prepare the result dictionary
  result = {
      "top5_customers": top5_customers.to_dict(orient='records'),
      "top_20_stage2": top_20_stage2.to_dict(orient='records'),
      "sector_data": sector.to_dict(orient='records'),
      "fcy_direct_percentage": fcy_direct_percentage,
      "fcy_total_percentage": fcy_total_percentage,
      "stage1_loans": sumof_stage1,
      "stage2_loans": sumof_stage2,
      "stage3_loans": sumof_stage3,
      "direct_exposure": sumof_direct,
      "contingent_exposure": sumof_contingent,
      "total_exposure": sumof_all,
      "sum_of_top_5": sumof_top5,
      "percentage_of_top_5": percentageof_top5,
      "ppl": ppl,
      "wpl": wpl,
      "npl": npl,
  }
  
  return result