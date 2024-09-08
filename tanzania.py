from fastapi import UploadFile
import pandas as pd
from io import BytesIO

async def process_Tanzania_file_logic(file: UploadFile):
  content = await file.read()
  df = pd.read_excel(BytesIO(content), sheet_name='CLR')

  # Apply conversion
  df['APPROVED AMOUNT TCY'] = df['APPROVED AMOUNT TCY'] / 2723

  # Aggregation for top 5 customers
  aggregateed_data = df.groupby('CUSTOMER_NAME')[['SECTOR', 'FACILITY_TYPE', 'APPROVED AMOUNT TCY', 'OUTSTANDING BALANCE USD', 'IFRS_CLASSIFICATION', 'PRUDENTIAL_CLASSIFICATION']].sum().reset_index()
  top5_customers = aggregateed_data.sort_values(by='OUTSTANDING BALANCE USD', ascending=False).head(5)

  # Calculate sums and percentages
  ccy = df[df['CURRENCY_TYPE'] == 'USD']
  contingent_exp = df[df['EXPOSURE_TYPE'] == 'CONTINGENT']
  missed_repayments = df['UNPAID AMOUNT INTEREST(USD)'].sum()
  sumof_all = df['OUTSTANDING BALANCE USD'].sum()
  fcy_total = ccy['OUTSTANDING BALANCE USD'].sum()
  fcy_direct = df[df['CURRENCY_TYPE'] == 'USD']['OUTSTANDING BALANCE USD'].sum()
  sumof_stage1 = df[df['IFRS_CLASSIFICATION'] == 'Stage 1']['OUTSTANDING BALANCE USD'].sum()
  sumof_stage2 = df[df['IFRS_CLASSIFICATION'] == 'Stage 2']['OUTSTANDING BALANCE USD'].sum()
  sumof_stage3 = df[df['IFRS_CLASSIFICATION'] == 'Stage 3']['OUTSTANDING BALANCE USD'].sum()

  fcy_total_percentage = (fcy_total / sumof_all) * 100
  percentageof_top5 = (top5_customers['OUTSTANDING BALANCE USD'].sum() / sumof_all) * 100
  ppl = (sumof_stage1 / sumof_all) * 100
  wpl = (sumof_stage2 / sumof_all) * 100
  npl = (sumof_stage3 / sumof_all) * 100
  mrr = (missed_repayments / sumof_all) * 100

  # Aggregated data for missed repayments
  aggregateed_missed = df.groupby('CUSTOMER_NAME')[['SECTOR', 'APPROVED AMOUNT TCY', 'OUTSTANDING BALANCE USD', 'UNPAID AMOUNT INTEREST(USD)']].sum().reset_index()
  missed_repayments_data = aggregateed_missed.sort_values(by='UNPAID AMOUNT INTEREST(USD)', ascending=False).head(20)

  # Aggregated data for Stage 2
  stage2_df = df[df['IFRS_CLASSIFICATION'] == 'Stage 2']
  stage2_grouped = stage2_df.groupby('CUSTOMER_NAME')[['APPROVED AMOUNT TCY', 'OUTSTANDING BALANCE USD']].sum().reset_index()
  top_20_stage2 = stage2_grouped.sort_values(by='OUTSTANDING BALANCE USD', ascending=False).head(20)

  # Aggregated data by sector
  aggregateed_sector = df.groupby('SECTOR')[['APPROVED AMOUNT TCY', 'OUTSTANDING BALANCE USD']].sum().reset_index()
  sector_data = aggregateed_sector.sort_values(by='OUTSTANDING BALANCE USD', ascending=False)

  # Prepare the result dictionary
  result = {
      "top5_customers": top5_customers.to_dict(orient='records'),
      "missed_repayments_data": missed_repayments_data.to_dict(orient='records'),
      "top_20_stage2": top_20_stage2.to_dict(orient='records'),
      "sector_data": sector_data.to_dict(orient='records'),
      "fcy_total_percentage": fcy_total_percentage,
      "stage1_loans": sumof_stage1,
      "stage2_loans": sumof_stage2,
      "stage3_loans": sumof_stage3,
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