from fastapi import UploadFile
import pandas as pd
from io import BytesIO

async def process_Botswana_file_logic(file: UploadFile):
    content = await file.read()
    df = pd.read_excel(BytesIO(content), sheet_name='CLR', skiprows=1)

    # Aggregation for top 5 customers
    aggregated_data = df.groupby('CUSTOMER_NAME')[['SECTOR', 'FACILITY_TYPE', 'APPROVED AMOUNT (USD)', 'CURRENT EXPOSURE (USD)', 'CLASSIFICATION', 'IFRS_CLASSIFICATION']].sum().reset_index()
    top5_customers = aggregated_data.sort_values(by='CURRENT EXPOSURE (USD)', ascending=False).head(5)

    # Other calculations and aggregations
    ccy = df[df['CURRENCY_TYPE'].isin(['GBP', 'EUR', 'USD', 'ZAR'])]
    direct_exp = df[df['EXPOSURE_TYPE'] == 'DIRECT']
    contingent_exp = df[df['EXPOSURE_TYPE'] == 'CONTINGENT']
    missed_repayments = df['UNPAID AMOUNT (USD)'].sum()
    sumof_all = df['CURRENT EXPOSURE (USD)'].sum()
    fcy_total = ccy['CURRENT EXPOSURE (USD)'].sum()
    fcy_direct = direct_exp[direct_exp['CURRENCY_TYPE'] == 'FCY']['CURRENT EXPOSURE (USD)'].sum()
    sumof_direct = direct_exp['CURRENT EXPOSURE (USD)'].sum()
    sumof_contingent = contingent_exp['CURRENT EXPOSURE (USD)'].sum()
    sumof_stage1 = direct_exp[direct_exp['IFRS_CLASSIFICATION'] == 1]['CURRENT EXPOSURE (USD)'].sum()
    sumof_stage2 = direct_exp[direct_exp['IFRS_CLASSIFICATION'] == 2]['CURRENT EXPOSURE (USD)'].sum()
    sumof_stage3 = direct_exp[direct_exp['IFRS_CLASSIFICATION'] == 3]['CURRENT EXPOSURE (USD)'].sum()

    fcy_direct_percentage = (fcy_direct / sumof_direct) * 100
    fcy_total_percentage = (fcy_total / sumof_all) * 100
    sumof_top5 = top5_customers['CURRENT EXPOSURE (USD)'].sum()
    percentageof_top5 = (sumof_top5 / sumof_all) * 100
    ppl = (sumof_stage1 / sumof_direct) * 100
    wpl = (sumof_stage2 / sumof_direct) * 100
    npl = (sumof_stage3 / sumof_direct) * 100
    mrr = (missed_repayments / sumof_direct) * 100

    # Aggregated data for missed repayments and sectors
    aggregateed_missed = df.groupby('CUSTOMER_NAME')[['SECTOR', 'APPROVED AMOUNT (USD)', 'CURRENT EXPOSURE (USD)', 'UNPAID AMOUNT (USD)']].sum().reset_index()
    missed_repayments_data = aggregateed_missed.sort_values(by='UNPAID AMOUNT (USD)', ascending=False).head(20)

    stage2_df = df[df['IFRS_CLASSIFICATION'] == 2]
    stage2_grouped = stage2_df.groupby('CUSTOMER_NAME')[['APPROVED AMOUNT (USD)', 'CURRENT EXPOSURE (USD)']].sum().reset_index()
    stage2_sorted = stage2_grouped.sort_values(by='CURRENT EXPOSURE (USD)', ascending=False)
    top_20_stage2 = stage2_sorted.head(20)

    aggregateed_sector = df.groupby('SECTOR')[['APPROVED AMOUNT (USD)', 'CURRENT EXPOSURE (USD)']].sum().reset_index()
    sector_data = aggregateed_sector.sort_values(by='CURRENT EXPOSURE (USD)', ascending=False)

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
