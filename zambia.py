from fastapi import UploadFile
import pandas as pd
from io import BytesIO

async def process_Zambia_file_logic(file: UploadFile):
    content = await file.read()
    df = pd.read_excel(BytesIO(content), sheet_name='CLR')

    # Normalize column names
    def normalize_column_name(col_name):
        col_name = col_name.strip().replace('\n', ' ').replace('\r', ' ')
        while '  ' in col_name:
            col_name = col_name.replace('  ', ' ')
        return col_name

    df.columns = [normalize_column_name(col) for col in df.columns]

    # Adjust for exchange rate
    df['APPROVED AMOUNT TCY'] = df['APPROVED AMOUNT TCY'] / df['EXCHANGE RATE']

    # Aggregation for top 5 customers
    aggregated_data = df.groupby('CUSTOMER_NAME')[['SECTOR', 'FACILITY_TYPE', 'APPROVED AMOUNT TCY', 'TOTAL EXPOSURE USD', 'IFRS_CLASSIFICATION', 'PRUDENTIAL_CLASSIFICATION']].sum().reset_index()
    top5_customers = aggregated_data.sort_values(by='TOTAL EXPOSURE USD', ascending=False).head(5)

    # Calculate sums and percentages
    ccy = df[df['CURRENCY_TYPE'] == 'FCY']
    direct_exp = df[df['EXPOSURE_TYPE'] == 'DIRECT']
    contingent_exp = df[df['EXPOSURE_TYPE'] == 'INDIRECT']
    missed_repayments = df['UNPAID AMOUNT INTEREST(USD)'].sum()
    sumof_all = df['TOTAL EXPOSURE USD'].sum()
    fcy_total = ccy['TOTAL EXPOSURE USD'].sum()
    fcy_direct = direct_exp[direct_exp['CURRENCY_TYPE'] == 'FCY']['TOTAL EXPOSURE USD'].sum()
    sumof_direct = direct_exp['TOTAL EXPOSURE USD'].sum()
    sumof_contingent = contingent_exp['TOTAL EXPOSURE USD'].sum()
    sumof_stage1 = direct_exp[direct_exp['IFRS_CLASSIFICATION'] == 'STAGE 1']['TOTAL EXPOSURE USD'].sum()
    sumof_stage2 = direct_exp[direct_exp['IFRS_CLASSIFICATION'] == 'STAGE 2']['TOTAL EXPOSURE USD'].sum()
    sumof_stage3 = direct_exp[direct_exp['IFRS_CLASSIFICATION'] == 'STAGE 3']['TOTAL EXPOSURE USD'].sum()

    fcy_direct_percentage = (fcy_direct / sumof_direct) * 100
    fcy_total_percentage = (fcy_total / sumof_all) * 100
    percentageof_top5 = (top5_customers['TOTAL EXPOSURE USD'].sum() / sumof_all) * 100
    ppl = (sumof_stage1 / sumof_direct) * 100
    wpl = (sumof_stage2 / sumof_direct) * 100
    npl = (sumof_stage3 / sumof_direct) * 100
    mrr = (missed_repayments / sumof_direct) * 100

    # Aggregated data for missed repayments
    aggregated_missed = df.groupby('CUSTOMER_NAME')[['SECTOR', 'APPROVED AMOUNT TCY', 'TOTAL EXPOSURE USD', 'UNPAID AMOUNT INTEREST(USD)']].sum().reset_index()
    missed_repayments_data = aggregated_missed.sort_values(by='UNPAID AMOUNT INTEREST(USD)', ascending=False).head(20)

    # Aggregated data for Stage 2
    stage2_df = df[df['IFRS_CLASSIFICATION'] == 'STAGE 2']
    stage2_grouped = stage2_df.groupby('CUSTOMER_NAME')[['APPROVED AMOUNT TCY', 'TOTAL EXPOSURE USD']].sum().reset_index()
    top_20_stage2 = stage2_grouped.sort_values(by='TOTAL EXPOSURE USD', ascending=False).head(20)

    # Aggregated data by sector
    aggregated_sector = df.groupby('SECTOR')[['APPROVED AMOUNT TCY', 'TOTAL EXPOSURE USD']].sum().reset_index()
    sector_data = aggregated_sector.sort_values(by='TOTAL EXPOSURE USD', ascending=False)

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
