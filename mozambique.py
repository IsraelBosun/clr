from fastapi import UploadFile
import pandas as pd
from io import BytesIO

async def process_Mozambique_file_logic(file: UploadFile):
    content = await file.read()
    df = pd.read_excel(BytesIO(content), sheet_name='CLR')

    # Aggregation for top 5 customers
    aggregateed_data = df.groupby('CUSTOMER NAME ')[['SECTOR', 'FACILITY_TYPE', 'AMOUNT_FINANCED_USD', 'OUTSTANDING_BALANCE_USD', 'IFR CLASSIFICATION', 'PRUDENTIAL_CLASSIFICATION']].sum().reset_index()
    top5_customers = aggregateed_data.sort_values(by='OUTSTANDING_BALANCE_USD', ascending=False).head(5)

    # Calculate sums and percentages
    sumof_top5 = top5_customers['OUTSTANDING_BALANCE_USD'].sum()
    sumof_all = df['OUTSTANDING_BALANCE_USD'].sum()

    # Aggregated data for Stage 2
    stage2_df = df[df['IFR CLASSIFICATION'] == 2]
    stage2_grouped = stage2_df.groupby('CUSTOMER NAME ')[['AMOUNT_FINANCED_USD', 'OUTSTANDING_BALANCE_USD']].sum().reset_index()
    stage2_sorted = stage2_grouped.sort_values(by='OUTSTANDING_BALANCE_USD', ascending=False)
    top_20_stage2 = stage2_sorted.head(20)

    # Aggregated data by sector
    aggregateed_sector = df.groupby('SECTOR')[['AMOUNT_FINANCED_USD', 'OUTSTANDING_BALANCE_USD']].sum().reset_index()
    sector_data = aggregateed_sector.sort_values(by='OUTSTANDING_BALANCE_USD', ascending=False)

    # Prepare the result dictionary
    result = {
        "top5_customers": top5_customers.to_dict(orient='records'),
        "top_20_stage2": top_20_stage2.to_dict(orient='records'),
        "sector_data": sector_data.to_dict(orient='records'),
        "sumof_top5": sumof_top5,
        "total_exposure": sumof_all,
    }

    return result
