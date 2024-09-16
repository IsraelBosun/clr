from fastapi import UploadFile
import pandas as pd
from io import BytesIO

async def process_Kenya_file_logic(file: UploadFile):
    content = await file.read()
    df = pd.read_excel(BytesIO(content), sheet_name='CLR', skiprows=6)

    pd.options.display.float_format = '{:,.2f}'.format

    # Aggregated data by customer name
    aggregated_data = df.groupby('CUSTOMER NAME')[['SECTOR', 'APPROVED TOTAL FACILITY AMOUNT/LIMIT', 'TOTAL EXPOSURES(USD)', 'IFRS', 'CLASSIFICATION']].sum().reset_index()
    top5_customers = aggregated_data.sort_values(by='TOTAL EXPOSURES(USD)', ascending=False).head(5)

    # Calculate the FCY and percentages
    ccy = df[df['CURRENCY TYPE'] == 'FCY']
    fcy_direct = ccy['TOTAL DIRECT EXPOSURES(USD)'].sum()
    fcy_total = ccy['TOTAL EXPOSURES(USD)'].sum()
    sumof_direct = df['TOTAL DIRECT EXPOSURES(USD)'].sum()
    sumof_all = df['TOTAL EXPOSURES(USD)'].sum()
    fcy_direct_percentage = (fcy_direct / sumof_direct) * 100 if sumof_direct != 0 else 0
    fcy_total_percentage = (fcy_total / sumof_all) * 100 if sumof_all != 0 else 0

    # Calculate the stages
    sumof_stage1 = df[df['IFRS'] == 'STAGE 1']['TOTAL EXPOSURES(USD)'].sum()
    sumof_stage2 = df[df['IFRS'] == 'STAGE 2']['TOTAL EXPOSURES(USD)'].sum()
    sumof_stage3 = df[df['IFRS'] == 'STAGE 3']['TOTAL EXPOSURES(USD)'].sum()
    sumof_contingent = df['TOTAL CONTINGENT EXPOSURES(USD)'].sum()

    ppl = (sumof_stage1 / sumof_direct) * 100 if sumof_direct != 0 else 0
    wpl = (sumof_stage2 / sumof_direct) * 100 if sumof_direct != 0 else 0
    npl = (sumof_stage3 / sumof_direct) * 100 if sumof_direct != 0 else 0

    # Prepare the result dictionary
    result = {
        "top5_customers": top5_customers.to_dict(orient='records'),
        "fcy_direct_percentage": fcy_direct_percentage,
        "fcy_total_percentage": fcy_total_percentage,
        "stage1_loans": sumof_stage1,
        "stage2_loans": sumof_stage2,
        "stage3_loans": sumof_stage3,
        "direct_exposure": sumof_direct,
        "contingent_exposure": sumof_contingent,
        "total_exposure": sumof_all,
        "ppl": ppl,
        "wpl": wpl,
        "npl": npl,
        "fcy_direct": fcy_direct,
        "fcy_total": fcy_total
    }
    
    return result
