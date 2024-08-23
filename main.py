from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from io import BytesIO

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)


@app.post("/botswana")
async def process_Botswana_file(file: UploadFile = File(...)):
    try:
        # Read the uploaded Excel file into a pandas DataFrame
        content = await file.read()
        df = pd.read_excel(BytesIO(content), sheet_name='CLR', skiprows=1)

        # Perform the aggregation
        pd.options.display.float_format = '{:,.2f}'.format
        aggregated_data = df.groupby('CUSTOMER_NAME')[['SECTOR', 'FACILITY_TYPE', 'APPROVED AMOUNT (USD)', 'CURRENT EXPOSURE (USD)', 'CLASSIFICATION', 'IFRS_CLASSIFICATION' ]].sum().reset_index()
        top5_customers = aggregated_data.sort_values(by='CURRENT EXPOSURE (USD)', ascending=False).head(5)

        # Convert the DataFrame to a dictionary for JSON response
        result = top5_customers.to_dict(orient='records')
        return {"top5_customers": result}

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An error occurred: {str(e)}")


@app.post("/kenya")
async def process_Kenya_file(file: UploadFile = File(...)):
    try:
        # Read the uploaded Excel file into a pandas DataFrame
        content = await file.read()
        df = pd.read_excel(BytesIO(content), sheet_name='CLR', skiprows=6)

        # Perform the aggregation for top 5 customers
        aggregated_data = df.groupby('CUSTOMER NAME')[['SECTOR', 'APPROVED TOTAL FACILITY AMOUNT/LIMIT', 'TOTAL EXPOSURES(USD)', 'IFRS', 'CLASSIFICATION']].sum().reset_index()
        top5_customers = aggregated_data.sort_values(by='TOTAL EXPOSURES(USD)', ascending=False).head(5)
        top5_customers['APPROVED TOTAL FACILITY AMOUNT/LIMIT'] = top5_customers['APPROVED TOTAL FACILITY AMOUNT/LIMIT'] / 129

        # Calculate the FCY and percentages
        ccy = df[df['CURRENCY TYPE'] == 'FCY']
        fcy_direct = ccy['TOTAL DIRECT EXPOSURES(USD)'].sum()
        fcy_total = ccy['TOTAL EXPOSURES(USD)'].sum()
        sumof_direct = df['TOTAL DIRECT EXPOSURES(USD)'].sum()
        sumof_all = df['TOTAL EXPOSURES(USD)'].sum()
        fcy_direct_percentage = (fcy_direct / sumof_direct) * 100
        fcy_total_percentage = (fcy_total / sumof_all) * 100

        # Calculate the stages
        sumof_stage1 = df[df['IFRS'] == 'STAGE 1']['TOTAL DIRECT EXPOSURES(USD)'].sum()
        sumof_stage2 = df[df['IFRS'] == 'STAGE 2']['TOTAL DIRECT EXPOSURES(USD)'].sum()
        sumof_stage3 = df[df['IFRS'] == 'STAGE 3']['TOTAL DIRECT EXPOSURES(USD)'].sum()
        sumof_contingent = df['TOTAL CONTINGENT EXPOSURES(USD)'].sum()
        sumof_all = df['TOTAL EXPOSURES(USD)'].sum()

        # Missed Repayments Calculation
        missed_repayments = df['MISSED INSTALLMENT'].sum() / 129
        mrr = (missed_repayments / sumof_direct) * 100

        # Additional Calculations
        sumof_top5 = top5_customers['TOTAL EXPOSURES(USD)'].sum()
        time_loan = df['TERM LOAN'].sum() / 129
        time_termLoan = df['TERM /TIME'].sum() / 129
        percentageof_top5 = (sumof_top5 / sumof_all) * 100
        percentageof_timeLoan = (time_loan / sumof_direct) * 100
        percentageof_timeTermLoan = (time_termLoan / sumof_direct) * 100

        # Aggregated Missed Customers Calculation
        aggregated_missed = df.groupby('CUSTOMER NAME')[['SECTOR', 'APPROVED TOTAL FACILITY AMOUNT/LIMIT', 'TOTAL EXPOSURES(USD)', 'MISSED INSTALLMENT']].sum().reset_index()
        missed_customers = aggregated_missed.sort_values(by='MISSED INSTALLMENT', ascending=False).head(20)
        missed_customers['MISSED INSTALLMENT'] = missed_customers['MISSED INSTALLMENT'] / 129
        missed_customers['APPROVED TOTAL FACILITY AMOUNT/LIMIT'] = missed_customers['APPROVED TOTAL FACILITY AMOUNT/LIMIT'] / 129

        # Prepare the result dictionary
        result = {
            "top5_customers": top5_customers.to_dict(orient='records'),
            "missed_customers": missed_customers.to_dict(orient='records'),
            "fcy_direct_percentage": fcy_direct_percentage,
            "fcy_total_percentage": fcy_total_percentage,
            "stage1_loans": sumof_stage1,
            "stage2_loans": sumof_stage2,
            "stage3_loans": sumof_stage3,
            "direct_exposure": sumof_direct,
            "contingent_exposure": sumof_contingent,
            "total_exposure": sumof_all,
            "missed_repayments": missed_repayments,
            "ppl": (sumof_stage1 / sumof_direct) * 100,
            "wpl": (sumof_stage2 / sumof_direct) * 100,
            "npl": (sumof_stage3 / sumof_direct) * 100,
            "fcy_direct": fcy_direct,
            "fcy_total": fcy_total,
            "mrr": mrr,
            "percentageof_top5": percentageof_top5,
            "percentageof_timeLoan": percentageof_timeLoan,
            "percentageof_timeTermLoan": percentageof_timeTermLoan,
        }
        return result

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An error occurred: {str(e)}")


@app.post("/ghana")
async def process_Ghana_file(file: UploadFile = File(...)):
    try:
        # Read the uploaded Excel file into a pandas DataFrame
        content = await file.read()
        df = pd.read_excel(BytesIO(content), sheet_name='CLR')

        # Aggregation for top 5 customers
        aggregateed_data = df.groupby('CUSTOMER_NAME')[['SECTOR', 'FACILITY DESCRIPTION', 'APPROVED AMOUNT (USD)', 'OUTSTANDING BALANCE (USD)', 'IFRS_CLASSIFICATION', 'PRUDENTIAL_CLASSIFICATION']].sum().reset_index()
        top5_customers = aggregateed_data.sort_values(by='OUTSTANDING BALANCE (USD)', ascending=False).head(5)

        # Calculate sums and percentages
        ccy = df[df['CURRENCY_TYPE'] == 'FCY']
        direct_exp = df[df['EXPOSURE_TYPE'] == 'DIRECT']
        contingent_exp = df[df['EXPOSURE_TYPE'] == 'CONTINGENT']
        missed_repayments = df['UNPAID AMOUNT (USD)'].sum()
        sumof_all = df['OUTSTANDING BALANCE (USD)'].sum()
        fcy_total = ccy['OUTSTANDING BALANCE (USD)'].sum()
        fcy_direct = direct_exp[direct_exp['CURRENCY_TYPE'] == 'FCY']['OUTSTANDING BALANCE (USD)'].sum()
        sumof_direct = direct_exp['OUTSTANDING BALANCE (USD)'].sum()
        sumof_contingent = contingent_exp['OUTSTANDING BALANCE (USD)'].sum()
        sumof_stage1 = direct_exp[direct_exp['IFRS_CLASSIFICATION'] == 'STAGE 1']['OUTSTANDING BALANCE (USD)'].sum()
        sumof_stage2 = direct_exp[direct_exp['IFRS_CLASSIFICATION'] == 'STAGE 2']['OUTSTANDING BALANCE (USD)'].sum()
        sumof_stage3 = direct_exp[direct_exp['IFRS_CLASSIFICATION'] == 'STAGE 3']['OUTSTANDING BALANCE (USD)'].sum()

        fcy_direct_percentage = (fcy_direct / sumof_direct) * 100
        fcy_total_percentage = (fcy_total / sumof_all) * 100
        sumof_top5 = top5_customers['OUTSTANDING BALANCE (USD)'].sum()
        percentageof_top5 = (sumof_top5 / sumof_all) * 100
        ppl = (sumof_stage1 / sumof_direct) * 100
        wpl = (sumof_stage2 / sumof_direct) * 100
        npl = (sumof_stage3 / sumof_direct) * 100
        mrr = (missed_repayments / sumof_direct) * 100

        # Aggregated data for missed repayments
        aggregateed_missed = df.groupby('CUSTOMER_NAME')[['SECTOR', 'APPROVED AMOUNT (USD)', 'OUTSTANDING BALANCE (USD)', 'UNPAID AMOUNT (USD)']].sum().reset_index()
        missed_repayments_data = aggregateed_missed.sort_values(by='UNPAID AMOUNT (USD)', ascending=False).head(20)

        # Aggregated data for Stage 2
        stage2_df = df[df['IFRS_CLASSIFICATION'] == 'STAGE 2']
        stage2_grouped = stage2_df.groupby('CUSTOMER_NAME')[['APPROVED AMOUNT (USD)', 'OUTSTANDING BALANCE (USD)']].sum().reset_index()
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

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An error occurred: {str(e)}")


@app.post("/angola")
async def process_Angola_file(file: UploadFile = File(...)):
    try:
        # Read the uploaded Excel file into a pandas DataFrame
        content = await file.read()
        df = pd.read_excel(BytesIO(content), sheet_name='CLR', skiprows = 2)

        # Perform the aggregation for top 5 customers
        pd.options.display.float_format = '{:,.2f}'.format
        aggregateed_data = df.groupby('CUSTOMER_NAME')[['SECTOR', 'FACILITY_TYPE', 'APPROVED AMOUNT (USD)', 'OUTSTANDING BALANCE \n(USD)', 'IFRS_CLASSIFICATION', 'PRUDENTIAL_CLASSIFICATION']].sum().reset_index()
        top5_customers = aggregateed_data.sort_values(by='OUTSTANDING BALANCE \n(USD)', ascending=False).head(5)
        print(df.columns)

        # Calculate the FCY and percentages
        ccy = df[df['CURRENCY_TYPE'] == 'FCY']
        exposure_type = df[df['EXPOSURE_TYPE'] == 'DIRECT    ']
        contingent_exp = df[df['EXPOSURE_TYPE'] == 'CONTINGENT']
        fcy_direct = exposure_type[exposure_type['CURRENCY_TYPE'] == 'FCY']['OUTSTANDING BALANCE \n(USD)'].sum()
        fcy_total = ccy['OUTSTANDING BALANCE \n(USD)'].sum()
        sumof_direct = exposure_type['OUTSTANDING BALANCE \n(USD)'].sum()         
        sumof_all = df['OUTSTANDING BALANCE \n(USD)'].sum()
        fcy_direct_percentage = (fcy_direct / sumof_direct) * 100
        fcy_total_percentage = (fcy_total / sumof_all) * 100

        # Calculate the stages
        sumof_stage1 = df[df['IFRS_CLASSIFICATION'] == 'Stage 1']['OUTSTANDING BALANCE \n(USD)'].sum()
        sumof_stage2 = df[df['IFRS_CLASSIFICATION'] == 'Stage 2']['OUTSTANDING BALANCE \n(USD)'].sum()
        sumof_stage3 = df[df['IFRS_CLASSIFICATION'] == 'Stage 3']['OUTSTANDING BALANCE \n(USD)'].sum()
        sumof_contingent = contingent_exp['OUTSTANDING BALANCE \n(USD)'].sum()
        missed_repayments = df['UNPAID AMOUNT (USD)'].sum()
        mrr = (missed_repayments / sumof_direct) * 100
        ppl = (sumof_stage1 / sumof_direct) * 100
        wpl = (sumof_stage2 / sumof_direct) * 100
        npl = (sumof_stage3 / sumof_direct) * 100

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
            "missed_repayments": missed_repayments,
            "ppl": ppl,
            "wpl": wpl,
            "npl": npl,
            "fcy_direct": fcy_direct,
            "fcy_total": fcy_total,
            "mrr": mrr,
        }
        return result

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An error occurred: {str(e)}")



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
