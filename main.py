from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from io import BytesIO
import traceback


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
    
        # Aggregation for top 5 customers
        aggregated_data = df.groupby('CUSTOMER_NAME')[['SECTOR', 'FACILITY_TYPE', 'APPROVED AMOUNT (USD)', 'CURRENT EXPOSURE (USD)', 'CLASSIFICATION', 'IFRS_CLASSIFICATION' ]].sum().reset_index()
        top5_customers = aggregated_data.sort_values(by='CURRENT EXPOSURE (USD)', ascending=False).head(5)

    
        # Calculate sums and percentages
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
    
        # Aggregated data for missed repayments
        aggregateed_missed = df.groupby('CUSTOMER_NAME')[['SECTOR', 'APPROVED AMOUNT (USD)', 'CURRENT EXPOSURE (USD)', 'UNPAID AMOUNT (USD)']].sum().reset_index()
        missed_repayments_data = aggregateed_missed.sort_values(by='UNPAID AMOUNT (USD)', ascending=False).head(20)
    
        # Aggregated data for Stage 2
        stage2_df = df[df['IFRS_CLASSIFICATION'] == 2]
        stage2_grouped = stage2_df.groupby('CUSTOMER_NAME')[['APPROVED AMOUNT (USD)', 'CURRENT EXPOSURE (USD)']].sum().reset_index()
        stage2_sorted = stage2_grouped.sort_values(by='CURRENT EXPOSURE (USD)', ascending=False)
        top_20_stage2 = stage2_sorted.head(20)
    
        # Aggregated data by sector
        aggregateed_sector = df.groupby('SECTOR')[['APPROVED AMOUNT (USD)', 'CURRENT EXPOSURE (USD)']].sum().reset_index()
        sector_data = aggregateed_sector.sort_values(by='CURRENT EXPOSURE (USD)', ascending=False)
    
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
            error_message = f"An error occurred: {str(e)}"
            print(error_message)
            print(traceback.format_exc())  # This will print the full stack trace
            raise HTTPException(status_code=500, detail=error_message)
    

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
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        print(traceback.format_exc())  # This will print the full stack trace
        raise HTTPException(status_code=500, detail=error_message)
        
@app.post("/angola")
async def process_Angola_file(file: UploadFile = File(...)):
    try:
        # Read the uploaded Excel file into a pandas DataFrame
        content = await file.read()
        df = pd.read_excel(BytesIO(content), sheet_name='CLR', skiprows=2)

    
        # Aggregation for top 5 customers
        aggregateed_data = df.groupby('CUSTOMER_NAME')[['SECTOR', 'FACILITY_TYPE', 'APPROVED AMOUNT (USD)', 'OUTSTANDING BALANCE \n(USD)', 'IFRS_CLASSIFICATION', 'PRUDENTIAL_CLASSIFICATION']].sum().reset_index()
        top5_customers = aggregateed_data.sort_values(by='OUTSTANDING BALANCE \n(USD)', ascending=False).head(5)
    
        # Calculate sums and percentages
        ccy = df[df['CURRENCY_TYPE'] == 'FCY']
        direct_exp = df[df['EXPOSURE_TYPE'] == 'DIRECT']
        contingent_exp = df[df['EXPOSURE_TYPE'] == 'CONTINGENT']
        missed_repayments = df['UNPAID AMOUNT (USD)'].sum()
        sumof_all = df['OUTSTANDING BALANCE \n(USD)'].sum()
        fcy_total = ccy['OUTSTANDING BALANCE \n(USD)'].sum()
        fcy_direct = direct_exp[direct_exp['CURRENCY_TYPE'] == 'FCY']['OUTSTANDING BALANCE \n(USD)'].sum()
        sumof_direct = direct_exp['OUTSTANDING BALANCE \n(USD)'].sum()
        sumof_contingent = contingent_exp['OUTSTANDING BALANCE \n(USD)'].sum()
        sumof_stage1 = direct_exp[direct_exp['IFRS_CLASSIFICATION'] == 'Stage 1']['OUTSTANDING BALANCE \n(USD)'].sum()
        sumof_stage2 = direct_exp[direct_exp['IFRS_CLASSIFICATION'] == 'Stage 2']['OUTSTANDING BALANCE \n(USD)'].sum()
        sumof_stage3 = direct_exp[direct_exp['IFRS_CLASSIFICATION'] == 'Stage 3']['OUTSTANDING BALANCE \n(USD)'].sum()
    
        fcy_direct_percentage = (fcy_direct / sumof_direct) * 100
        fcy_total_percentage = (fcy_total / sumof_all) * 100
        sumof_top5 = top5_customers['OUTSTANDING BALANCE \n(USD)'].sum()
        percentageof_top5 = (sumof_top5 / sumof_all) * 100
        ppl = (sumof_stage1 / sumof_direct) * 100
        wpl = (sumof_stage2 / sumof_direct) * 100
        npl = (sumof_stage3 / sumof_direct) * 100
        mrr = (missed_repayments / sumof_direct) * 100
    
        # Aggregated data for missed repayments
        aggregateed_missed = df.groupby('CUSTOMER_NAME')[['SECTOR', 'APPROVED AMOUNT (USD)', 'OUTSTANDING BALANCE \n(USD)', 'UNPAID AMOUNT (USD)']].sum().reset_index()
        missed_repayments_data = aggregateed_missed.sort_values(by='UNPAID AMOUNT (USD)', ascending=False).head(20)
    
        # Aggregated data for Stage 2
        stage2_df = df[df['IFRS_CLASSIFICATION'] == 'Stage 2']
        stage2_grouped = stage2_df.groupby('CUSTOMER_NAME')[['APPROVED AMOUNT (USD)', 'OUTSTANDING BALANCE \n(USD)']].sum().reset_index()
        stage2_sorted = stage2_grouped.sort_values(by='OUTSTANDING BALANCE \n(USD)', ascending=False)
        top_20_stage2 = stage2_sorted.head(20)
    
        # Aggregated data by sector
        aggregateed_sector = df.groupby('SECTOR')[['APPROVED AMOUNT (USD)', 'OUTSTANDING BALANCE \n(USD)']].sum().reset_index()
        sector_data = aggregateed_sector.sort_values(by='OUTSTANDING BALANCE \n(USD)', ascending=False)
    
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



@app.post("/rwanda")
async def process_Rwanda_file(file: UploadFile = File(...)):
    try:
        # Read the uploaded Excel file into a pandas DataFrame
        content = await file.read()
        df = pd.read_excel(BytesIO(content), sheet_name='CLR', skiprows = 5)

        df['APPROVED AMOUNT'] = pd.to_numeric(df['APPROVED AMOUNT'], errors='coerce') * 1000
        df['OUTSTANDING BALANCE'] = pd.to_numeric(df['OUTSTANDING BALANCE'], errors='coerce') * 1000
        df['UNPAID AMOUNT'] = pd.to_numeric(df['UNPAID AMOUNT'], errors = 'coerce') * 1000

        # Aggregation for top 5 customers
        aggregateed_data = df.groupby('CUSTOMER_NAME')[['SECTOR', 'FACILITY_TYPE', 'APPROVED AMOUNT', 'OUTSTANDING BALANCE', 'IFRS_CLASSIFICATION', 'PRUDENTIAL_CLASSIFICATION']].sum().reset_index()
        top5_customers = aggregateed_data.sort_values(by='OUTSTANDING BALANCE', ascending=False).head(5)

        # Calculate sums and percentages
        ccy = df[df['CURRENCY_TYPE'] == 'FCY']
        direct_exp = df[df['EXPOSURE_TYPE'] == 'DIRECT']
        contingent_exp = df[df['EXPOSURE_TYPE'] == 'CONTINGENT']
        missed_repayments = df['UNPAID AMOUNT'].sum()
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
        aggregateed_missed = df.groupby('CUSTOMER_NAME')[['SECTOR', 'APPROVED AMOUNT', 'OUTSTANDING BALANCE', 'UNPAID AMOUNT']].sum().reset_index()
        missed_repayments_data = aggregateed_missed.sort_values(by='UNPAID AMOUNT', ascending=False).head(20)

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

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        print(traceback.format_exc())  # This will print the full stack trace
        raise HTTPException(status_code=500, detail=error_message)


@app.post("/southAfrica")
async def process_SouthAfrica_file(file: UploadFile = File(...)):
    try:
        # Read the uploaded Excel file into a pandas DataFrame
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

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        print(traceback.format_exc())  # This will print the full stack trace
        raise HTTPException(status_code=500, detail=error_message)

@app.post("/congo")
async def process_Congo_file(file: UploadFile = File(...)):
    try:
        # Read the uploaded Excel file into a pandas DataFrame
        content = await file.read()
        df = pd.read_excel(BytesIO(content), sheet_name='CLR', skiprows = 7)

        df['TOTAL UNPAID'] = pd.to_numeric(df['TOTAL UNPAID'], errors='coerce')

        # Aggregation for top 5 customers
        aggregateed_data = df.groupby('CUSTOMER_NAME')[['SECTOR ', 'FACILITY  TYPE', 'APPROVED AMOUNT', 'TOTAL OUSTANDING AMOUNT', 'IFRS  CLASSIFICATION', 'BANK CLASSIFICATION  AS AT TODAY ']].sum().reset_index()
        top5_customers = aggregateed_data.sort_values(by='TOTAL OUSTANDING AMOUNT', ascending=False).head(5)

        # Calculate sums and percentages
        ccy = df[df['CURRENCY  TYPE'] == 'FCY']
        direct_exp = df[df['EXPOSURE  TYPE'] == 'DIRECT']
        contingent_exp = df[df['EXPOSURE  TYPE'] == 'CONTINGENT']
        missed_repayments = df['TOTAL UNPAID'].sum()
        sumof_all = df['TOTAL OUSTANDING AMOUNT'].sum()
        fcy_total = ccy['TOTAL OUSTANDING AMOUNT'].sum()
        fcy_direct = direct_exp[direct_exp['CURRENCY  TYPE'] == 'FCY']['TOTAL OUSTANDING AMOUNT'].sum()
        sumof_direct = direct_exp['TOTAL OUSTANDING AMOUNT'].sum()
        sumof_contingent = contingent_exp['TOTAL OUSTANDING AMOUNT'].sum()
        sumof_stage1 = direct_exp[direct_exp['IFRS  CLASSIFICATION'] == 'STAGE 1']['TOTAL OUSTANDING AMOUNT'].sum()
        sumof_stage2 = direct_exp[direct_exp['IFRS  CLASSIFICATION'] == 'STAGE 2']['TOTAL OUSTANDING AMOUNT'].sum()
        sumof_stage3 = direct_exp[direct_exp['IFRS  CLASSIFICATION'] == 'STAGE 3']['TOTAL OUSTANDING AMOUNT'].sum()

        fcy_direct_percentage = (fcy_direct / sumof_direct) * 100
        fcy_total_percentage = (fcy_total / sumof_all) * 100
        sumof_top5 = top5_customers['TOTAL OUSTANDING AMOUNT'].sum()
        percentageof_top5 = (sumof_top5 / sumof_all) * 100
        ppl = (sumof_stage1 / sumof_direct) * 100
        wpl = (sumof_stage2 / sumof_direct) * 100
        npl = (sumof_stage3 / sumof_direct) * 100
        mrr = (missed_repayments / sumof_direct) * 100

        # Aggregated data for missed repayments
        aggregateed_missed = df.groupby('CUSTOMER_NAME')[['SECTOR ', 'APPROVED AMOUNT', 'TOTAL OUSTANDING AMOUNT', 'TOTAL UNPAID']].sum().reset_index()
        missed_repayments_data = aggregateed_missed.sort_values(by='TOTAL UNPAID', ascending=False).head(20)

        # Aggregated data for Stage 2
        stage2_df = df[df['IFRS  CLASSIFICATION'] == 'STAGE 2']
        stage2_grouped = stage2_df.groupby('CUSTOMER_NAME')[['APPROVED AMOUNT', 'TOTAL OUSTANDING AMOUNT']].sum().reset_index()
        stage2_sorted = stage2_grouped.sort_values(by='TOTAL OUSTANDING AMOUNT', ascending=False)
        top_20_stage2 = stage2_sorted.head(20)

        # Aggregated data by sector
        aggregateed_sector = df.groupby('SECTOR ')[['APPROVED AMOUNT', 'TOTAL OUSTANDING AMOUNT']].sum().reset_index()
        sector_data = aggregateed_sector.sort_values(by='TOTAL OUSTANDING AMOUNT', ascending=False)

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
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        print(traceback.format_exc())  # This will print the full stack trace
        raise HTTPException(status_code=500, detail=error_message)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
