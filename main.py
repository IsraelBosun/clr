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

@app.post("/angola")
async def process_angola_file(file: UploadFile = File(...)):
    try:
        # Read the uploaded Excel file into a pandas DataFrame
        content = await file.read()
        df = pd.read_excel(BytesIO(content), sheet_name='CLR', skiprows=2)

        # Perform the aggregation
        pd.options.display.float_format = '{:,.2f}'.format
        aggregateed_data = df.groupby('CUSTOMER_NAME')[['SECTOR', 'FACILITY_TYPE', 'APPROVED AMOUNT (USD)', 'OUTSTANDING BALANCE \n(USD)', 'IFRS_CLASSIFICATION', 'PRUDENTIAL_CLASSIFICATION']].sum().reset_index()
        top5_customers = aggregateed_data.sort_values(by='OUTSTANDING BALANCE \n(USD)', ascending=False).head(5)

        # Convert the DataFrame to a dictionary for JSON response
        result = top5_customers.to_dict(orient='records')
        return {"top5_customers": result}

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An error occurred: {str(e)}")



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
        pd.options.display.float_format = '{:,.2f}'.format
        aggregated_data = df.groupby('CUSTOMER NAME')[['SECTOR', 'APPROVED TOTAL FACILITY AMOUNT/LIMIT', 'TOTAL EXPOSURES(USD)','IFRS', 'CLASSIFICATION' ]].sum().reset_index()
        top5_customers = aggregated_data.sort_values(by='TOTAL EXPOSURES(USD)', ascending=False).head(5)

        # Calculate the FCY and percentages
        ccy = df[df['CURRENCY TYPE'] == 'FCY']
        fcy_direct = ccy['TOTAL DIRECT EXPOSURES(USD)'].sum()
        fcy_total = ccy['TOTAL EXPOSURES(USD)'].sum()
        sumof_direct = df['TOTAL DIRECT EXPOSURES(USD)'].sum()
        sumof_all = df['TOTAL EXPOSURES(USD)'].sum()
        fcy_direct_percentage = (fcy_direct / sumof_direct) * 100
        fcy_total_percentage = (fcy_total / sumof_all) * 100

        # Calculate the stages
        sumof_stage1 = df[df['IFRS'] == 'STAGE 1']['TOTAL EXPOSURES(USD)'].sum()
        sumof_stage2 = df[df['IFRS'] == 'STAGE 2']['TOTAL EXPOSURES(USD)'].sum()
        sumof_stage3 = df[df['IFRS'] == 'STAGE 3']['TOTAL EXPOSURES(USD)'].sum()
        sumof_contingent = df['TOTAL CONTINGENT EXPOSURES(USD)'].sum()
        missed_repayments = df['MISSED INSTALLMENT'].sum() / 129
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
