from fastapi import FastAPI, File, UploadFile, HTTPException
import pandas as pd
from io import BytesIO

app = FastAPI()

@app.post("/angola/")
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
