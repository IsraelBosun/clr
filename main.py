from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from botswana import process_Botswana_file_logic
from ghana import process_Ghana_file_logic
from angola import process_Angola_file_logic
from rwanda import process_Rwanda_file_logic
from southAfrica import process_SouthAfrica_file_logic
from congo import process_Congo_file_logic
from gambia import process_Gambia_file_logic
from guinea import process_Guinea_file_logic
from mozambique import process_Mozambique_file_logic
from tanzania import process_Tanzania_file_logic
from cameroon import process_Cameroon_file_logic
from zambia import process_Zambia_file_logic
from kenya import process_Kenya_file_logic
from sierraLeone import process_SierraLeone_file_logic
import pandas as pd
from io import BytesIO
import traceback


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

@app.post("/kenya")
async def process_Kenya_file(file: UploadFile = File(...)):
    try:
        result = await process_Kenya_file_logic(file)
        return result
    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        print(traceback.format_exc())  # This will print the full stack trace
        raise HTTPException(status_code=500, detail=error_message)
  


@app.post("/botswana")
async def process_Botswana_file(file: UploadFile = File(...)):
    try:
        result = await process_Botswana_file_logic(file)
        return result
    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        print(traceback.format_exc())  # This will print the full stack trace
        raise HTTPException(status_code=500, detail=error_message)
    

@app.post("/ghana")
async def process_Ghana_file(file: UploadFile = File(...)):
    try:
        result = await process_Ghana_file_logic(file)
        return result

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        print(traceback.format_exc())  # This will print the full stack trace
        raise HTTPException(status_code=500, detail=error_message)
        
@app.post("/angola")
async def process_Angola_file(file: UploadFile = File(...)):
    try:
        result = await process_Angola_file_logic(file)
        return result

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        print(traceback.format_exc())  # This will print the full stack trace
        raise HTTPException(status_code=500, detail=error_message)


@app.post("/rwanda")
async def process_Rwanda_file(file: UploadFile = File(...)):
    try:
        result = await process_Rwanda_file_logic(file)
        return result

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        print(traceback.format_exc())  # This will print the full stack trace
        raise HTTPException(status_code=500, detail=error_message)


@app.post("/southAfrica")
async def process_SouthAfrica_file(file: UploadFile = File(...)):
    try:
        result = await process_SouthAfrica_file_logic(file)
        return result

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        print(traceback.format_exc())  # This will print the full stack trace
        raise HTTPException(status_code=500, detail=error_message)


@app.post("/congo")
async def process_Congo_file(file: UploadFile = File(...)):
    try:
        result = await process_Congo_file_logic(file)
        return result

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        print(traceback.format_exc())  # This will print the full stack trace
        raise HTTPException(status_code=500, detail=error_message)


@app.post("/gambia")
async def process_Gambia_file(file: UploadFile = File(...)):
    try:
        result = await process_Gambia_file_logic(file)
        return result

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        print(traceback.format_exc())  # This will print the full stack trace
        raise HTTPException(status_code=500, detail=error_message)

@app.post("/guinea")
async def process_Guinea_file(file: UploadFile = File(...)):
    try:
        result = await process_Guinea_file_logic(file)
        return result

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        print(traceback.format_exc())  # This will print the full stack trace
        raise HTTPException(status_code=500, detail=error_message)

@app.post("/mozambique")
async def process_Mozambique_file(file: UploadFile = File(...)):
    try:
        result = await process_Mozambique_file_logic(file)
        return result
    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        print(traceback.format_exc())  # This will print the full stack trace
        raise HTTPException(status_code=500, detail=error_message)

@app.post("/tanzania")
async def process_Tanzania_file(file: UploadFile = File(...)):
    try:
        result = await process_Tanzania_file_logic(file)
        return result
    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        print(traceback.format_exc())  # This will print the full stack trace
        raise HTTPException(status_code=500, detail=error_message)

@app.post("/cameroon")
async def process_Cameroon_file(file: UploadFile = File(...)):
    try:
        result = await process_Cameroon_file_logic(file)
        return result
    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        print(traceback.format_exc())  # This will print the full stack trace
        raise HTTPException(status_code=500, detail=error_message)


@app.post("/zambia")
async def process_Zambia_file(file: UploadFile = File(...)):
    try:
        result = await process_Zambia_file_logic(file)
        return result
    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        print(traceback.format_exc())  # This will print the full stack trace
        raise HTTPException(status_code=500, detail=error_message)

@app.post("/sierraLeone")
async def process_Sierra_Leone_file(file: UploadFile = File(...)):
    try:
        result = await process_SierraLeone_file_logic(file)
        return result
    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        print(traceback.format_exc())  # This will print the full stack trace
        raise HTTPException(status_code=500, detail=error_message)



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
