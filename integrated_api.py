"""
integrated_api.py - FastAPI application that receives data from n8n and processes it directly
"""

from fastapi import FastAPI, Request
import json
import re
from datetime import datetime

app = FastAPI()

def extract_json_from_string(input_string):
    """
    Extracts and parses JSON from a string that starts with 'return'
    
    Args:
        input_string (str): The string containing 'return' followed by JSON
        
    Returns:
        dict/list: The parsed JSON data
    """
    # Handle string or dict input
    if isinstance(input_string, dict) or isinstance(input_string, list):
        return input_string
        
    # Remove the 'return ' prefix to get only the JSON part
    if isinstance(input_string, str) and input_string.strip().startswith('return '):
        json_string = input_string.replace('return ', '', 1)
    else:
        json_string = input_string
    
    # Fix escaping issues for proper JSON parsing
    # Replace single backslashes with double backslashes, except for already escaped newlines
    if isinstance(json_string, str):
        fixed_json_string = re.sub(r'\\(?!n)', r'\\\\', json_string)
        
        try:
            # Parse the fixed JSON string
            return json.loads(fixed_json_string)
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}")
            raise
    return json_string

@app.post("/receive-n8n-data")
async def receive_n8n_data(request: Request):
    # Get the raw data from the request
    data = await request.json()
    
    # Process the data directly without saving to a file
    processed_data = extract_json_from_string(data)
    
    # Here you can perform additional processing on the data
    # For example, transform the data, filter it, or send it to another service
    
    # Log the processing time for debugging
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Return the processed data and processing info
    return {
        "status": "processed",
        "timestamp": timestamp,
        "processed_data": processed_data
    }

@app.post("/process-json-string")
async def process_json_string(request: Request):
    # Get the request body
    body = await request.json()
    
    # Extract the JSON string from the request body
    if "json_string" not in body:
        return {"error": "Missing 'json_string' field in request body"}
    
    json_string = body["json_string"]
    
    # Process the JSON string
    try:
        processed_data = extract_json_from_string(json_string)
        return {
            "status": "success",
            "processed_data": processed_data
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

if __name__ == "__main__":
    import uvicorn
    # Run the server with uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9000)