import json
import re
import os


def extract_json_from_string(input_string):
    """
    Extracts and parses JSON from a string that starts with 'return'
    
    Args:
        input_string (str): The string containing 'return' followed by JSON
        
    Returns:
        dict/list: The parsed JSON data
    """
    # Remove the 'return ' prefix to get only the JSON part
    if input_string.strip().startswith('return '):
        json_string = input_string.replace('return ', '', 1)
    else:
        json_string = input_string
    
    # Fix escaping issues for proper JSON parsing
    # Replace single backslashes with double backslashes, except for already escaped newlines
    fixed_json_string = re.sub(r'\\(?!n)', r'\\\\', json_string)
    
    try:
        # Parse the fixed JSON string
        return json.loads(fixed_json_string)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        raise


def read_json_file(file_path):
    """
    Reads a JSON file and returns the parsed data
    
    Args:
        file_path (str): Path to the JSON file
        
    Returns:
        dict/list: The parsed JSON data
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
            
        # Process the content
        return extract_json_from_string(content)
    except Exception as e:
        print(f"Error reading JSON file {file_path}: {e}")
        raise