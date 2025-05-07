import boto3
import json
from dotenv import load_dotenv
import os

# Tải biến môi trường từ file .env
load_dotenv()

def is_number(value):
    """
    Kiểm tra xem một chuỗi có phải là số không
    """
    try:
        # Thử chuyển đổi thành float
        float_value = float(value)
        
        # Kiểm tra xem có phải là số nguyên không
        if float_value.is_integer():
            return int(float_value)
        return float_value
    except (ValueError, TypeError):
        # Nếu không chuyển đổi được, trả về giá trị ban đầu
        return value

def convert_string_to_number(obj):
    """
    Đệ quy chuyển đổi tất cả các giá trị chuỗi số thành kiểu số
    """
    if isinstance(obj, dict):
        # Xử lý từng cặp key-value trong dictionary
        return {key: convert_string_to_number(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        # Xử lý từng phần tử trong list
        return [convert_string_to_number(item) for item in obj]
    elif isinstance(obj, str):
        # Chuyển đổi chuỗi thành số nếu có thể
        return is_number(obj)
    else:
        # Giữ nguyên các kiểu dữ liệu khác
        return obj

def invoke_lambda_function():
    # Khởi tạo client Lambda
    lambda_client = boto3.client(
        'lambda',
        region_name=os.getenv('AWS_REGION'),  # Thay đổi theo region của bạn
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),      # Thay thế bằng key của bạn
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')   # Thay thế bằng secret key của bạn
    )
    
    # Gọi hàm Lambda mà không có payload
    response = lambda_client.invoke(
        FunctionName='logging_test',
        InvocationType='RequestResponse'  # Đồng bộ, đợi phản hồi
    )
    
    # Đọc và giải mã phản hồi
    response_payload = response['Payload'].read().decode('utf-8')
    
    # Chuyển đổi chuỗi JSON thành Python object
    data = json.loads(response_payload)
    
    # Chuyển đổi các chuỗi số thành kiểu số thực sự
    converted_data = convert_string_to_number(data)
    
    return converted_data

# Gọi hàm và lấy kết quả
data = invoke_lambda_function()

# Lưu dữ liệu đã chuyển đổi vào file
with open('data_test.json', 'w') as f:
    json.dump(data, f, indent=4, ensure_ascii=False)

print("Đã lưu dữ liệu vào file data_test.json")