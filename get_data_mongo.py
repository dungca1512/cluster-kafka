# mongodb_exporter.py
import pymongo
import json
import sys
import time
from datetime import datetime
from loguru import logger

# Cấu hình loguru với màu sắc
logger.remove()  # Xóa cấu hình mặc định
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
    colorize=True,  # Đảm bảo bật màu
    level="INFO"
)
logger.add(
    "mongodb_export.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
    rotation="10 MB",
    level="INFO"
)

def connect_to_mongodb(uri, db_name):
    """Kết nối đến MongoDB"""
    try:
        client = pymongo.MongoClient(uri)
        db = client[db_name]
        # Kiểm tra kết nối
        client.admin.command('ping')
        logger.info(f"Đã kết nối thành công đến MongoDB: {db_name}")
        return client, db
    except Exception as e:
        logger.error(f"Lỗi kết nối đến MongoDB: {str(e)}")
        sys.exit(1)

def export_batch(collection, query, batch_size, skip, output_file, append=False):
    """Xuất một batch dữ liệu từ MongoDB và lưu vào file"""
    try:
        start_time = time.time()
        # Truy vấn dữ liệu
        cursor = collection.find(query).skip(skip).limit(batch_size)
        
        # Chế độ ghi file (ghi mới hoặc append)
        mode = 'a' if append else 'w'
        count = 0
        
        with open(output_file, mode, encoding='utf-8') as f:
            # Nếu là file mới, mở array JSON
            if not append:
                f.write('[\n')
            else:
                # Nếu append, thêm dấu phẩy sau phần tử cuối cùng
                f.seek(0, 2)  # Di chuyển con trỏ đến cuối file
                pos = f.tell()  # Lấy vị trí hiện tại
                if pos > 2:  # Nếu file không rỗng và có dữ liệu
                    f.seek(pos - 2)  # Lùi lại 2 ký tự (\n])
                    f.write(',\n')  # Thay thế \n] bằng ,\n
                else:
                    f.write('[\n')  # Nếu file rỗng, bắt đầu array
            
            # Ghi từng document vào file
            for doc in cursor:
                count += 1
                # Chuyển ObjectId thành string
                doc['_id'] = str(doc['_id'])
                # Chuyển date thành string ISO format
                for key, value in doc.items():
                    if isinstance(value, datetime):
                        doc[key] = value.isoformat()
                
                # Ghi document vào file
                json_str = json.dumps(doc, ensure_ascii=False)
                if count < batch_size:
                    f.write(json_str + ',\n')
                else:
                    f.write(json_str + '\n')
            
            # Nếu là batch cuối, đóng array JSON
            if count < batch_size:
                f.write(']')
            
        elapsed_time = time.time() - start_time
        logger.info(f"Đã xuất {count:,} bản ghi trong {elapsed_time:.2f} giây ({count/elapsed_time:.2f} bản ghi/giây)")
        
        return count
    
    except Exception as e:
        logger.error(f"Lỗi khi xuất dữ liệu: {str(e)}")
        return 0

def export_collection(uri, db_name, collection_name, output_file, query=None, batch_size=10000):
    """Xuất toàn bộ collection theo từng batch"""
    if query is None:
        query = {}
    
    client, db = connect_to_mongodb(uri, db_name)
    collection = db[collection_name]
    
    try:
        # Đếm tổng số bản ghi
        total_records = collection.count_documents(query)
        logger.info(f"Tổng số bản ghi: {total_records:,}")
        
        # Xuất dữ liệu theo từng batch
        processed = 0
        start_time = time.time()
        
        while processed < total_records:
            batch_count = export_batch(collection, query, batch_size, processed, output_file, append=(processed > 0))
            processed += batch_count
            
            # Hiển thị tiến độ
            progress = (processed / total_records) * 100
            logger.info(f"Tiến độ: {processed:,}/{total_records:,} ({progress:.2f}%)")
            
            # Nếu không còn dữ liệu nào được xuất, thoát khỏi vòng lặp
            if batch_count == 0:
                break
        
        total_time = time.time() - start_time
        avg_speed = processed/total_time if total_time > 0 else 0
        logger.success(f"✅ Hoàn thành xuất {processed:,} bản ghi trong {total_time:.2f} giây ({avg_speed:.2f} bản ghi/giây)")
    
    except Exception as e:
        logger.exception(f"Lỗi: {str(e)}")
    finally:
        client.close()
        logger.info("Đã đóng kết nối MongoDB")

if __name__ == "__main__":
    # In thông tin banner với màu sắc
    logger.info("🚀 === MongoDB Collection Exporter ===")
    
    # Cấu hình mặc định
    uri = "mongodb://localhost:27017"
    db_name = "user_fcm"
    collection_name = "users_fcm"
    output_file = "output.json"
    batch_size = 10000
    
    # Đọc tham số từ command line
    if len(sys.argv) > 1:
        uri = sys.argv[1]
    if len(sys.argv) > 2:
        db_name = sys.argv[2]
    if len(sys.argv) > 3:
        collection_name = sys.argv[3]
    if len(sys.argv) > 4:
        output_file = sys.argv[4]
    if len(sys.argv) > 5:
        try:
            batch_size = int(sys.argv[5])
        except ValueError:
            logger.warning(f"⚠️ Kích thước batch không hợp lệ: {sys.argv[5]}, sử dụng giá trị mặc định: 10000")
    
    # In thông tin cấu hình
    logger.info("📋 Cấu hình:")
    logger.info(f"  • URI: {uri}")
    logger.info(f"  • Database: {db_name}")
    logger.info(f"  • Collection: {collection_name}")
    logger.info(f"  • Output file: {output_file}")
    logger.info(f"  • Batch size: {batch_size:,}")
    
    # Query (tùy chọn)
    query = {}  # Truy vấn tất cả document
    # Ví dụ: query = {"status": "active"} # Chỉ lấy document có status=active
    
    # Chạy export
    logger.info(f"🔄 Bắt đầu xuất dữ liệu từ {db_name}.{collection_name} sang {output_file}")
    export_collection(uri, db_name, collection_name, output_file, query, batch_size)