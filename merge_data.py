import json
import pymongo
import os
import sys
from typing import List, Dict, Any, Optional, Union, Tuple
from loguru import logger

# Cấu hình logger
logger.remove()  # Xóa handler mặc định
logger.add(
    sys.stderr, 
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
    level="INFO"
)
logger.add(
    "merge_data.log", 
    rotation="10 MB", 
    level="INFO", 
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function} - {message}"
)

class MergeData:
    """
    Class để merge thông báo với device tokens từ MongoDB
    """
    def __init__(self, mongo_uri: str = "mongodb://localhost:27017", 
                 db_name: str = "user_fcm", 
                 collection_name: str = "users_fcm"):
        """
        Khởi tạo lớp MergeData
        
        Args:
            mongo_uri: URI kết nối MongoDB
            db_name: Tên database
            collection_name: Tên collection chứa device tokens
        """
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.client = None
        self.db = None
        self.collection = None
        
        logger.debug(f"Khởi tạo MergeData với MongoDB: {mongo_uri}, DB: {db_name}, Collection: {collection_name}")
    
    def connect_to_mongodb(self) -> bool:
        """
        Kết nối đến MongoDB
        
        Returns:
            bool: True nếu kết nối thành công, False nếu thất bại
        """
        try:
            if self.client is not None:
                # Đã kết nối trước đó, đóng kết nối cũ
                try:
                    self.client.close()
                except Exception:
                    pass
            
            # Kết nối mới
            self.client = pymongo.MongoClient(self.mongo_uri)
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            
            # Kiểm tra kết nối
            self.client.admin.command('ping')
            logger.debug(f"Đã kết nối thành công đến MongoDB: {self.db_name}.{self.collection_name}")
            return True
        
        except Exception as e:
            logger.error(f"Lỗi kết nối đến MongoDB: {str(e)}")
            return False
    
    def close_connection(self):
        """Đóng kết nối đến MongoDB"""
        if self.client is not None:
            try:
                self.client.close()
                logger.debug("Đã đóng kết nối MongoDB")
            except Exception as e:
                logger.error(f"Lỗi khi đóng kết nối MongoDB: {str(e)}")
    
    def get_devices_by_segment(self, segment: str, limit: int = 0, use_mock: bool = False) -> List[Dict[str, Any]]:
        """
        Lấy danh sách device tokens dựa vào segment
        
        Args:
            segment: Tên segment
            limit: Giới hạn số lượng device (0 = không giới hạn)
            use_mock: Sử dụng dữ liệu giả lập nếu không tìm thấy
            
        Returns:
            List[Dict[str, Any]]: Danh sách device tokens
        """
        try:
            # Kiểm tra kết nối
            if self.client is None:
                if not self.connect_to_mongodb():
                    if use_mock:
                        return self._generate_mock_data(segment)
                    return []
            
            # Tạo query
            query = {"segment": segment}
            
            # Đếm số lượng bản ghi
            count = self.collection.count_documents(query)
            logger.info(f"Tìm thấy {count} device tokens cho segment: {segment}")
            
            if count == 0 and use_mock:
                return self._generate_mock_data(segment)
            
            # Lấy danh sách device tokens
            devices = []
            cursor = self.collection.find(query)
            
            # Giới hạn số lượng nếu cần
            if limit > 0:
                cursor = cursor.limit(limit)
            
            for doc in cursor:
                # Lấy các trường cần thiết
                device = {
                    "user_id": str(doc.get("_id")) if "_id" in doc else None,
                    "firebaseToken": doc.get("firebaseToken")
                }
                
                # Thêm các trường khác nếu có
                for key, value in doc.items():
                    if key not in ["_id", "firebaseToken"]:
                        device[key] = value
                
                devices.append(device)
            
            logger.info(f"Đã lấy {len(devices)} device tokens cho segment: {segment}")
            return devices
        
        except Exception as e:
            logger.error(f"Lỗi khi lấy device tokens: {str(e)}")
            if use_mock:
                return self._generate_mock_data(segment)
            return []
    
    def _generate_mock_data(self, segment: str, count: int = 10) -> List[Dict[str, Any]]:
        """
        Tạo dữ liệu giả lập cho testing
        
        Args:
            segment: Tên segment
            count: Số lượng device tokens cần tạo
            
        Returns:
            List[Dict[str, Any]]: Danh sách device tokens giả lập
        """
        logger.warning(f"Đang tạo {count} device tokens giả lập cho segment: {segment}")
        return [
            {
                "user_id": f"mock_user_{i}",
                "firebaseToken": f"mock_token_{segment}_{i}",
                "segment": segment,
                "language": "en",
                "is_mock": True
            }
            for i in range(1, count + 1)
        ]
    
    def extract_notification_fields(self, notification_text: str) -> Dict[str, Any]:
        """
        Trích xuất các trường từ chuỗi notification_text
        
        Args:
            notification_text: Chuỗi chứa thông tin thông báo
            
        Returns:
            Dict[str, Any]: Các trường đã trích xuất
        """
        result = {}
        
        try:
            # Chia thành các dòng
            lines = notification_text.split('\n')
            
            for line in lines:
                # Bỏ qua các dòng trống
                if not line.strip():
                    continue
                
                # Tìm dấu ':' đầu tiên để phân tách key và value
                parts = line.split(':', 1)
                if len(parts) == 2:
                    key = parts[0].strip()
                    value = parts[1].strip()
                    result[key] = value
            
            logger.debug(f"Đã trích xuất {len(result)} trường từ notification_text")
            return result
        
        except Exception as e:
            logger.error(f"Lỗi khi trích xuất trường từ notification_text: {str(e)}")
            return result
    
    def merge_notifications_with_devices(self, 
                                        notifications: List[Dict[str, Any]], 
                                        segment_field: str = "segment",
                                        country_field: str = "quốc gia",
                                        notification_field: str = "notification"
                                        ) -> List[Dict[str, Any]]:
        """
        Merge thông báo với device tokens
        
        Args:
            notifications: Danh sách thông báo
            segment_field: Tên trường chứa thông tin segment
            country_field: Tên trường chứa mã quốc gia
            notification_field: Tên trường chứa nội dung thông báo
            
        Returns:
            List[Dict[str, Any]]: Danh sách thông báo đã merge với device tokens
        """
        if not notifications:
            logger.warning("Không có thông báo nào để merge")
            return []
        
        try:
            # Kết nối đến MongoDB
            if not self.connect_to_mongodb():
                logger.error("Không thể kết nối đến MongoDB")
                return []
            
            merged_events = []
            
            # Xử lý từng thông báo
            for idx, notification in enumerate(notifications):
                logger.info(f"Đang xử lý thông báo #{idx+1}/{len(notifications)}")
                
                # Lấy thông tin segment
                segment = notification.get(segment_field)
                if not segment:
                    logger.warning(f"Thông báo #{idx+1} không có segment")
                    continue
                
                # Lấy mã quốc gia (mặc định là 'en')
                country_code = notification.get(country_field, 'en')
                
                # Lấy nội dung thông báo
                notification_text = notification.get(notification_field, '')
                if not notification_text:
                    logger.warning(f"Thông báo #{idx+1} không có nội dung")
                    continue
                
                # Trích xuất các trường từ notification_text
                notification_fields = self.extract_notification_fields(notification_text)
                
                # Lấy danh sách device tokens cho segment
                devices = self.get_devices_by_segment(segment, use_mock=True)
                if not devices:
                    logger.warning(f"Không tìm thấy device tokens nào cho segment: {segment}")
                    continue
                
                logger.info(f"Đang merge thông báo với {len(devices)} device tokens")
                
                # Tạo thông báo cho từng device
                for device in devices:
                    event = {
                        "country_code": country_code,
                        "segment": segment,
                        "user_id": device.get("user_id"),
                        "fcm_token": device.get("firebaseToken"),
                        "is_mock": device.get("is_mock", False)
                    }
                    
                    # Thêm các trường từ notification_fields
                    for key, value in notification_fields.items():
                        event[key] = value
                    
                    merged_events.append(event)
                
                logger.info(f"Đã tạo {len(devices)} sự kiện cho thông báo #{idx+1}")
            
            logger.success(f"Đã merge {len(merged_events)} sự kiện từ {len(notifications)} thông báo")
            return merged_events
        
        except Exception as e:
            logger.error(f"Lỗi khi merge thông báo với device tokens: {str(e)}")
            return []
        
        finally:
            # Đóng kết nối MongoDB
            self.close_connection()
    
    def process_notifications(self, 
                             data: Union[str, Dict[str, Any], List[Dict[str, Any]]],
                             extract_from_data_field: bool = True
                             ) -> List[Dict[str, Any]]:
        """
        Xử lý dữ liệu thông báo và merge với device tokens
        
        Args:
            data: Dữ liệu thông báo (có thể là chuỗi JSON, dict hoặc list)
            extract_from_data_field: Trích xuất từ trường 'data' nếu có
            
        Returns:
            List[Dict[str, Any]]: Danh sách thông báo đã merge với device tokens
        """
        try:
            # Chuyển đổi dữ liệu thành notifications
            notifications = []
            
            # Xử lý dữ liệu dạng chuỗi
            if isinstance(data, str):
                try:
                    data_dict = json.loads(data)
                    if extract_from_data_field and isinstance(data_dict, dict) and 'data' in data_dict:
                        data_dict = data_dict['data']
                    
                    if isinstance(data_dict, list):
                        notifications = data_dict
                    elif isinstance(data_dict, dict):
                        notifications = [data_dict]
                    else:
                        logger.error(f"Định dạng dữ liệu không hợp lệ: {type(data_dict)}")
                        return []
                except json.JSONDecodeError as e:
                    logger.error(f"Lỗi parse JSON: {str(e)}")
                    return []
            
            # Xử lý dữ liệu dạng dict
            elif isinstance(data, dict):
                if extract_from_data_field and 'data' in data:
                    data_field = data['data']
                    
                    # Kiểm tra nếu data là chuỗi
                    if isinstance(data_field, str):
                        try:
                            data_parsed = json.loads(data_field)
                            if isinstance(data_parsed, list):
                                notifications = data_parsed
                            else:
                                notifications = [data_parsed]
                        except json.JSONDecodeError:
                            # Nếu không parse được, coi như một thông báo đơn lẻ
                            notifications = [data]
                    elif isinstance(data_field, list):
                        notifications = data_field
                    else:
                        notifications = [data_field]
                else:
                    notifications = [data]
            
            # Xử lý dữ liệu dạng list
            elif isinstance(data, list):
                notifications = data
            
            else:
                logger.error(f"Kiểu dữ liệu không được hỗ trợ: {type(data)}")
                return []
            
            logger.info(f"Đã parse được {len(notifications)} thông báo")
            
            # Merge với device tokens
            return self.merge_notifications_with_devices(notifications)
        
        except Exception as e:
            logger.error(f"Lỗi khi xử lý thông báo: {str(e)}")
            return []


# Utility functions - Có thể gọi trực tiếp mà không cần tạo instance

def merge_json_with_mongodb(mongo_uri: str, db_name: str, collection_name: str, 
                           data: Any, extract_from_data_field: bool = True) -> List[Dict[str, Any]]:
    """
    Hàm tiện ích để merge dữ liệu JSON với device tokens từ MongoDB
    
    Args:
        mongo_uri: URI kết nối MongoDB
        db_name: Tên database
        collection_name: Tên collection
        data: Dữ liệu cần merge
        extract_from_data_field: Trích xuất từ trường 'data' nếu có
        
    Returns:
        List[Dict[str, Any]]: Danh sách đã merge
    """
    merger = MergeData(mongo_uri, db_name, collection_name)
    return merger.process_notifications(data, extract_from_data_field)

def extract_notification_fields_simple(notification_text: str) -> Dict[str, Any]:
    """
    Hàm đơn giản để trích xuất các trường từ chuỗi notification_text
    
    Args:
        notification_text: Chuỗi chứa thông tin thông báo
        
    Returns:
        Dict[str, Any]: Các trường đã trích xuất
    """
    result = {}
    
    try:
        lines = notification_text.split('\n')
        
        for line in lines:
            if ':' in line:
                parts = line.split(':', 1)
                key = parts[0].strip()
                value = parts[1].strip()
                result[key] = value
        
        return result
    
    except Exception as e:
        logger.error(f"Lỗi khi trích xuất trường: {str(e)}")
        return {}

def generate_mock_device_tokens(segment: str, count: int = 10) -> List[Dict[str, Any]]:
    """
    Tạo dữ liệu giả lập các device tokens
    
    Args:
        segment: Tên segment
        count: Số lượng device tokens
        
    Returns:
        List[Dict[str, Any]]: Danh sách device tokens giả lập
    """
    return [
        {
            "user_id": f"mock_user_{i}",
            "firebaseToken": f"mock_token_{segment}_{i}",
            "segment": segment,
            "language": "en",
            "is_mock": True
        }
        for i in range(1, count + 1)
    ]


# For testing
if __name__ == "__main__":
    # Cấu hình logger cho test
    logger.remove()
    logger.add(
        lambda msg: print(msg),
        colorize=True,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO"
    )
    
    # Dữ liệu test
    test_data = {
        "data": """return [{"quốc gia": "en","notification": "messageTitle: 🎉 Refresh and conquer\\nmessageText: 🎁 80% off this week \\ 🌟 Don't miss out!\\nimageUrl: http\\nexpiryTime: 64800\\nanalyticsLabel: Motivation","segment": "Inactive_users_6d"},
        {"quốc gia": "th","notification": "messageTitle: การแจ้งเตือน 10:\\nmessageText: 🎉 เรียนรู้ใหม่และชนะ \\ 🎁 ส่วนลด 80% ในสัปดาห์นี้ \\ 🌟 ห้ามพลาดนะ!\\nimageUrl: http2\\nexpiryTime: 64800\\nanalyticsLabel: Motivation","segment": "Inactive_users_6d"}]"""
    }
    
    # Sử dụng class
    logger.info("=== Test sử dụng class MergeData ===")
    merger = MergeData()
    events = merger.process_notifications(test_data)
    logger.info(f"Đã tạo {len(events)} sự kiện")
    
    # Sử dụng hàm tiện ích
    logger.info("\n=== Test sử dụng hàm tiện ích ===")
    events2 = merge_json_with_mongodb("mongodb://localhost:27017", "user_fcm", "users_fcm", test_data)
    logger.info(f"Đã tạo {len(events2)} sự kiện")
    
    # In một vài sự kiện mẫu
    if events:
        logger.info("\n=== Sự kiện mẫu ===")
        for i in range(min(3, len(events))):
            logger.info(f"Sự kiện #{i+1}: {events[i]}")