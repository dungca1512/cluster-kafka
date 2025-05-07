"""
api_receiver.py - Module nhận dữ liệu từ API của n8n
"""

from fastapi import FastAPI, Request
import json
from typing import Dict, Any, Callable, Optional
import uvicorn
from loguru import logger
import sys

# Cấu hình logging
logger.remove()
logger.add(
    sys.stdout,
    colorize=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
    level="INFO",
    serialize=False  # Không serialize log message
)

class ApiReceiver:
    """
    Class để nhận dữ liệu từ API của n8n
    """
    def __init__(self, host: str = "0.0.0.0", port: int = 9000):
        """
        Khởi tạo ApiReceiver
        
        Args:
            host: Host để lắng nghe
            port: Port để lắng nghe
        """
        self.host = host
        self.port = port
        self.app = FastAPI()
        self.data_handler = None
        
        # Đăng ký endpoint
        @self.app.post("/receive-n8n-data")
        async def receive_n8n_data(request: Request):
            try:
                # Đọc dữ liệu từ request
                data = await request.json()
                logger.info("Đã nhận dữ liệu từ n8n API")
                
                # Log dữ liệu đã giải mã
                try:
                    if isinstance(data, dict):
                        # Giới hạn độ dài log và đảm bảo hiển thị Unicode
                        data_str = json.dumps(data, ensure_ascii=False, indent=None)[:300]
                        logger.info(f"Đã nhận dữ liệu: {data_str}...")
                    else:
                        logger.info(f"Đã nhận dữ liệu: {str(data)[:300]}...")
                except Exception as e:
                    logger.warning(f"Không thể log dữ liệu: {str(e)}")
                
                # Gọi handler nếu được đăng ký
                result = None
                if self.data_handler:
                    try:
                        result = self.data_handler(data)
                        logger.info("Đã xử lý dữ liệu thành công")
                    except Exception as e:
                        logger.error(f"Lỗi khi xử lý dữ liệu: {str(e)}")
                        return {
                            "status": "error",
                            "message": "Lỗi khi xử lý dữ liệu",
                            "error": str(e)
                        }
                
                # Trả về kết quả
                if result:
                    return result
                else:
                    return {
                        "status": "received",
                        "message": "Đã nhận dữ liệu thành công"
                    }
            
            except Exception as e:
                logger.error(f"Lỗi khi nhận dữ liệu: {str(e)}")
                return {
                    "status": "error",
                    "message": "Lỗi khi nhận dữ liệu",
                    "error": str(e)
                }
    
    def register_handler(self, handler: Callable[[Dict[str, Any]], Dict[str, Any]]):
        """
        Đăng ký hàm xử lý dữ liệu
        
        Args:
            handler: Hàm xử lý dữ liệu (nhận vào data từ n8n, trả về kết quả)
        """
        self.data_handler = handler
        logger.info("Đã đăng ký handler xử lý dữ liệu")
    
    def start(self):
        """Khởi động API server"""
        logger.info(f"Khởi động API server tại http://{self.host}:{self.port}")
        uvicorn.run(self.app, host=self.host, port=self.port)


# Hàm tiện ích để nhanh chóng tạo server nhận dữ liệu
def create_receiver(host: str = "0.0.0.0", port: int = 9000, 
                   handler: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None):
    """
    Tạo và khởi động API receiver
    
    Args:
        host: Host để lắng nghe
        port: Port để lắng nghe
        handler: Hàm xử lý dữ liệu (không bắt buộc)
        
    Returns:
        ApiReceiver: Đối tượng ApiReceiver đã được cấu hình
    """
    receiver = ApiReceiver(host, port)
    
    if handler:
        receiver.register_handler(handler)
    
    return receiver


# Sử dụng trực tiếp
if __name__ == "__main__":
    # Hàm xử lý mẫu
    def sample_handler(data):
        # Hiển thị dữ liệu với Unicode
        data_preview = json.dumps(data, ensure_ascii=False, indent=2)[:500]
        logger.info(f"Xử lý dữ liệu:\n{data_preview}...")
        
        # Lấy thông tin chi tiết nếu có
        if isinstance(data, dict):
            data_type = "Dictionary"
            keys = list(data.keys())
            keys_str = ", ".join(keys)
            logger.info(f"Loại dữ liệu: {data_type}, có các keys: {keys_str}")
        elif isinstance(data, list):
            data_type = "List"
            length = len(data)
            logger.info(f"Loại dữ liệu: {data_type}, độ dài: {length}")
        else:
            data_type = type(data).__name__
            logger.info(f"Loại dữ liệu: {data_type}")
        
        return {
            "status": "success",
            "message": "Đã xử lý dữ liệu",
            "data_type": data_type,
            "data_received": True
        }
    
    # Tạo và khởi động receiver
    receiver = create_receiver(handler=sample_handler)
    receiver.start()