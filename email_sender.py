# email_sender.py
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from dotenv import load_dotenv
from loguru import logger

# Load biến môi trường
load_dotenv()

class EmailSender:
    def __init__(self, config=None):
        """
        Khởi tạo EmailSender với cấu hình
        
        Nếu config=None, sẽ đọc cấu hình từ biến môi trường
        """
        if config is None:
            self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
            self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
            self.username = os.getenv('EMAIL_USERNAME', '')
            self.password = os.getenv('EMAIL_PASSWORD', '')
        else:
            self.smtp_server = config.get('smtp_server', 'smtp.gmail.com')
            self.smtp_port = int(config.get('smtp_port', 587))
            self.username = config.get('username', '')
            self.password = config.get('password', '')
        
        # Kiểm tra cấu hình
        if not self.username or not self.password:
            logger.warning("Email credentials are not properly configured")

    def send_email(self, to_email, subject, body, cc=None):
        """
        Gửi email đến địa chỉ đã cho
        
        Args:
            to_email (str): Địa chỉ email nhận
            subject (str): Tiêu đề email
            body (str): Nội dung email
            cc (list, optional): Danh sách các địa chỉ CC
            
        Returns:
            bool: True nếu gửi thành công, False nếu thất bại
        """
        try:
            # Tạo message
            msg = MIMEMultipart()
            msg['From'] = self.username
            msg['To'] = to_email
            msg['Subject'] = subject
            
            if cc:
                msg['Cc'] = ", ".join(cc)
                
            # Thêm body
            msg.attach(MIMEText(body, 'plain'))
            
            # Thiết lập danh sách người nhận
            recipients = [to_email]
            if cc:
                recipients.extend(cc)
                
            # Gửi email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.username, self.password)
                server.sendmail(self.username, recipients, msg.as_string())
                
            logger.success(f"Email sent successfully to {to_email}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email to {to_email}: {str(e)}")
            return False

    def format_notification(self, notification_text):
        """
        Phân tích và định dạng nội dung thông báo từ dữ liệu Kafka
        
        Args:
            notification_text (str): Chuỗi thông báo thô
            
        Returns:
            tuple: (title, body) đã được định dạng
        """
        title = self._extract_field(notification_text, "messageTitle")
        text = self._extract_field(notification_text, "messageText")
        expiry_time = self._extract_field(notification_text, "expiryTime")
        analytics_label = self._extract_field(notification_text, "analyticsLabel")
        
        # Định dạng body
        body = text or notification_text
        
        # Thêm thông tin hết hạn nếu có
        if expiry_time:
            body += f"\n\nOffer expires in {int(expiry_time)//3600} hours."
            
        return title, body

    def _extract_field(self, notification_text, field_name):
        """
        Trích xuất giá trị trường từ chuỗi thông báo
        """
        try:
            start_tag = f"{field_name}: "
            if start_tag in notification_text:
                start_pos = notification_text.find(start_tag) + len(start_tag)
                end_pos = notification_text.find("\n", start_pos)
                if end_pos == -1:
                    end_pos = len(notification_text)
                return notification_text[start_pos:end_pos].strip()
        except Exception as e:
            logger.error(f"Error extracting field {field_name}: {str(e)}")
        return None


if __name__ == "__main__":
    # Test EmailSender
    sender = EmailSender()
    test_notification = "messageTitle: 🎉 A big opportunity is here!\nmessageText: 🔖 Get 80% off tuition fees! \\ ✈️ Let's explore Japan!\nimageUrl: \nexpiryTime: 64800\nanalyticsLabel: Motivation"
    title, body = sender.format_notification(test_notification)
    logger.info(f"Title: {title}")
    logger.info(f"Body: {body}")