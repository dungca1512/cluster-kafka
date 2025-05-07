from process_data import read_json_file, extract_json_from_string
import json

try:
    data = read_json_file('n8n_output_20250424_082022.json')
    print("Data from file:")
    print(json.dumps(data, indent=4, ensure_ascii=False))
except Exception as e:
    print(f"Error: {e}")

# Example 2: Process a string directly
string_with_json = '''return [{"quốc gia": "en","notification": "messageTitle: 🎉 Refresh and conquer\\nmessageText: 🎁 80% off this week \\ 🌟 Don't miss out!\\nimageUrl: http\\nexpiryTime: 64800\\nanalyticsLabel: Motivation","segment": "Inactive_users_6d"},
{"quốc gia": "th","notification": "messageTitle: การแจ้งเตือน 10:\\nmessageText: 🎉 เรียนรู้ใหม่และชนะ \\ 🎁 ส่วนลด 80% ในสัปดาห์นี้ \\ 🌟 ห้ามพลาดนะ!\\nimageUrl: http2\\nexpiryTime: 64800\\nanalyticsLabel: Motivation","segment": "Inactive_users_6d"}]'''

parsed_data = extract_json_from_string(string_with_json)
print("\nData from string:")
print(json.dumps(parsed_data, indent=4, ensure_ascii=False))