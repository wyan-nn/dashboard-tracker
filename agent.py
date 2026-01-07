try:
    import google.generativeai
except ImportError:
    pass 

import os
import requests
import gspread
from google.oauth2.credentials import Credentials
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Metric, Dimension, OrderBy, FilterExpression, Filter
from google import genai
from datetime import datetime, timedelta

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
LARK_WEBHOOK_URL = os.environ.get("LARK_WEBHOOK_URL")
GCP_CLIENT_ID = os.environ.get("GCP_CLIENT_ID")
GCP_CLIENT_SECRET = os.environ.get("GCP_CLIENT_SECRET")
GCP_REFRESH_TOKEN = os.environ.get("GCP_REFRESH_TOKEN")

GA4_PROPERTY_ID = "347977871"
SPREADSHEET_ID = "1veYNmir-oqbmbnvGvVZGixnTArz8TXLALDvLE9rQ14Q"
SHEET_NAME = "2026" 
SIGNUP_EVENT_NAME = "sign_up_intent" 


def get_creds():
    if not GCP_REFRESH_TOKEN:
        print("❌ 错误：未检测到 GCP_REFRESH_TOKEN，请检查 GitHub Secrets！")
        return None
    return Credentials(
        None,
        refresh_token=GCP_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GCP_CLIENT_ID,
        client_secret=GCP_CLIENT_SECRET
    )

def get_ga4_data(creds, r_s, r_e, c_s, c_e):
    client = BetaAnalyticsDataClient(credentials=creds, transport="rest")
    prop_path = f"properties/{GA4_PROPERTY_ID}"
    report = {}

    try:
        # 1. 流量 (Web Traffic)
        res_curr = client.run_report(RunReportRequest(property=prop_path, date_ranges=[DateRange(start_date=r_s, end_date=r_e)], metrics=[Metric(name="activeUsers")]))
        curr = int(res_curr.rows[0].metric_values[0].value) if res_curr.rows else 0
        
        res_prev = client.run_report(RunReportRequest(property=prop_path, date_ranges=[DateRange(start_date=c_s, end_date=c_e)], metrics=[Metric(name="activeUsers")]))
        prev = int(res_prev.rows[0].metric_values[0].value) if res_prev.rows else 0
        
        pct = ((curr - prev) / prev) * 100 if prev > 0 else 0
        report['users'] = f"{curr}"       # 纯数字
        report['wow'] = f"{pct:+.1f}%"    # 涨跌幅

        # 2. 意向 (Sign-Up Intent)
        res_intent = client.run_report(RunReportRequest(
            property=prop_path, date_ranges=[DateRange(start_date=r_s, end_date=r_e)], 
            dimensions=[Dimension(name="country")], metrics=[Metric(name="eventCount")],
            dimension_filter=FilterExpression(filter=Filter(field_name="eventName", string_filter=Filter.StringFilter(value=SIGNUP_EVENT_NAME))),
            order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="eventCount"), desc=True)], limit=3
        ))
        total_intent = sum([int(r.metric_values[0].value) for r in res_intent.rows]) if res_intent.rows else 0
        
        # 只要前三个国家，格式化为: Singapore, Malaysia, UK
        top_countries = ", ".join([f"{r.dimension_values[0].value}" for r in res_intent.rows])
        report['intent_total'] = f"{total_intent}"
        report['top_countries'] = top_countries

        # 3. 渠道 (Channels)
        res_src = client.run_report(RunReportRequest(
            property=prop_path, date_ranges=[DateRange(start_date=r_s, end_date=r_e)], 
            dimensions=[Dimension(name="sessionSourceMedium")], metrics=[Metric(name="activeUsers")],
            order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="activeUsers"), desc=True)], limit=3
        ))
        # 格式化: google/organic, direct/none
        report['channels'] = ", ".join([f"{r.dimension_values[0].value}" for r in res_src.rows])
        
        return report
    except Exception as e:
        print(f"❌ GA4 Error: {e}")
        return None

# --- B: Google Sheet 数据抓取 ---
def get_sheet_data(creds, target_date_obj):
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SPREADSHEET_ID)
        worksheet = sh.worksheet(SHEET_NAME)
        
        # 构造查找字符串: 5/1/2026 (不带前导0)
        target_str = f"{target_date_obj.day}/{target_date_obj.month}/{target_date_obj.year}"
        print(f"Testing Date: 寻找表格中的日期 {target_str}...")
        
        try:
            cell = worksheet.find(target_str)
        except gspread.exceptions.CellNotFound:
            print(f"⚠️ Warning: 表格里没找到 {target_str}")
            return "Social Data: N/A (Data not updated yet)"

        row_values = worksheet.row_values(cell.row)
        def get_col(idx): return row_values[idx] if len(row_values) > idx else "0"
        
        # H=7(Twitter), N=13(Medium), P=15(Youtube)
        return f"Twitter: {get_col(7)}, Medium: {get_col(13)}, YouTube: {get_col(15)}"

    except Exception as e:
        print(f"❌ Sheet Error: {e}")
        return "Social Data: Error"

# --- C: AI 分析与推送 (定制排版) ---
def analyze_and_push(ga4_data, social_data, date_range_str):
    client_ai = genai.Client(api_key=GEMINI_API_KEY)
    
    # 🌟 这里的 Prompt 是核心：强制规定了每一行的写法
    prompt = f"""
    Role: Head of Marketing at dtcpay.
    Task: Write a Weekly Pulse email for Lark.
    
    **Context:**
    - Period: {date_range_str} (Last Week).
    
    **Data:**
    - Web Users: {ga4_data['users']} (WoW: {ga4_data['wow']})
    - Intent Signals ('{SIGNUP_EVENT_NAME}'): {ga4_data['intent_total']}
    - Top Intent Countries: {ga4_data['top_countries']}
    - Top Channels: {ga4_data['channels']}
    - Social Stats: {social_data}
    
    **Visual Layout Instructions (Strictly Follow This):**
    1. Start with the title: 🚀 Marketing Weekly Pulse
    2. Salutation: "Hi Team,"
    3. Opening: "Here's the performance update for the period of **{date_range_str}**."
    4. **Body Sections**: Use the exact Japanese brackets 「 」 for titles.
    5. **Numbers**: Put key metrics inside standard parentheses ( ).
    
    **Drafting Template (Fill in the content):**
    
    🚀 Marketing Weekly Pulse
    
    Hi Team,
    
    Here's our Marketing Weekly Pulse for the period  **{date_range_str}**.
    
    「Web Traffic」
    We recorded/ended ({ga4_data['users']}) active users last week, representing a ({ga4_data['wow']}) trend week-over-week.
    
    「Growth & Intent」
    We generated/captured ({ga4_data['intent_total']}) high-intent signals. Top markets driving this interest are {ga4_data['top_countries']}. Key acquisition channels include {ga4_data['channels']}.
    
    「Social Media」
    Current snapshot: {social_data}.
    
    「Next Step」
    [Write 1 short, actionable sentence based on the data above].
    
    Best,
    """
    
    print(">>> AI 正在按照定制模板撰写...")
    try:
        response = client_ai.models.generate_content(model="gemini-2.5-flash", contents=prompt)
        
        # 清洗可能存在的 Markdown 加粗 (**)，让 Lark 显示更干净
        msg = response.text.replace("**", "") 
        
        requests.post(LARK_WEBHOOK_URL, json={"msg_type": "text", "content": {"text": msg}})
        print("✅ 推送成功！")
        
    except Exception as e:
        print(f"❌ Error: {e}")

# ==========================================
# 4. 执行
# ==========================================
if __name__ == "__main__":
    creds = get_creds()
    if creds:
        # 日期逻辑：找“上周一”
        today = datetime.now()
        offset = today.weekday() + 1
        end_date = today - timedelta(days=offset) # 上周日
        start_date = end_date - timedelta(days=6) # 上周一
        
        print(f">>> 启动任务: {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}")
        
        # GA4 对比周期
        prev_end = start_date - timedelta(days=1)
        prev_start = prev_end - timedelta(days=6)
        
        ga4_res = get_ga4_data(creds, 
                               start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'),
                               prev_start.strftime('%Y-%m-%d'), prev_end.strftime('%Y-%m-%d'))
        
        # Sheet 数据
        sheet_res = get_sheet_data(creds, start_date)
        
        if ga4_res:
            date_str = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            analyze_and_push(ga4_res, sheet_res, date_str)
