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
from google.genai import types
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

# --- 模块 A: GA4 (视野扩大版) ---
def get_ga4_data(creds, r_s, r_e, c_s, c_e):
    client = BetaAnalyticsDataClient(credentials=creds, transport="rest")
    prop_path = f"properties/{GA4_PROPERTY_ID}"
    report = {}

    try:
        # 1. 流量总数
        res_curr = client.run_report(RunReportRequest(property=prop_path, date_ranges=[DateRange(start_date=r_s, end_date=r_e)], metrics=[Metric(name="activeUsers")]))
        curr = int(res_curr.rows[0].metric_values[0].value) if res_curr.rows else 0
        
        res_prev = client.run_report(RunReportRequest(property=prop_path, date_ranges=[DateRange(start_date=c_s, end_date=c_e)], metrics=[Metric(name="activeUsers")]))
        prev = int(res_prev.rows[0].metric_values[0].value) if res_prev.rows else 0
        
        pct = ((curr - prev) / prev) * 100 if prev > 0 else 0
        report['users'] = f"{curr} ({pct:+.1f}%)"

        # 2. 意向 (扩大到 Top 7)
        # 这样 AI 就能看到除了前三名之外，有没有表现不错的第四名
        res_intent = client.run_report(RunReportRequest(
            property=prop_path, date_ranges=[DateRange(start_date=r_s, end_date=r_e)], 
            dimensions=[Dimension(name="country")], metrics=[Metric(name="eventCount")],
            dimension_filter=FilterExpression(filter=Filter(field_name="eventName", string_filter=Filter.StringFilter(value=SIGNUP_EVENT_NAME))),
            order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="eventCount"), desc=True)], 
            limit=7 
        ))
        total_intent = sum([int(r.metric_values[0].value) for r in res_intent.rows]) if res_intent.rows else 0
        # 将结果拼接成 "Singapore(30), Malaysia(10)..." 的长字符串给 AI 看
        top_countries = ", ".join([f"{r.dimension_values[0].value}({r.metric_values[0].value})" for r in res_intent.rows])
        report['intent'] = f"{total_intent} signals"
        report['top_intent_country'] = top_countries

        # 3. 渠道 (扩大到 Top 7)
        res_src = client.run_report(RunReportRequest(
            property=prop_path, date_ranges=[DateRange(start_date=r_s, end_date=r_e)], 
            dimensions=[Dimension(name="sessionSourceMedium")], metrics=[Metric(name="activeUsers")],
            order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="activeUsers"), desc=True)], 
            limit=7
        ))
        report['channels'] = ", ".join([f"{r.dimension_values[0].value}({r.metric_values[0].value})" for r in res_src.rows])
        
        return report
    except Exception as e:
        print(f"❌ GA4 Error: {e}")
        return None

# --- 模块 B: Google Sheet (容错版) ---
def get_sheet_data(creds, target_date_obj):
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SPREADSHEET_ID)
        worksheet = sh.worksheet(SHEET_NAME)
        
        target_str = f"{target_date_obj.day}/{target_date_obj.month}/{target_date_obj.year}"
        print(f"Testing Date: 寻找表格中的日期 {target_str}...")
        
        try:
            cell = worksheet.find(target_str)
        except gspread.exceptions.CellNotFound:
            print(f"⚠️ Warning: 表格里没找到 {target_str}。可能是新的一年数据还没填。")
            return "No Social Data available (Data missing in Sheet)."

        row_values = worksheet.row_values(cell.row)
        def get_col(idx): return row_values[idx] if len(row_values) > idx else "N/A"
        
        # H=7(Twitter), N=13(Medium), P=15(Youtube)
        return f"Twitter: {get_col(7)}, Medium: {get_col(13)}, YouTube: {get_col(15)}"

    except Exception as e:
        print(f"❌ Sheet Error: {e}")
        return "Social Data Error"

# --- 模块 C: AI 分析 (灵动分析师版) ---
def analyze_and_push(ga4_data, social_data, date_range_str):
    client_ai = genai.Client(api_key=GEMINI_API_KEY)
    
    prompt = f"""
    Role: Senior Marketing Analyst at dtcpay.
    Task: Write a Weekly Pulse email for Lark.
    
    **Context:**
    - Reporting Period: **{date_range_str}** (Last Week).
    
    **Raw Data Inputs:**
    1. Web Traffic: {ga4_data['users']} (Active Users & Week-over-Week trend).
    2. Intent ('{SIGNUP_EVENT_NAME}'): Total {ga4_data['intent']}. Breakdown: {ga4_data['top_intent_country']}.
    3. Channel Mix: {ga4_data['channels']}.
    4. Social Media: {social_data}.
    
    **Writing Instructions (Be Human & Insightful):**
    
    1.  **Dynamic Language:** Do NOT use the exact same opening or closing every week. Vary your vocabulary. Use an energetic, professional tone.
    2.  **Smart Selection (Crucial):** - You are provided with the Top 7 sources/cities. **Do NOT just list the top 3 blindly.**
        - Look at the data. Is there a dominant #1? Or is it evenly split?
        - Is there a "rising star" at #4 or #5 that has decent volume? Mention it! 
        - Example: "While Singapore leads, we see notable volume emerging from [Country X]..."
    3.  **Narrative Flow:** Connect the dots. Instead of just listing numbers, explain *what* they mean.
        - e.g., "Organic search remains our engine, driving X% of traffic..."
    
    **Structure (Maintain Visual Consistency):**
    - **Header:** Start with "Hi Team,"
    - **Opening:** One sentence summary of the week's vibe (e.g., "Solid growth week" or "Steady performance").
    - **Section 1: 「Web Traffic」**
      - Report the user count and the trend.
    - **Section 2: 「Growth & Intent」**
      - Analyze the sign-up intent. Mention the top contributors and any interesting runner-ups.
    - **Section 3: 「Channel & Social」**
      - Comment on the channel mix (Organic vs Direct vs Others) and the Social snapshot.
    - **Closing:** A brief, encouraging sign-off + "Best,".
    
    **Formatting Rules:**
    - Use `「Title」` for section headers.
    - **Highlight key numbers** in parentheses like `(713)` or `(+15%)`.
    - NO Markdown bold (**). Keep it clean text.
    """
    
    print(">>> AI 正在进行深度分析...")
    try:
        response = client_ai.models.generate_content(
            model="gemini-2.5-flash", 
            contents=prompt,
            config=types.GenerateContentConfig(temperature=0.6)
        )
        msg = response.text.replace("**", "") 
        
        requests.post(LARK_WEBHOOK_URL, json={"msg_type": "text", "content": {"text": msg}})
        print("✅ 推送成功！")
        
    except Exception as e:
        print(f"❌ Error: {e}")

# ==========================================
# 主程序
# ==========================================
if __name__ == "__main__":
    creds = get_creds()
    if creds:
        # 永远抓取“上周一”到“上周日”
        today = datetime.now()
        offset = today.weekday() + 1
        end_date = today - timedelta(days=offset) # 上周日
        start_date = end_date - timedelta(days=6) # 上周一
        
        print(f">>> 启动分析 Agent: {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}")
        
        prev_end = start_date - timedelta(days=1)
        prev_start = prev_end - timedelta(days=6)
        
        ga4_res = get_ga4_data(creds, 
                               start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'),
                               prev_start.strftime('%Y-%m-%d'), prev_end.strftime('%Y-%m-%d'))
        
        sheet_res = get_sheet_data(creds, start_date)
        
        if ga4_res:
            date_str = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            analyze_and_push(ga4_res, sheet_res, date_str)
