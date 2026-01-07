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
DOWNLOAD_EVENT_NAME = "download_intent"


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
        # 1. Web 流量
        res_curr = client.run_report(RunReportRequest(property=prop_path, date_ranges=[DateRange(start_date=r_s, end_date=r_e)], metrics=[Metric(name="activeUsers")]))
        curr = int(res_curr.rows[0].metric_values[0].value) if res_curr.rows else 0
        
        res_prev = client.run_report(RunReportRequest(property=prop_path, date_ranges=[DateRange(start_date=c_s, end_date=c_e)], metrics=[Metric(name="activeUsers")]))
        prev = int(res_prev.rows[0].metric_values[0].value) if res_prev.rows else 0
        
        pct = ((curr - prev) / prev) * 100 if prev > 0 else 0
        report['users'] = f"{curr} ({pct:+.1f}%)"

        # 2. 注册意向 (Top 7)
        res_intent = client.run_report(RunReportRequest(
            property=prop_path, date_ranges=[DateRange(start_date=r_s, end_date=r_e)], 
            dimensions=[Dimension(name="country")], metrics=[Metric(name="eventCount")],
            dimension_filter=FilterExpression(filter=Filter(field_name="eventName", string_filter=Filter.StringFilter(value=SIGNUP_EVENT_NAME))),
            order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="eventCount"), desc=True)], limit=7
        ))
        total_intent = sum([int(r.metric_values[0].value) for r in res_intent.rows]) if res_intent.rows else 0
        top_countries = ", ".join([f"{r.dimension_values[0].value}({r.metric_values[0].value})" for r in res_intent.rows])
        report['intent'] = f"{total_intent} signals"
        report['top_intent_country'] = top_countries

        # 3. App 下载意向 (Web Button Clicks)
        res_app = client.run_report(RunReportRequest(
            property=prop_path, date_ranges=[DateRange(start_date=r_s, end_date=r_e)], 
            metrics=[Metric(name="eventCount")],
            dimension_filter=FilterExpression(filter=Filter(field_name="eventName", string_filter=Filter.StringFilter(value=DOWNLOAD_EVENT_NAME)))
        ))
        app_clicks = int(res_app.rows[0].metric_values[0].value) if res_app.rows else 0
        report['app_clicks'] = str(app_clicks)

        # 4. 渠道 (Top 7)
        res_src = client.run_report(RunReportRequest(
            property=prop_path, date_ranges=[DateRange(start_date=r_s, end_date=r_e)], 
            dimensions=[Dimension(name="sessionSourceMedium")], metrics=[Metric(name="activeUsers")],
            order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="activeUsers"), desc=True)], limit=7
        ))
        
        src_list = []
        for r in res_src.rows:
            source_name = r.dimension_values[0].value
            if "t.co" in source_name:
                source_name = source_name.replace("t.co", "X (Twitter)")
            
            src_list.append(f"{source_name}({r.metric_values[0].value})")
            
        report['channels'] = ", ".join(src_list)
        
        return report
    except Exception as e:
        print(f"❌ GA4 Error: {e}")
        return None

def get_sheet_data(creds, target_date_obj):
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SPREADSHEET_ID)
        worksheet = sh.worksheet(SHEET_NAME)
        
        target_str = f"{target_date_obj.day}/{target_date_obj.month}/{target_date_obj.year}"
        print(f"Testing Date: {target_str}...")
        
        cell = None
        try:
            cell = worksheet.find(target_str)
        except gspread.exceptions.CellNotFound:
            pass
            
        if cell is None:
            return "Data pending update in Sheet"

        row_values = worksheet.row_values(cell.row)
        def get_col(idx): return row_values[idx] if len(row_values) > idx else "N/A"
        return f"Twitter: {get_col(7)}, Medium: {get_col(13)}, YouTube: {get_col(15)}"

    except Exception as e:
        print(f"❌ Sheet Error: {e}")
        return "Sheet connection issue"

def analyze_and_push(ga4_data, social_data, date_range_str):
    client_ai = genai.Client(api_key=GEMINI_API_KEY)
    
    prompt = f"""
    Role: Senior Marketing Analyst at dtcpay.
    Task: Write a Weekly Pulse email for Lark.
    
    **Context:**
    - Reporting Period: **{date_range_str}** (Last Week).
    
    **Raw Data Inputs:**
    1. Web Traffic: {ga4_data['users']} (Active Users & Trend).
    2. Intent Breakdown:
       - **Web Sign-Up Intent**: {ga4_data['intent']} (Top Geos: {ga4_data['top_intent_country']}).
       - **Web-Driven App Clicks**: {ga4_data['app_clicks']} (Metric: '{DOWNLOAD_EVENT_NAME}').
    3. Channel Mix: {ga4_data['channels']}.
    4. Social Media: "{social_data}" (If pending, mention tracking is underway).
    
    **Writing Instructions (Refined):**
    
    1.  **Opening:** Ultra-concise (Max 15 words). No fluff.
    2.  **Smart Analysis:** - Combine Web Intent and App Interest.
        - **Nuance on App Data (Important):** If 'App Clicks' is 0, do NOT say "no one downloaded the app". Say "no web-originated app clicks were recorded". Acknowledge that users may still download directly from Stores.
        - Identify outliers in Channels and Geos.
    3.  **Structure:**
        - **Header:** "Hi Team,"
        - **Intro:** Punchy summary.
        - **Section 1: 「Web Traffic」**
        - **Section 2: 「Growth & Intent」** (Discuss Sign-ups. Handle App data carefully as instructed above).
        - **Section 3: 「Channel & Social」**
        - **Closing:** "Best,"
    4.  **Format:** Use parentheses `( )` for numbers. NO Markdown bold (**).
    """
    
    print(">>> AI 正在进行深度分析...")
    try:
        response = client_ai.models.generate_content(
            model="gemini-2.5-flash", 
            contents=prompt,
            config=types.GenerateContentConfig(temperature=0.6) 
        )
        msg = response.text.replace("**", "") 

        if "Subject:" in msg:
            msg = msg.split("Subject:")[1].split("\n", 1)[1].strip()

        final_msg = "🚀 Marketing Weekly Pulse\n\n" + msg
        
        requests.post(LARK_WEBHOOK_URL, json={"msg_type": "text", "content": {"text": final_msg}})
        print("✅ 推送成功！")
        
    except Exception as e:
        print(f"❌ AI Push Error: {e}")

if __name__ == "__main__":
    creds = get_creds()
    if creds:
        today = datetime.now()
        offset = today.weekday() + 1
        end_date = today - timedelta(days=offset)
        start_date = end_date - timedelta(days=6)
        
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
