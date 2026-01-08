try:
    import google.generativeai
except ImportError:
    pass 

import os
import requests
import gspread
import sys
from google.oauth2.credentials import Credentials
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Metric, Dimension, OrderBy, FilterExpression, Filter
from google import genai
from google.genai import types
from datetime import datetime, timedelta, date
import calendar

# ============
TEST_MODE = True
# ============

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

def get_last_month_dates():
    today = date.today()
    # 本月第一天
    first_day_this_month = today.replace(day=1)
    # 上个月最后一天
    last_day_last_month = first_day_this_month - timedelta(days=1)
    # 上个月第一天
    first_day_last_month = last_day_last_month.replace(day=1)
    
    # 环比周期 (上上个月)
    last_day_prev_month = first_day_last_month - timedelta(days=1)
    first_day_prev_month = last_day_prev_month.replace(day=1)
    
    return (
        first_day_last_month.strftime('%Y-%m-%d'),
        last_day_last_month.strftime('%Y-%m-%d'),
        first_day_prev_month.strftime('%Y-%m-%d'),
        last_day_prev_month.strftime('%Y-%m-%d')
    )

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
        # 1. 宏观流量 & 质量 (新增 Engagement Rate)
        res_curr = client.run_report(RunReportRequest(
            property=prop_path, 
            date_ranges=[DateRange(start_date=r_s, end_date=r_e)], 
            metrics=[Metric(name="activeUsers"), Metric(name="engagementRate")]
        ))
        
        # 上个月数据
        curr_users = int(res_curr.rows[0].metric_values[0].value) if res_curr.rows else 0
        curr_eng_rate = float(res_curr.rows[0].metric_values[1].value) if res_curr.rows else 0
        
        # 环比数据 (只取 Users 做对比)
        res_prev = client.run_report(RunReportRequest(
            property=prop_path, 
            date_ranges=[DateRange(start_date=c_s, end_date=c_e)], 
            metrics=[Metric(name="activeUsers")]
        ))
        prev_users = int(res_prev.rows[0].metric_values[0].value) if res_prev.rows else 0
        
        pct = ((curr_users - prev_users) / prev_users) * 100 if prev_users > 0 else 0
        
        # 格式化输出：用户数 (环比) | 互动率
        report['users_context'] = f"{curr_users} Active Users ({pct:+.1f}% MoM). Engagement Rate: {curr_eng_rate:.1%}"

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

        # 3. App 下载意向
        res_app = client.run_report(RunReportRequest(
            property=prop_path, date_ranges=[DateRange(start_date=r_s, end_date=r_e)], 
            metrics=[Metric(name="eventCount")],
            dimension_filter=FilterExpression(filter=Filter(field_name="eventName", string_filter=Filter.StringFilter(value=DOWNLOAD_EVENT_NAME)))
        ))
        app_clicks = int(res_app.rows[0].metric_values[0].value) if res_app.rows else 0
        report['app_clicks'] = str(app_clicks)

        # 4. 渠道
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

def get_sheet_data(creds, last_month_end_date_str):
    # 策略：寻找上个月 "最后的一个周一" 的日期
    # 解析传入的字符串 (e.g., "2026-01-31")
    last_day = datetime.strptime(last_month_end_date_str, "%Y-%m-%d").date()
    
    # 往回找，直到找到周一 (0 = Monday)
    target_date = last_day
    while target_date.weekday() != 0:
        target_date -= timedelta(days=1)
    
    try:
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SPREADSHEET_ID)
        worksheet = sh.worksheet(SHEET_NAME)
        
        # 构造 Google Sheet 格式: 26/1/2026 (d/m/yyyy)
        target_str = f"{target_date.day}/{target_date.month}/{target_date.year}"
        print(f"Testing Date (End of Month snapshot): {target_str}...")
        
        cell = None
        try:
            cell = worksheet.find(target_str)
        except gspread.exceptions.CellNotFound:
            pass
            
        if cell is None:
            return f"Data pending (Could not find entry for week of {target_str})"

        row_values = worksheet.row_values(cell.row)
        def get_col(idx): return row_values[idx] if len(row_values) > idx else "N/A"
        return f"Twitter: {get_col(7)}, Medium: {get_col(13)}, YouTube: {get_col(15)}"

    except Exception as e:
        print(f"❌ Sheet Error: {e}")
        return "Sheet connection issue"

def analyze_and_push(ga4_data, social_data, date_range_str):
    client_ai = genai.Client(api_key=GEMINI_API_KEY)
    
    prompt = f"""
    Role: Marketing Strategy Lead at dtcpay.
    Task: Write a **Monthly Marketing Pulse** push for Lark.
    
    **Context:**
    - **Reporting Period:** {date_range_str} (The Entire Previous Month).
    - **Audience:** Management Team.
    - **Tone:** Strategic, Insightful, Professional.
    - **Constraint:** DO NOT include a "Subject" line. Start directly with "Hi Team,".
    
    **Data Inputs:**
    1. **Traffic & Quality:** {ga4_data['users_context']}. (Note: Engagement Rate > 50% is good).
    2. **Intent & Conversion:**
       - Web Sign-Ups: {ga4_data['intent']} (Top Geos: {ga4_data['top_intent_country']}).
       - Web-to-App Interest: {ga4_data['app_clicks']} clicks.
    3. **Acquisition Mix:** {ga4_data['channels']}.
    4. **Social Snapshot (End of Month):** "{social_data}".
    
    **Writing Instructions:**
    
    1.  **Overview:** Start with a high-level summary of the month's performance (MoM growth and Traffic Quality).
    2.  **Analysis (Not just numbers):**
        - Comment on **Engagement Rate**. Is the traffic quality healthy?
        - Connect Geo data with Sign-ups. Are we seeing growth in strategic markets (e.g., SG/KL/HK/APAC)?
        - Handle App Clicks: If 0, state "No web-originated app clicks recorded."
    3.  **Structure:**
        - **Header:** "Hi Team,"
        - **Intro:** Strategic Summary (1 sentence).
        - **「Traffic & Quality」**: Users, MoM trend, and Engagement Rate.
        - **「Growth & Intent」**: Sign-ups and App interest.
        - **「Channel & Social」**: Source mix and Social stats.
        - **Closing:** "Best,"
    4.  **Format:** Use parentheses `( )` for numbers. NO Markdown bold (**).
    """
    
    print(">>> AI 正在进行月度分析...")
    try:
        response = client_ai.models.generate_content(
            model="gemini-2.5-flash", 
            contents=prompt,
            config=types.GenerateContentConfig(temperature=0.6) 
        )
        msg = response.text.replace("**", "") 
        
        # 强制清洗 Subject 行
        if "Subject:" in msg:
            msg = msg.split("Subject:")[1].split("\n", 1)[1].strip()
        
        final_msg = "📅 Marketing Monthly Pulse\n\n" + msg
        
        requests.post(LARK_WEBHOOK_URL, json={"msg_type": "text", "content": {"text": final_msg}})
        print("✅ 推送成功！")
        
    except Exception as e:
        print(f"❌ AI Push Error: {e}")

if __name__ == "__main__":
    today = date.today()
    
    if not TEST_MODE:
        if today.day > 7:
            print(f"📅 今天是 {today}，不是本月的第一个周一，生产模式下跳过推送。")
            sys.exit(0)
        else:
            print("🚀 检测到今天是本月第一个周一，开始执行生产推送！")
    else:
        print("🔧 [调试模式] 强制执行月报逻辑 (Reporting Last Month)...")

    creds = get_creds()
    if creds:
        # 获取上个月的起止日期
        r_s, r_e, c_s, c_e = get_last_month_dates()
        
        print(f">>> 启动月报 Agent: 报告周期 {r_s} 至 {r_e}")
        
        ga4_res = get_ga4_data(creds, r_s, r_e, c_s, c_e)
        sheet_res = get_sheet_data(creds, r_e)
        
        if ga4_res:
            date_str = f"{r_s} to {r_e}"
            analyze_and_push(ga4_res, sheet_res, date_str)
