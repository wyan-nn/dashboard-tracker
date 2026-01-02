import os
import time
import re
from datetime import datetime, timedelta
import gspread
from google.oauth2.credentials import Credentials 
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Metric, RunReportRequest
from googleapiclient.discovery import build
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup

# ================= 配置区域 =================
SPREADSHEET_ID = '1veYNmir-oqbmbnvGvVZGixnTArz8TXLALDvLE9rQ14Q' 
SHEET_NAME = '2026'

GA4_PROPERTY_ID = '347977871'
YOUTUBE_CHANNEL_ID = 'UCSheH8EH_2CrCWYZg9AW91w'
MEDIUM_URL = "https://medium.com/@dtcpay" 
TWITTER_HANDLE = "dtc_pay"

# ================= 认证模块 =================
def get_user_credentials():
    # 从 GitHub Secrets 读取 OAuth 信息
    client_id = os.environ.get('GCP_CLIENT_ID')
    client_secret = os.environ.get('GCP_CLIENT_SECRET')
    refresh_token = os.environ.get('GCP_REFRESH_TOKEN')
    
    if not all([client_id, client_secret, refresh_token]):
        raise ValueError("❌ 缺少 OAuth 凭证，请检查 GitHub Secrets!")

    # 手动构建凭证对象
    creds = Credentials(
        None, 
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=[
            'https://www.googleapis.com/auth/analytics.readonly',
            'https://www.googleapis.com/auth/youtube.readonly',
            'https://www.googleapis.com/auth/spreadsheets'
        ]
    )
    return creds

# ================= 爬虫模块  =================
def get_ga4_data(creds, start_date_str, end_date_str):
    """
    start_date_str: 格式 '2026-01-05'
    end_date_str:   格式 '2026-01-11'
    """
    try:
        client = BetaAnalyticsDataClient(credentials=creds)
        request = RunReportRequest(
            property=f"properties/{GA4_PROPERTY_ID}",
            date_ranges=[DateRange(start_date=start_date_str, end_date=end_date_str)],
            metrics=[Metric(name="totalUsers")],
        )
        response = client.run_report(request)
        if response.rows:
            return int(response.rows[0].metric_values[0].value)
    except Exception as e:
        print(f"❌ GA4 Error: {e}")
    return 0

def get_youtube_data(creds):
    try:
        youtube = build('youtube', 'v3', credentials=creds)
        request = youtube.channels().list(part="statistics", id=YOUTUBE_CHANNEL_ID)
        response = request.execute()
        if response['items']:
            return int(response['items'][0]['statistics']['subscriberCount'])
    except Exception as e:
        print(f"❌ YouTube Error: {e}")
    return 0

def get_medium_data():
    # 尝试 Selenium 抓取
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    try:
        driver.get(MEDIUM_URL)
        time.sleep(5)
        # 简单查找逻辑
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        target = soup.find(string=lambda t: t and 'Followers' in t)
        if target:
            num = target.strip().split(' ')[0]
            if 'K' in num: num = float(num.replace('K',''))*1000
            return int(str(num).replace(',','').split('.')[0])
    except Exception as e:
        print(f"❌ Medium Error: {e}")
    finally:
        driver.quit()
    return 0

def get_twitter_data():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    try:
        url = f"https://x.com/{TWITTER_HANDLE}"
        driver.get(url)
        time.sleep(5)
        body = driver.find_element(By.TAG_NAME, "body").text
        match = re.search(r"([\d.,]+[KM]?)\s+Followers", body)
        if match:
            raw = match.group(1).replace(',', '')
            if 'K' in raw: raw = float(raw.replace('K',''))*1000
            elif 'M' in raw: raw = float(raw.replace('M',''))*1000000
            return int(float(raw))
    except Exception as e:
        print(f"❌ Twitter Error: {e}")
    finally:
        driver.quit()
    return 0


def main():
    print("🚀 开始执行 (周一运行版: 抓取上周一至上周日)...")
    creds = get_user_credentials()
    
    # --- 日期计算 (最终确定的逻辑) ---
    # 假设今天是 1月5日 (周一)
    today = datetime.now()
    
    # 1. 锚点日期 = 今天 (即 1月5日)
    sheet_date_str = today.strftime('%-d/%-m/%Y') # 写入表格: 5/1/2026
    
    # 2. 数据范围: 上周一 到 上周日 (昨天)
    # 结束日期 = 昨天 (1月4日)
    end_date = today - timedelta(days=1)
    
    # 开始日期 = 昨天再往前推6天 (12月29日)
    start_date = end_date - timedelta(days=6)
    
    # 格式化 API 需要的格式
    ga4_start_str = start_date.strftime('%Y-%m-%d')
    ga4_end_str = end_date.strftime('%Y-%m-%d')
    
    print(f"📅 锚点日期 (A列): {sheet_date_str}")
    print(f"📊 数据抓取区间: {ga4_start_str} (上周一) -> {ga4_end_str} (上周日)")
    # 预期输出: 2025-12-29 到 2026-01-04

    # --- 抓取 ---
    val_ga4 = get_ga4_data(creds, ga4_start_str, ga4_end_str)
    val_yt = get_youtube_data(creds)
    val_med = get_medium_data()
    val_x = get_twitter_data()
    
    # --- 写入表格 ---
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    worksheet = sh.worksheet(SHEET_NAME)
    
    row_data = [
        sheet_date_str, # A列: 5/1/2026
        val_ga4,        # B列: GA4 (29/12 - 4/1)
        "", "", "", "", "", 
        val_x,          # H列: X
        "", "", "", "", "", 
        val_med,        # N列: Medium
        "",             
        val_yt          # P列: YouTube
    ]
    
    worksheet.append_row(row_data, value_input_option='USER_ENTERED')
    print(f"✅ 成功写入第 {len(worksheet.get_all_values())} 行！")

if __name__ == "__main__":
    main()
