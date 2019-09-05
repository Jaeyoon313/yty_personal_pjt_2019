import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
import ssl
from operator import eq
from urllib import request



def get_last_page_num(code):
    npage = 1
    url = 'http://finance.naver.com/item/sise_day.nhn?code=%s&page=1' % (code)
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "lxml")
    td = soup.find('td', attrs={'class': 'pgRR'})
    if td:
        npage = td.a['href'].split('page=')[1]
    return int(npage)


def get_data_naver(code, start=datetime(1900, 1, 1), end=datetime(2100, 1, 1)):
    url_tmpl = 'http://finance.naver.com/item/sise_day.nhn?code=%s&page=%d'
    npages = get_last_page_num(code)
    df_price = pd.DataFrame()
    for p in range(1, npages + 1):


        url = url_tmpl % (code, p)

        #mac python
        context = ssl._create_unverified_context()
        response = request.urlopen(url, context=context)
        html = response.read()
        #\mac python

        #window -> dfs = pd.read_html(url)
        #mac    -> dfs = pd.read_html(html)
        dfs = pd.read_html(html)


        print(3)
        # first page
        df = dfs[0]
        df.columns = ['날짜', '종가', '등락', '시가', '고가', '저가', '거래량']
        df = df[1:].copy()
        df.dropna(inplace=True)
        df = df.replace('\.', '-', regex=True)

        # select date range
        start_str = start.strftime("%Y-%m-%d")
        end_str = end.strftime("%Y-%m-%d")
        mask = (df['날짜'] >= start_str) & (df['날짜'] <= end_str)
        df_in = df[mask]

        # merge dataframe
        df_price = df_price.append(df_in)
        print('%d,' % p, end='', flush=True)
        print(df['날짜'].max())
        if len(df) <= 0 or df['날짜'].max() <= start_str:
            break
    print()
    df_price['날짜'] = pd.to_datetime(df_price['날짜'])
    int_cols = ['종가', '등락', '시가', '고가', '저가', '거래량']
    df_price[int_cols] = df_price[int_cols].astype('int', errors='ignore')
    return df_price


if __name__ == "__main__":
    conn = sqlite3.connect('stock_price_hynix.db')
    create_sql = """
    CREATE TABLE IF NOT EXISTS "stock_price_hynix" (
        "날짜" DATETIME,
        "종목코드" TEXT,
        "시가" BIGINT,
        "고가" BIGINT, 
        "저가" BIGINT, 
        "종가" BIGINT,
        "거래량" BIGINT, 
        "등락" BIGINT, 
        UNIQUE("날짜", "종목코드") ON CONFLICT REPLACE
    );
    """
    conn.execute(create_sql)
    conn.execute('CREATE INDEX IF NOT EXISTS ix_price_date on stock_price_hynix(날짜);')
    conn.execute('CREATE INDEX IF NOT EXISTS ix_price_code on stock_price_hynix(종목코드);')

    # 종목마스터 ('stock_master.db') 연결
    conn.execute("attach database 'stock_master.db' as master")

    df_master = pd.read_sql("SELECT * FROM stock_master WHERE 종목코드 = '000660'", conn)
    for inx, row in df_master.iterrows():
        print(row['종목코드'], row['종목명'])
        print("문자열 길이 : " + str(len(row['종목코드'])))
        if eq(len(row['종목코드']), 6):
            print("적합한 코드입니다")
            #  start: DB에 저장된 마지막 날짜 + 1일
            df_max = pd.read_sql('SELECT MAX (날짜) AS "maxdate" FROM stock_price_hynix WHERE 종목코드="%s"' % row['종목코드'], conn)
            last_date = datetime(1900, 1, 1)
            if df_max['maxdate'].iloc[0] != None:
                last_date = datetime.strptime(df_max['maxdate'].iloc[0], "%Y-%m-%d %H:%M:%S")
            start = last_date + timedelta(1)

            # end: 전일
            yday = datetime.today() - timedelta(1)
            end = datetime(yday.year, yday.month, yday.day)

            print("on going...")
            df_price = get_data_naver(row['종목코드'], start, end)
            df_price['종목코드'] = row['종목코드']
            df_price.to_sql('stock_price_hynix', conn, if_exists='append', index=False)
            print('%d rows' % len(df_price))

        else:
            print("접합하지 않은 코드입니다.")
    conn.close()