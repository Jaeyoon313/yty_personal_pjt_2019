import sqlite3
import pandas as pd
import ssl
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
from operator import eq
from urllib import request



def get_last_page_num(code):
    npage = 1
    url = 'http://finance.naver.com/item/frgn.nhn?code=%s&page=1' % (code)

    r = requests.get(url)
    soup = BeautifulSoup(r.text, "lxml")
    td = soup.find('td', attrs={'class': 'pgRR'})
    if td:
        npage = td.a['href'].split('page=')[1]
    return int(npage)


def get_investor_trend_data_naver(code, start=datetime(1900, 1, 1), end=datetime(2100, 1, 1)):
    url_tmpl = 'http://finance.naver.com/item/frgn.nhn?code=%s&page=%d'

    npages = get_last_page_num(code)
    df_investor_trend = pd.DataFrame()
    print("#########nPages : " + str(npages))

    if npages == 1:
        return -1;

    for p in range(1, npages + 1):
        url = url_tmpl % (code, p)
        #dfs = pd.read_html(url)

        #mac python
        context = ssl._create_unverified_context()
        response = request.urlopen(url, context=context)
        html = response.read()
        #\mac python

        #window -> dfs = pd.read_html(url)
        #mac    -> dfs = pd.read_html(html)
        dfs = pd.read_html(html)

        # first page
        df = dfs[2]
        df.columns = ['날짜', '종가', '전일비', '등락률', '거래량', '기관_순매매량', '외국인_순매매량', '외국인_보유주수', '외국인_보유율']
        df = df[0:].copy()
        df.dropna(inplace=True)
        df = df.replace('\.', '-', regex=True)

        # 몇몇 종목에서는 페이지에 아무런 내용이 없는 경우가 있고
        # 이 경우에는 하기 type을 float으로 인식해 오류가 남  해당 오류 제거를 위해서 하기 if문 삽입.
        if isinstance(df['날짜'].max(), str) == 0:
            break

        # select date range
        start_str = start.strftime("%Y-%m-%d")
        print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
        print(type(start_str))
        print(start_str)
        end_str = end.strftime("%Y-%m-%d")
        # print(end_str)
        print("#############################################")
        print(type(df['날짜'].max()))

        mask = (df['날짜'] >= start_str) & (df['날짜'] <= end_str)
        df_in = df[mask]

        # merge dataframe
        df_investor_trend = df_investor_trend.append(df_in)
        print('%d,' % p, end='', flush=True)
        print(df['날짜'].max())
        print(len(df))
        # print(start_str)
        if len(df) <= 0 or df['날짜'].max() <= start_str:
            print("break")
            break
    print()
    #df_investor_trend['날짜'] = pd.to_datetime(df_investor_trend['날짜'])
    #이상하게도 mac 환경에서는 pd.to_datetime function 사용시 최종적으로 담으면 날짜형식이 깨지게 된다...!

    df_investor_trend['등락률'] = df_investor_trend['등락률'].str[-8:-4] + '.' + df_investor_trend['등락률'].str[-3: -1]
    df_investor_trend['외국인_보유율'] = df_investor_trend['외국인_보유율'].str[-8:-4] + '.' + df_investor_trend['외국인_보유율'].str[
                                                                                   -3: -1]
    print(df_investor_trend['등락률'])
    print(df_investor_trend['날짜'])
    int_cols = ['날짜', '종가', '전일비', '등락률', '거래량', '기관_순매매량', '외국인_순매매량', '외국인_보유주수', '외국인_보유율']
    df_investor_trend[int_cols] = df_investor_trend[int_cols].astype('int', errors='ignore')
    print("aaaaaaaaaaaaaaaaaa")
    print(df_investor_trend['날짜'])
    return df_investor_trend


if __name__ == "__main__":
    conn = sqlite3.connect('investor_trend_hynix.db')
    create_sql = """
    CREATE TABLE IF NOT EXISTS "investor_trend_hynix" (
        "날짜" DATETIME,
        "종목코드" TEXT,
		"종목명" TEXT,
        "종가" BIGINT,
        "전일비" BIGINT, 
        "등락률" BIGINT, 
        "거래량" BIGINT,
        "기관_순매매량" BIGINT,
		"외국인_순매매량" BIGINT,
		"외국인_보유주수" UNSIGNED BIG INT,
        "외국인_보유율" BIGINT, 
        UNIQUE("날짜", "종목코드") ON CONFLICT REPLACE
    );
    """
    conn.execute(create_sql)
    conn.execute('CREATE INDEX IF NOT EXISTS ix_price_date on investor_trend_hynix(날짜);')
    conn.execute('CREATE INDEX IF NOT EXISTS ix_price_code on investor_trend_hynix(종목코드);')

    # 종목마스터 ('stock_master.db') 연결
    conn.execute("attach database 'stock_master.db' as master")

    #df_master = pd.read_sql("SELECT * FROM stock_master", conn)
    df_master = pd.read_sql("SELECT * FROM stock_master WHERE 종목코드 = '000660'", conn)
for inx, row in df_master.iterrows():
    print(row['종목코드'], row['종목명'])
    print("문자열 길이 : " + str(len(row['종목코드'])))
    if eq(len(row['종목코드']), 6):
        print("적합한 코드입니다")
        #  start: DB에 저장된 마지막 날짜 + 1일
        df_max = pd.read_sql('SELECT MAX (날짜) AS "maxdate" FROM investor_trend_hynix WHERE 종목코드="%s"' % row['종목코드'], conn)

        last_date = datetime(1900, 1, 1)
        if df_max['maxdate'].iloc[0] != None:
            last_date = datetime.strptime(df_max['maxdate'].iloc[0], "%Y-%m-%d %H:%M:%S")
        start = last_date + timedelta(1)

        # end: 전일
        yday = datetime.today() - timedelta(1)
        end = datetime(yday.year, yday.month, yday.day)

        df_investor_trend = get_investor_trend_data_naver(row['종목코드'], start, end)

        print(type(df_investor_trend))
        print("a---------------------")
        print(df_investor_trend['날짜'])
        if isinstance(df_investor_trend, int):
            print("외국인/기관 거래내역 없음")
        else:
            df_investor_trend['종목코드'] = row['종목코드']
            df_investor_trend['종목명'] = row['종목명']

            df_investor_trend.to_sql('investor_trend_hynix', conn, if_exists='append', index=False)
            print('%d rows' % len(df_investor_trend))

    else:
        print("접합하지 않은 코드입니다.")
conn.close()