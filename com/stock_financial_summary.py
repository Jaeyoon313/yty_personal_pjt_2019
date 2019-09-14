import re
import pandas as pd
import sqlite3
import requests
from bs4 import BeautifulSoup
from io import StringIO

'''
get_date_str(s) - 문자열 s 에서 "YYYY/MM" 문자열 추출
'''


def get_date_str(s):
    date_str = ''
    r = re.search("\d{4}/\d{2}", s)
    if r:
        date_str = r.group()
        date_str = date_str.replace('/', '-')

    return date_str


'''
* code: 종목코드
* fin_type = '0': 재무제표 종류 (0: 주재무제표, 1: GAAP개별, 2: GAAP연결, 3: IFRS별도, 4:IFRS연결)
* freq_type = 'Y': 기간 (Y:년, Q:분기)
'''


def get_finstate_naver(code, fin_type='0', freq_type='Y'):
    # encparam, encid  추출
    url_tmpl = 'http://companyinfo.stock.naver.com/v1/company/c1010001.aspx?cmp_cd=%s'
    url = url_tmpl % (code)

    html_text = requests.get(url).text
    if not re.search("encparam: '(.*?)'", html_text):
        print('encparam not found')
        return None
    encparam = re.findall("encparam: '(.*?)'", html_text)[0]
    encid = re.findall("id: '(.*?)'", html_text)[0]

    #  재무데이터 표 추출
    url_tmpl = 'http://companyinfo.stock.naver.com/v1/company/ajax/cF1001.aspx?' \
               'cmp_cd=%s&fin_typ=%s&freq_typ=%s&encparam=%s&id=%s'

    url = url_tmpl % (code, fin_type, freq_type, encparam, encid)

    header = {
        'Referer': 'https://companyinfo.stock.naver.com/v1/company/c1010001.aspx',
    }

    html_text = requests.get(url, headers=header).text
    dfs = pd.read_html(StringIO(html_text))
    #print(dfs)
    df = dfs[1]
    if df.iloc[0, 0].find('해당 데이터가 존재하지 않습니다') >= 0:
        return None
    print(df.columns)
    cols = list(df.columns)
    new_cols = []
    #new_cols.append(cols[0][0])
    #print(cols[0][0])
    #print(cols[1][0])
    #print(cols[0][1])
    #print(cols[1][1])
    new_cols += [c[1] for c in cols[:9]]
    df.columns = new_cols
    #print("new_cols")
    #print(new_cols)
    df.rename(columns={'주요재무정보': 'date'}, inplace=True)

    df.set_index(df.columns[0], inplace=True)
    #print("df.index")
    #print(df.columns[0])

    df.columns = [get_date_str(x) for x in df.columns]

    dft = df.T
    dft.index = pd.to_datetime(dft.index)
    #print("dft.index : ")
    #print(dft.index)
    dft['날짜'] = dft.index
    print("----------------------------------------------------------------------------------------------")
    # remove if index is NaT
    dft = dft[pd.notnull(dft.index)]
    print(dft)

    return dft


if __name__ == "__main__":
    conn = sqlite3.connect('stock_finstate.db')
    conn.execute("attach database 'stock_master.db' as master")

    create_sql = """
    CREATE TABLE IF NOT EXISTS "stock_finstate_hynix" (
        "날짜" DATETIME,
        "종목코드" TEXT,
        "종목명" TEXT,
        "종류" TEXT, -- 연결/개별
        "기간" TEXT, -- 분기/년
        "매출액" REAL,
        "영업이익" REAL,
        "영업이익(발표기준)" REAL,
        "세전계속사업이익" REAL,
        "당기순이익" REAL,
        "당기순이익(지배)" REAL,
        "당기순이익(비지배)" REAL,
        "자산총계" REAL,
        "부채총계" REAL,
        "자본총계" REAL,
        "자본총계(지배)" REAL,
        "자본총계(비지배)" REAL,
        "자본금" REAL,
        "영업활동현금흐름" REAL,
        "투자활동현금흐름" REAL,
        "재무활동현금흐름" REAL,
        "CAPEX" REAL,
        "FCF" REAL,
        "이자발생부채" REAL,
        "영업이익률" REAL,
        "순이익률" REAL,
        "ROE(%)" REAL,
        "ROA(%)" REAL,
        "부채비율" REAL,
        "자본유보율" REAL,
        "EPS(원)" REAL,
        "PER(배)" REAL,
        "BPS(원)" REAL,
        "PBR(배)" REAL,
        "현금DPS(원)" REAL,
        "현금배당수익률" REAL,
        "현금배당성향(%)" REAL,
        "발행주식수(보통주)" REAL,
        UNIQUE("날짜", "종목코드", "종류", "기간") ON CONFLICT REPLACE
    );
    """

    # conn.execute("DROP TABLE stock_finstate") # 필요시 삭제
    conn.execute(create_sql)
    conn.execute('CREATE INDEX IF NOT EXISTS "ix_instate_date" ON "stock_finstate_hynix" ("날짜")')
    conn.execute('CREATE INDEX IF NOT EXISTS "ix_instate_code" ON "stock_finstate_hynix" ("종목코드")')
    conn.execute('CREATE INDEX IF NOT EXISTS "ix_instate_code" ON "stock_finstate_hynix" ("종류")')
    conn.execute('CREATE INDEX IF NOT EXISTS "ix_instate_code" ON "stock_finstate_hynix" ("기간")')

    # 전종목 (종목코드, 종목명) 읽기
    #df_master = pd.read_sql("SELECT 종목코드, 종목명 FROM stock_master WHERE 종목명 = '삼성전자'", conn)
    df_master = pd.read_sql("SELECT 종목코드, 종목명 FROM stock_master WHERE 종목코드 = '000660'", conn)
    stock_counts = len(df_master)
    msg_text = {'0': '주 재무제표', '3': 'IFRS개별', '4': 'IFRS연결', 'Q': '분기', 'Y': '년'}

    for ix, row in df_master.iterrows():  # 모든 종목에 대해
        for fin_type, freq_type in [('0', 'Q'), ('0', 'Y'), ('3', 'Q'), ('3', 'Y'), ('4', 'Q'),
                                    ('4', 'Y')]:  # IFRS개별/IFRS연결, 분기/년 각각 조합
            # check if data exists
            #print("##################################################################################################")
            print(ix + 1, '/', stock_counts, row['종목코드'], row['종목명'], msg_text[fin_type], msg_text[freq_type])

            df_fs = get_finstate_naver(row['종목코드'], fin_type=fin_type, freq_type=freq_type)
            #print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

            if df_fs is None:  # '데이터가 존재하지 않습니다'
                print('데이터가 존재하지 않습니다')
                continue
            df_fs['종목코드'] = row['종목코드']
            df_fs['종목명'] = row['종목명']
            df_fs['종류'] = fin_type
            df_fs['기간'] = freq_type
            print(df_fs)
            df_fs.to_sql('stock_finstate_hynix', conn, index=False, if_exists='append')