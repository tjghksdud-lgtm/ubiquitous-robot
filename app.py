import os, json, time, threading
from collections import defaultdict, deque
from datetime import datetime, timezone
import pandas as pd
import streamlit as st
import websocket

st.set_page_config(page_title='Penny Radar V1', page_icon='📈', layout='wide', initial_sidebar_state='collapsed')
st.markdown('''<style>
@media (max-width: 700px) {
 .block-container {padding: 1rem .7rem 2rem;}
 h1 {font-size: 1.65rem !important;}
 [data-testid="stDataFrame"] {font-size: .8rem;}
 div[data-testid="stMetricValue"] {font-size: 1.4rem;}
}
</style>''', unsafe_allow_html=True)

@st.cache_resource
def state():
    return {'lock':threading.Lock(),'bars':defaultdict(lambda:deque(maxlen=90)), 'last':None,'status':'연결 전','running':False,'feed':None}
S=state()

def start(key,secret,feed):
    if S['running']: return
    S['running']=True; S['feed']=feed
    def worker():
        url=f'wss://stream.data.alpaca.markets/v2/{feed}'
        def on_open(ws):
            with S['lock']: S['status']='인증 중'
            ws.send(json.dumps({'action':'auth','key':key,'secret':secret}))
        def on_message(ws,msg):
            try: events=json.loads(msg)
            except Exception: return
            for e in events:
                if e.get('T')=='success' and e.get('msg')=='authenticated':
                    ws.send(json.dumps({'action':'subscribe','bars':['*']}))
                    with S['lock']: S['status']='실시간 수신 중'
                elif e.get('T')=='error':
                    with S['lock']: S['status']=f"오류: {e.get('msg',e)}"
                elif e.get('T')=='b':
                    sym=e.get('S'); close=e.get('c'); vol=e.get('v'); vw=e.get('vw')
                    if sym and close and vol is not None:
                        with S['lock']:
                            S['bars'][sym].append({'t':e.get('t'),'c':float(close),'v':int(vol),'vw':float(vw or close)})
                            S['last']=datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        def on_error(ws,error):
            with S['lock']: S['status']=f'연결 오류: {error}'
        def on_close(ws,*args):
            with S['lock']: S['status']='연결 종료'; S['running']=False
        websocket.WebSocketApp(url,on_open=on_open,on_message=on_message,on_error=on_error,on_close=on_close).run_forever(ping_interval=25,ping_timeout=10)
    threading.Thread(target=worker,daemon=True).start()

def scan(lo,hi,min_dollar):
    rows=[]
    with S['lock']:
        snapshot={k:list(v) for k,v in S['bars'].items()}
    for sym,b in snapshot.items():
        if len(b)<2: continue
        price=b[-1]['c']
        if not lo<=price<=hi: continue
        recent=b[-5:]; prev=b[:-5] or b[:1]
        vol5=sum(x['v'] for x in recent)
        baseline=sum(x['v'] for x in prev)/len(prev)
        rvol=(vol5/len(recent))/max(1,baseline)
        change=(price/b[0]['c']-1)*100 if b[0]['c'] else 0
        dollar=sum(x['c']*x['v'] for x in recent)
        if dollar<min_dollar: continue
        vwap=sum(x['vw']*x['v'] for x in b)/max(1,sum(x['v'] for x in b))
        score=min(100,round(max(0,change)*2+min(30,rvol*7)+(15 if price>vwap else 0)+min(20,dollar/25000)))
        rows.append({'종목':sym,'현재가($)':round(price,4),'관측 상승률(%)':round(change,2),'5분 상대거래량':round(rvol,2),'5분 거래대금($)':round(dollar),'VWAP($)':round(vwap,4),'점수':score,'수집 봉 수':len(b)})
    return pd.DataFrame(sorted(rows,key=lambda x:x['점수'],reverse=True))

st.title('📈 Penny Radar V1')
st.caption('미국 초저가·소형주 탐지 · 실제 데이터 연결 전에는 결과가 표시되지 않습니다 · 투자 추천 아님')
with st.sidebar:
    st.header('실시간 데이터 연결')
    # 배포 시 Streamlit secrets 또는 서버 환경변수로 키를 설정할 수 있습니다.
    saved_key=st.secrets.get('ALPACA_API_KEY',os.getenv('ALPACA_API_KEY',''))
    saved_secret=st.secrets.get('ALPACA_API_SECRET',os.getenv('ALPACA_API_SECRET',''))
    key=saved_key or st.text_input('Alpaca API Key',type='password')
    secret=saved_secret or st.text_input('Alpaca API Secret',type='password')
    if saved_key and saved_secret: st.caption('서버에 저장된 API 설정 사용 중')
    feed=st.selectbox('피드',['iex','sip'],help='IEX는 전체 미국 거래량이 아닙니다. SIP는 별도 유료 권한이 필요할 수 있습니다.')
    if st.button('연결 시작',disabled=S['running']):
        if key and secret: start(key,secret,feed)
        else: st.warning('API 키 두 개를 입력하세요.')
    st.divider()
    low=st.number_input('최소 주가 ($)',value=0.10,min_value=0.01,step=0.05)
    high=st.number_input('최대 주가 ($)',value=5.0,min_value=0.1,step=0.5)
    min_dollar=st.number_input('최근 5개 봉 최소 거래대금 ($)',value=10000,step=5000)
    st.caption('새로고침 버튼으로 최신 데이터를 조회하세요. 서버가 실행 중이어야 수집이 유지됩니다.')
st.write('연결 상태:',S['status'],'| 마지막 수신:',S['last'] or '없음')
if st.button('🔄 결과 새로고침',use_container_width=True): st.rerun()
df=scan(low,high,min_dollar)
if df.empty: st.info('아직 조건에 맞는 데이터가 없습니다. 연결 후 몇 분간 봉을 수집하세요.')
else:
    st.metric('탐지 종목 수',len(df))
    st.dataframe(df,use_container_width=True,hide_index=True)
    selected=st.selectbox('종목별 가격 흐름',df['종목'].tolist())
    with S['lock']: b=list(S['bars'][selected])
    st.line_chart(pd.DataFrame(b).set_index('t')[['c','vw']])
st.warning('점수는 임의로 설계한 탐지 우선순위이며, 상승 확률이나 검증된 예측 모델이 아닙니다. 상승률은 연결 후 수집된 첫 봉 대비이며 전일 종가 대비가 아닙니다. 상대거래량은 수집 구간 내부 비교로, 과거 동시간대 기준 RVOL이 아닙니다. 장전·장후 데이터는 공급사/피드 지원 범위에 따릅니다.')
