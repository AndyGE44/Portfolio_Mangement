import streamlit as st
import pandas as pd
import yfinance as yf
import plotly.express as px

# --- 1. 页面配置 ---
st.set_page_config(page_title="我的量化模拟器", layout="wide")

st.title("🚀 个人量化投资组合推演 (Demo版)")
st.markdown("""
这是一个基于 **Python + Streamlit** 构建的轻量级投资组合模拟器。
你可以输入美股代码（如 NVDA, AAPL），模拟买入并查看组合的实时价值和分布。
*注意：本演示仅在当前会话有效，刷新页面后数据将重置。*
""")

# --- 2. 初始化 Session State (内存数据库) ---
# 如果是第一次打开，初始化一个空的 DataFrame
if 'portfolio' not in st.session_state:
    st.session_state.portfolio = pd.DataFrame(columns=['Ticker', 'Qty', 'Cost_Price', 'Market_Price', 'Market_Value', 'P_L'])

# --- 3. 辅助函数：获取实时价格 ---
def get_stock_price(ticker):
    try:
        stock = yf.Ticker(ticker)
        # 获取最新一天的收盘价/当前价
        price = stock.history(period='1d')['Close'].iloc[-1]
        return price
    except:
        return None

# --- 4. 侧边栏：交易控制台 ---
with st.sidebar:
    st.header("🕹️ 模拟交易台")
    
    input_ticker = st.text_input("股票代码 (Ticker)", value="NVDA").upper()
    input_qty = st.number_input("买入数量 (Qty)", min_value=1, value=10)
    
    # 模拟买入按钮
    if st.button("💰 执行买入"):
        with st.spinner(f"正在获取 {input_ticker} 的实时价格..."):
            current_price = get_stock_price(input_ticker)
            
        if current_price:
            # 计算市值
            market_val = input_qty * current_price
            
            # 创建新持仓行
            new_row = pd.DataFrame({
                'Ticker': [input_ticker],
                'Qty': [input_qty],
                'Cost_Price': [current_price], # 假设以市价买入
                'Market_Price': [current_price],
                'Market_Value': [market_val],
                'P_L': [0.0] # 刚买入盈亏为0
            })
            
            # 合并到总持仓 (简单追加模式，如果要合并同类项可以加逻辑)
            st.session_state.portfolio = pd.concat([st.session_state.portfolio, new_row], ignore_index=True)
            st.success(f"成功买入 {input_qty} 股 {input_ticker} @ ${current_price:.2f}")
        else:
            st.error("❌ 找不到该股票代码，请检查拼写。")

    st.markdown("---")
    if st.button("🔄 重置所有持仓"):
        st.session_state.portfolio = st.session_state.portfolio[0:0] # 清空
        st.rerun()

# --- 5. 主界面：仪表盘 ---

# 如果持仓不为空，展示数据
if not st.session_state.portfolio.empty:
    
    # --- 5.1 核心指标 (KPI) ---
    total_value = st.session_state.portfolio['Market_Value'].sum()
    total_cost = (st.session_state.portfolio['Qty'] * st.session_state.portfolio['Cost_Price']).sum()
    total_pl = total_value - total_cost
    pl_pct = (total_pl / total_cost) * 100 if total_cost > 0 else 0
    
    col1, col2, col3 = st.columns(3)
    col1.metric("总资产 (Total Assets)", f"${total_value:,.2f}")
    col2.metric("总投入 (Principal)", f"${total_cost:,.2f}")
    col3.metric("总盈亏 (P&L)", f"${total_pl:,.2f}", f"{pl_pct:.2f}%")
    
    st.markdown("---")
    
    # --- 5.2 图表区 ---
    c1, c2 = st.columns([2, 1])
    
    with c1:
        st.subheader("📊 持仓分布")
        # 按 Ticker 汇总数据画图
        chart_data = st.session_state.portfolio.groupby('Ticker')['Market_Value'].sum().reset_index()
        fig = px.pie(chart_data, values='Market_Value', names='Ticker', hole=0.4)
        st.plotly_chart(fig, use_container_width=True)
        
    with c2:
        st.subheader("📋 持仓明细")
        st.dataframe(
            st.session_state.portfolio[['Ticker', 'Qty', 'Market_Price', 'Market_Value']],
            use_container_width=True,
            hide_index=True
        )

else:
    st.info("👈 请在左侧侧边栏输入股票代码并点击“执行买入”开始推演。")
    # 放置一个空的占位图，让界面不那么空
    st.markdown("### 等待数据输入...")
