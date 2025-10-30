import streamlit as st
import pandas as pd
import geopandas as gpd
import pydeck as pdk
from typing import Optional
import logging

# utils.pyから関数と設定をインポート
from utils import (
    generate_sql,
    execute_query,
    get_all_data,
    calculate_derived_metrics,
    generate_interpretation,
    generate_contextual_explanation,
    get_top_bottom_insights,
    extract_query_parameters,
    detect_metric_question,
    load_geojson_data,
    get_db_connection,
    MODEL_CONFIG  # MODEL_CONFIGをインポート
)

# ロギング設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- 初期設定 ---
st.set_page_config(page_title="八王子市 事業者数分析", layout="wide")
st.title("🏢 自然言語で八王子市の事業者データを分析")

# --- サイドバー ---
st.sidebar.title("⚙️ 設定")

# モデル選択
# utilsからインポートしたMODEL_CONFIGを使って選択肢を動的に生成
model_options = {config["label"]: model_name for model_name, config in MODEL_CONFIG.items()}

selected_model_label = st.sidebar.selectbox(
    "🤖 AIモデルを選択",
    options=list(model_options.keys()),
    help="質問を解釈し、SQLを生成するAIモデルを選択します。"
)

# ラベルからモデル名を取得してセッション状態に保存
st.session_state.model_name = model_options[selected_model_label]

st.sidebar.markdown("---")
st.sidebar.info("APIキーはStreamlitのSecretsに設定してください。\n- `GOOGLE_API_KEY`\n- `OPENROUTER_API_KEY`")


with st.expander("📘 使い方とデータについて"):
    st.markdown("""
    このアプリでは、八王子市の事業者に関する統計データについて、自然言語で質問することができます。
    AIがあなたの質問を解釈してSQLクエリを生成し、データベースから結果を取得・表示します。

    **利用可能なデータ**
    - **事業者統計データ (`business_stats`)**:
        - **対応年度**: 2015年～2024年
        - **事業種別**: 農林漁業, 建設業, 製造業, 情報通信業, 卸売業_小売業, 宿泊業_飲食サービス業, 医療_福祉, 金融業_保険業, 不動産業_物品賃貸業, 電気･ガス･熱供給･水道業, 運輸業_郵便業, 学術研究_専門･技術サービス業, 鉱業_採石業_砂利採取業, 生活関連サービス業_娯楽業, 教育_学習支援業, 複合サービス事業, サービス業（他に分類されないもの）, 公務（他に分類されるものを除く）
        - **カラム**: 年度、町名、事業種別、事業所数、従業者数
    - **人口統計データ (`population`)**:
        - **カラム**: 年度、町名、世帯数、人口数、男性数、女性数

    **質問の例**
    - `2021年の町名別で、建設業の事業所数が多いトップ5を教えて`
    - `情報通信業の事業所数が最も多い年度は？`
    - `八王子市全体の従業員数の推移を年度別に教えて`
    - `2022年の人口に対する事業者数の割合が高い町はどこ？`

    **ご注意**
    - AIが生成するSQLは必ずしも正確ではありません。意図した通りの結果が得られない場合は、質問の仕方を変えてみてください。
    """)

# --- UI部分 ---
st.markdown("---")

# セッション状態の初期化
if 'user_question' not in st.session_state:
    st.session_state.user_question = "町名毎に2021年の全事業所密度（事業所数÷世帯数）と従業者比率（従業者数÷人口数）を比較して"
if 'result_df' not in st.session_state:
    st.session_state.result_df = None
if 'generated_sql' not in st.session_state:
    st.session_state.generated_sql = None
if 'metrics_df' not in st.session_state:
    st.session_state.metrics_df = None
if 'is_metric_question' not in st.session_state:
    st.session_state.is_metric_question = False
if 'query_params' not in st.session_state:
    st.session_state.query_params = {}

# サンプル質問ボタン
st.subheader("💡 質問例")
col1, col2, col3 = st.columns(3)
with col1:
    if st.button("🏗️ 建設業トップ5"):
        st.session_state.user_question = "2015年の建設業の事業所数が多い町名トップ5"
with col2:
    if st.button("📈 従業員数推移"):
        st.session_state.user_question = "旭町の年度別の全従業員数の推移"
with col3:
    if st.button("🏘️ 事業所密度分析"):
        st.session_state.user_question = "2024年の町名毎の事業所密度を教えて"

# 質問入力（セッション状態と直接バインド）
user_question = st.text_input("🔍 分析したい内容を質問してください:", key="user_question")

if st.button("🚀 分析を実行", type="primary"):
    if user_question:
        with st.spinner(f"🤖 AI ({MODEL_CONFIG[st.session_state.model_name]['label']}) がSQLを生成中..."):
            generated_sql = generate_sql(user_question, st.session_state.model_name)

        if generated_sql:
            st.session_state.generated_sql = generated_sql
            
            with st.spinner("💾 データベースでクエリを実行中..."):
                result_df = execute_query(generated_sql)
                st.session_state.result_df = result_df
                st.session_state.is_metric_question = detect_metric_question(user_question)

            if st.session_state.is_metric_question and result_df is not None and not result_df.empty:
                with st.spinner("📊 派生指標を計算中..."):
                    query_params = extract_query_parameters(generated_sql, user_question)
                    population_df = get_all_data('population')
                    business_df = get_all_data('business_stats')

                    if population_df is not None and business_df is not None:
                        metrics_df = calculate_derived_metrics(
                            business_df, 
                            population_df,
                            year=query_params['year'],
                            industry=query_params['industry'],
                            town=query_params['town']
                        )
                        st.session_state.metrics_df = metrics_df
                        st.session_state.query_params = query_params
                    else:
                        st.session_state.metrics_df = None
                        st.session_state.query_params = {}
            else:
                st.session_state.metrics_df = None
                st.session_state.query_params = {}
    else:
        st.warning("⚠️ 質問を入力してください。")

# --- 結果表示（セッション状態から復元） ---
if st.session_state.generated_sql:
    with st.expander("📝 生成されたSQLクエリ", expanded=False):
        st.code(st.session_state.generated_sql, language="sql")

if st.session_state.result_df is not None and not st.session_state.result_df.empty:
    result_df = st.session_state.result_df
    
    st.success(f"✅ クエリ結果 ({len(result_df)}行)")
    st.dataframe(result_df, use_container_width=True)

    if st.session_state.is_metric_question and st.session_state.metrics_df is not None:
        metrics_df = st.session_state.metrics_df
        query_params = st.session_state.get('query_params', {})
        
        if not metrics_df.empty:
            st.markdown("---")
            
            filter_info = []
            if query_params.get('year'):
                filter_info.append(f"📅 **{query_params['year']}年度**")
            if query_params.get('industry'):
                filter_info.append(f"🏢 **{query_params['industry']}**")
            if query_params.get('town'):
                filter_info.append(f"📍 **{query_params['town']}**")
            
            if filter_info:
                st.info(f"🔍 **分析対象**: {' / '.join(filter_info)} のデータに基づいて指標を計算しました")
            
            context_explanation = generate_contextual_explanation(user_question, metrics_df)
            st.info(f"📊 **分析の背景**\n\n{context_explanation}")
            
            st.subheader("📊 経済指標の詳細分析")

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                avg_density = metrics_df['office_density'].mean()
                st.metric("平均事業所密度", f"{avg_density:.4f}", help="事業所数 ÷ 世帯数\n\n値が高いほど、世帯数に対して事業所が多い（商業地域的）")
            with col2:
                avg_ratio = metrics_df['employee_ratio'].mean()
                st.metric("平均従業者比率", f"{avg_ratio:.4f}", help="従業者数 ÷ 人口数\n\n値が高いほど、人口に対して働く人が多い（雇用が活発）")
            with col3:
                avg_size = metrics_df['office_size'].mean()
                st.metric("平均事業所規模", f"{avg_size:.1f}人", help="従業者数 ÷ 事業所数\n\n値が大きいほど、1事業所あたりの従業員が多い（大規模事業所）")
            with col4:
                avg_per_1000 = metrics_df['offices_per_1000_pop'].mean()
                st.metric("人口1000人あたり事業所数", f"{avg_per_1000:.1f}", help="(事業所数 ÷ 人口) × 1000\n\n国際比較などで使われる標準指標")

            interpretation = generate_interpretation(metrics_df)
            
            insights = ""
            if 'town_name' in metrics_df.columns and len(metrics_df['town_name'].unique()) > 1:
                insights += get_top_bottom_insights(metrics_df, 'office_density', '事業所密度', n=3)
            
            full_interpretation = f"💡 **データから読み取れること**\n\n{interpretation}{insights}"
            st.success(full_interpretation)

            with st.expander("📋 詳細な指標データを表示", expanded=False):
                display_cols = ['year', 'town_name', 'num_offices', 'num_employees', 'num_households', 'num_population',
                              'office_density', 'employee_ratio', 'office_size', 'offices_per_1000_pop']
                available_cols = [col for col in display_cols if col in metrics_df.columns]
                
                col_rename = {'year': '年度', 'town_name': '町名', 'num_offices': '事業所数', 'num_employees': '従業者数', 'num_households': '世帯数', 'num_population': '人口数', 'office_density': '事業所密度', 'employee_ratio': '従業者比率', 'office_size': '事業所規模', 'offices_per_1000_pop': '人口千人あたり事業所数'}
                
                display_df = metrics_df[available_cols].copy().rename(columns=col_rename)
                
                st.dataframe(display_df.round(4), use_container_width=True, hide_index=True)
                
                csv = display_df.to_csv(index=False).encode('utf-8-sig')
                st.download_button("📥 CSVでダウンロード", csv, "hachioji_metrics.csv", "text/csv", key='download-csv')

    if len(result_df.columns) >= 2:
        try:
            numeric_cols = result_df.select_dtypes(include=['number']).columns.tolist()
            category_cols = result_df.select_dtypes(include=['object']).columns.tolist()

            if category_cols and numeric_cols:
                st.subheader("📈 データ可視化")
                chart_df = result_df.set_index(category_cols[0])[numeric_cols[0]]
                st.bar_chart(chart_df)
        except Exception as e:
            logger.warning(f"グラフ描画スキップ: {e}")

    if result_df is not None and not result_df.empty:

        numeric_cols = result_df.select_dtypes(include=['number']).columns.tolist()
        if 'town_name' in result_df.columns and len(numeric_cols) > 0:
            st.subheader("🗺️ 地図で結果を確認")
            
            metric_to_map = st.selectbox("地図に表示する指標を選択してください:", options=numeric_cols, index=0)

            with st.spinner("🗺️ 地図データを生成中..."):
                gdf = load_geojson_data()
                if gdf is not None:
                    map_df = gdf.merge(result_df, on='town_name', how='inner')

                    if not map_df.empty:
                        max_val = map_df[metric_to_map].max()
                        min_val = map_df[metric_to_map].min()
                        
                        if max_val > min_val:
                            map_df['normalized'] = (map_df[metric_to_map] - min_val) / (max_val - min_val)
                        else:
                            map_df['normalized'] = 0.5
                        
                        def get_color(normalized_value):
                            r = int(255 * (1 - normalized_value))
                            g = int(255 * normalized_value)
                            b = 0
                            return [r, g, b, 180]
                        
                        map_df['fill_color'] = map_df['normalized'].apply(get_color)
                        
                        st.pydeck_chart(pdk.Deck(
                            map_style=None,
                            initial_view_state=pdk.ViewState(latitude=35.655, longitude=139.33, zoom=11, pitch=0),
                            layers=[
                                pdk.Layer('PolygonLayer', data=map_df, get_polygon='coordinates', filled=True, stroked=True, get_fill_color='fill_color', get_line_color=[80, 80, 80], line_width_min_pixels=1, pickable=True, auto_highlight=True)
                            ],
                            tooltip={"html": f"<b>町名:</b> {{town_name}}<br/><b>{metric_to_map}:</b> {{{metric_to_map}}}", "style": {"backgroundColor": "steelblue", "color": "white"}}
                        ))
                        
                        st.caption(f"🎨 色の凡例: 赤（低い値: {min_val:.2f}）→ 黄色（中間）→ 緑（高い値: {max_val:.2f}）")
                    else:
                        st.warning("⚠️ 地図データと結合できる町名が見つかりませんでした。")
                else:
                    st.error("❌ 地図データの読み込みに失敗しました。")

elif st.session_state.result_df is not None:
    st.warning("⚠️ 結果が0件でした。質問を変えてみてください。")

# フッター
st.markdown("---")
st.caption(f"💡 Powered by {MODEL_CONFIG[st.session_state.model_name]['label']} & DuckDB | 八王子市オープンデータを活用")