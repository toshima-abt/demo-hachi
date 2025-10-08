import streamlit as st
import folium
from streamlit_folium import st_folium
import pandas as pd
from utils import (
    generate_contextual_explanation, 
    generate_interpretation, 
    get_top_bottom_insights,
    load_geojson_data
)

def render_header():
    """ タイトルと説明文を表示 """
    st.title("🏢 自然言語で八王子市の事業者データを分析")
    with st.expander("📘 使い方とデータについて"):
        st.markdown("""
        このアプリでは、八王子市の事業者に関する統計データについて、自然言語で質問することができます。
        AIがあなたの質問を解釈してSQLクエリを生成し、データベースから結果を取得・表示します。

        **利用可能なデータ**
        - **事業者統計データ (`business_stats`)**: 年度, 町名, 事業種別, 事業所数, 従業者数
        - **人口統計データ (`population`)**: 年度, 町名, 世帯数, 人口数, 男性数, 女性数

        **質問の例**
        - `2021年の町名別で、建設業の事業所数が多いトップ5を教えて`
        - `情報通信業の事業所数が最も多い年度は？`
        - `八王子市全体の従業員数の推移を年度別に教えて`
        """)

def render_sample_questions():
    """ サンプル質問ボタンを表示 """
    st.subheader("💡 質問例")
    col1, col2, col3 = st.columns(3)
    if col1.button("🏗️ 建設業トップ5"):
        st.session_state.user_question = "2015年の建設業の事業所数が多い町名トップ5"
        st.rerun()
    if col2.button("📈 従業員数推移"):
        st.session_state.user_question = "旭町の年度別の全従業員数の推移"
        st.rerun()
    if col3.button("🏘️ 事業所密度分析"):
        st.session_state.user_question = "2024年の町名毎の事業所密度を教えて"
        st.rerun()

def render_main_form():
    """ メインの質問入力フォームを表示 """
    st.text_input("🔍 分析したい内容を質問してください:", key="user_question")
    st.button("🚀 分析を実行", type="primary", key="run_analysis_button")

def render_results(result_df, generated_sql):
    """ SQLとクエリ結果のデータフレームを表示 """
    if generated_sql:
        with st.expander("📝 生成されたSQLクエリ", expanded=False):
            st.code(generated_sql, language="sql")

    if result_df is not None and not result_df.empty:
        st.success(f"✅ クエリ結果 ({len(result_df)}行)")
        st.dataframe(result_df, use_container_width=True)
    elif result_df is not None:
        st.warning("⚠️ 結果が0件でした。質問を変えてみてください。")

def render_metrics_and_insights(metrics_df, user_question, query_params):
    """ 派生指標とそれに関する洞察を表示 """
    if metrics_df is None or metrics_df.empty:
        return

    st.markdown("---")
    
    # フィルタ情報の表示
    filter_info = []
    if query_params.get('year'):
        filter_info.append(f"📅 **{query_params['year']}年度**")
    if query_params.get('industry'):
        filter_info.append(f"🏢 **{query_params['industry']}**")
    if query_params.get('town'):
        filter_info.append(f"📍 **{query_params['town']}**")
    if filter_info:
        st.info(f"🔍 **分析対象**: {' / '.join(filter_info)} のデータに基づいて指標を計算しました")

    # 分析の背景説明
    context_explanation = generate_contextual_explanation(user_question, metrics_df)
    st.info(f"📊 **分析の背景**\n\n{context_explanation}")
    
    st.subheader("📊 経済指標の詳細分析")

    # メトリクス表示
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("平均事業所密度", f"{metrics_df['office_density'].mean():.4f}", help="事業所数 ÷ 世帯数")
    col2.metric("平均従業者比率", f"{metrics_df['employee_ratio'].mean():.4f}", help="従業者数 ÷ 人口数")
    col3.metric("平均事業所規模", f"{metrics_df['office_size'].mean():.1f}人", help="従業者数 ÷ 事業所数")
    col4.metric("人口1000人あたり事業所数", f"{metrics_df['offices_per_1000_pop'].mean():.1f}", help="(事業所数 ÷ 人口) × 1000")

    # 解釈と洞察
    interpretation = generate_interpretation(metrics_df)
    insights = ""
    if 'town_name' in metrics_df.columns and len(metrics_df['town_name'].unique()) > 1:
        insights += get_top_bottom_insights(metrics_df, 'office_density', '事業所密度')
    
    st.success(f"💡 **データから読み取れること**\n\n{interpretation}{insights}")

    # 詳細データ
    with st.expander("📋 詳細な指標データを表示"):
        display_cols = ['year', 'town_name', 'num_offices', 'num_employees', 'num_households', 'num_population',
                      'office_density', 'employee_ratio', 'office_size', 'offices_per_1000_pop']
        available_cols = [col for col in display_cols if col in metrics_df.columns]
        col_rename = {'year': '年度', 'town_name': '町名', 'num_offices': '事業所数', 'num_employees': '従業者数',
                      'num_households': '世帯数', 'num_population': '人口数', 'office_density': '事業所密度',
                      'employee_ratio': '従業者比率', 'office_size': '事業所規模', 'offices_per_1000_pop': '人口千人あたり事業所数'}
        display_df = metrics_df[available_cols].rename(columns=col_rename)
        st.dataframe(display_df.round(4), use_container_width=True, hide_index=True)
        csv = display_df.to_csv(index=False).encode('utf-8-sig')
        st.download_button("📥 CSVでダウンロード", csv, "hachioji_metrics.csv", "text/csv")

def render_visualizations(result_df):
    """ グラフと地図を表示 """
    if result_df is None or result_df.empty:
        return

    st.markdown("---")
    st.subheader("📈 データ可視化")

    # グラフ表示
    try:
        numeric_cols = result_df.select_dtypes(include=['number']).columns.tolist()
        category_cols = result_df.select_dtypes(include=['object']).columns.tolist()
        if category_cols and numeric_cols:
            chart_df = result_df.set_index(category_cols[0])[numeric_cols[0]]
            st.bar_chart(chart_df)
        else:
            st.write("グラフ化に適したデータ（カテゴリと数値の組み合わせ）がありませんでした。")
    except Exception as e:
        st.warning(f"グラフ描画スキップ: {e}")

    # 地図表示
    numeric_cols = result_df.select_dtypes(include=['number']).columns.tolist()
    if 'town_name' in result_df.columns and len(numeric_cols) > 0:
        st.subheader("🗺️ 地図で結果を確認")
        metric_to_map = st.selectbox("地図に表示する指標を選択してください:", options=numeric_cols, index=0)

        with st.spinner("🗺️ 地図データを生成中..."):
            gdf = load_geojson_data()
            if gdf is not None:
                map_df = gdf.merge(result_df, on='town_name', how='inner')
                if not map_df.empty:
                    m = folium.Map(
                        location=[35.655, 139.33], 
                        zoom_start=11,
                        tiles='https://cyberjapandata.gsi.go.jp/xyz/std/{z}/{x}/{y}.png',
                        attr='国土地理院'
                    )
                    choropleth = folium.Choropleth(
                        geo_data=map_df,
                        name='choropleth',
                        data=map_df,
                        columns=['town_name', metric_to_map],
                        key_on='feature.properties.town_name',
                        fill_color='RdYlGn',
                        fill_opacity=0.7,
                        line_opacity=0.3,
                        line_color='blue',
                        line_weight=2.0,
                        legend_name=f'{metric_to_map} の値',
                        bins=8,
                    ).add_to(m)
                    folium.GeoJsonTooltip(
                        fields=['town_name', metric_to_map],
                        aliases=['町名:', f'{metric_to_map}:'],
                        style=('background-color: grey; color: white; font-family: courier new; font-size: 12px; padding: 10px;')
                    ).add_to(choropleth.geojson)
                    st_folium(m, use_container_width=True, returned_objects=[])
                else:
                    st.warning("⚠️ 地図データと結合できる町名が見つかりませんでした。")
            else:
                st.error("❌ 地図データの読み込みに失敗しました。")
