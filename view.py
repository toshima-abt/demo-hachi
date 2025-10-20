import streamlit as st
import folium
from streamlit_folium import st_folium
import pandas as pd
from utils import (
    generate_contextual_explanation, 
    generate_interpretation, 
    generate_ai_summary,
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
    """ サンプル質問の選択UIを表示（犯罪データ対応版） """
    st.subheader("💡 サンプル質問を選択")

    sample_questions = {
        "🏗️ 建設業トップ10": "2015年の町名毎の建設業の事業所数トップ10",
        "📈 従業員数推移": "旭町の年度別の全従業員数の推移",
        "🏘️ 事業所密度分析": "2024年の町名毎の事業所密度を教えて",

        # --- 新規追加: 犯罪関連を含む統計的に興味深い質問 ---
        "🔍 世帯数あたりの犯罪発生率": "2020年の町名ごとの世帯数あたり犯罪発生率を教えて",
        "🍽️ 飲食業と犯罪件数の関係": "飲食業の事業所数が多い町ほど犯罪件数は多いですか？",
        "📊 犯罪件数の推移": "2010年から2020年の間で犯罪件数が最も増えた町を教えて",
        "🏢 犯罪と産業構造": "犯罪件数が多い地域ではどの産業の事業所が多いですか？",
        "🏙️ 犯罪と世帯数の関係": "世帯数が多い地域ほど犯罪件数が多い傾向はありますか？",
        "🔎 治安改善エリア": "ここ5年間で犯罪件数が減少している地域とその比率を教えて",
    }

    # 選択メニュー
    selected_question = st.selectbox(
        "質問例から選んでみましょう：",
        options=["（選択しない）"] + list(sample_questions.keys())
    )

    # 実行ボタン
    if selected_question != "（選択しない）":
        if st.button("🧠 この質問を登録"):
            st.session_state.user_question = sample_questions[selected_question]
            st.rerun()



def render_main_form():
    """ メインの質問入力フォームを表示 """
    st.text_input("🔍 分析したい内容を質問してください:", key="user_question")
    st.button("🚀 分析を実行", type="primary", key="run_analysis_button")

def render_results(result_df, generated_sql, user_question):
    """ SQLとクエリ結果のデータフレームを表示 """
    if generated_sql:
        with st.expander("📝 生成されたSQLクエリ", expanded=False):
            st.code(generated_sql, language="sql")

    if result_df is not None and not result_df.empty:
        st.success(f"✅ クエリ結果 ({len(result_df)}行)")
        st.dataframe(result_df, use_container_width=True)
    elif result_df is not None:
        st.warning("⚠️ 結果が0件でした。質問を変えてみてください。")

    if result_df is not None and not result_df.empty:
        with st.expander("🤖 AIによる分析コメント", expanded=True):
            ai_comment = generate_ai_summary(result_df, user_question)
            st.markdown(ai_comment)


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
                    import branca.colormap as cm
                    from folium import Element
                    
                    m = folium.Map(
                        location=[35.655, 139.33], 
                        zoom_start=11,
                        tiles='https://cyberjapandata.gsi.go.jp/xyz/std/{z}/{x}/{y}.png',
                        attr='国土地理院'
                    )
                    
                    # カラーマップの作成
                    values = map_df[metric_to_map].values
                    vmin, vmax = values.min(), values.max()
                    colormap = cm.LinearColormap(
                        colors=['#d73027', '#fee08b', '#1a9850'],
                        index=[vmin, (vmin + vmax) / 2, vmax],
                        vmin=vmin,
                        vmax=vmax,
                        caption=f'{metric_to_map} の値'
                    )
                    
                    # GeoJsonレイヤー（四角形なし、ポリゴン強調）
                    folium.GeoJson(
                        map_df,
                        style_function=lambda feature: {
                            'fillColor': colormap(feature['properties'][metric_to_map]),
                            'color': 'gray',
                            'weight': 1,
                            'fillOpacity': 0.7,
                        },
                        highlight_function=lambda feature: {
                            'fillColor': colormap(feature['properties'][metric_to_map]),
                            'color': 'blue',
                            'weight': 3,
                            'fillOpacity': 0.95,
                        },
                        tooltip=folium.GeoJsonTooltip(
                            fields=['town_name', metric_to_map],
                            aliases=['町名:', f'{metric_to_map}:'],
                            style=('background-color: white; color: black; '
                                'font-family: courier new; font-size: 12px; padding: 10px;')
                        )
                    ).add_to(m)
                    
                    colormap.add_to(m)

                    # CSSでクリック時の四角形（outline）を無効化
                    css_style = """
                    <style>
                    path.leaflet-interactive:focus {
                        outline: none !important;
                    }
                    </style>
                    """
                    m.get_root().html.add_child(Element(css_style))                    
                    
                    st_folium(m, use_container_width=True, returned_objects=[], key="hachioji_map")
                else:
                    st.warning("⚠️ 地図データと結合できる町名が見つかりませんでした。")
            else:
                st.error("❌ 地図データの読み込みに失敗しました。")
