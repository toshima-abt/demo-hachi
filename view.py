import streamlit as st
import folium
from streamlit_folium import st_folium
import pandas as pd
from utils import (
    generate_contextual_explanation, 
    generate_interpretation, 
    generate_ai_summary,
    get_top_bottom_insights,
    load_geojson_data,
    get_yearly_business_summary,
    get_yearly_population_summary,
    get_yearly_crime_summary,
    get_available_years,
    get_town_business_data,
    get_town_population_data,
    get_town_crime_data
)
import branca.colormap as cm
from folium import Element

METRIC_NAME_MAPPING = {
    "num_offices": "事業所数",
    "num_employees": "従業者数",
    "num_households": "世帯数",
    "num_population": "人口",
    "crime_count": "犯罪件数",
    "office_density": "事業所密度",
    "employee_ratio": "従業者比率",
    "office_size": "事業所規模",
    "offices_per_1000_pop": "人口1000人あたり事業所数",
}

def render_header():
    """ タイトルと説明文を表示 """
    st.title("🏢 自然言語で八王子市の事業者データを分析")
    with st.expander("📘 使い方とデータについて"):
        st.markdown(f"""
        このアプリでは、八王子市の事業者に関する統計データについて、自然言語で質問することができます。
        AIがあなたの質問を解釈してSQLクエリを生成し、データベースから結果を取得・表示します。

        **利用可能なデータ**
        - **事業者統計データ (`business_stats`)**: 年度, 町名, 事業種別, 事業所数, 従業者数
        - **人口統計データ (`population`)**: 年度, 町名, 世帯数, 人口数, 男性数, 女性数
        - **犯罪統計データ (`crime_stats`)**: 年度, 町名, 犯罪大分類, 犯罪小分類, 犯罪件数

        **質問の例**
        - `2021年の町名別で、建設業の事業所数が多いトップ5を教えて`
        - `情報通信業の事業所数が最も多い年度は？`
        - `八王子市全体の従業員数の推移を年度別に教えて`
        """ ) # Corrected: Removed unnecessary f-string prefix and escaped quotes within markdown

def render_sample_questions():
    """ サンプル質問をボタンのグリッドとして表示 """
    st.subheader("💡 サンプル質問をワンクリック")

    sample_questions = {
        "🏗️ 建設業トップ10": "町名毎の建設業の事業所数トップ10",
        "📈 従業員数推移": "旭町の年度別の全従業員数の推移",
        "🏘️ 事業所密度分析": "町名毎の事業所密度を教えて",
        "🔍 世帯数あたり犯罪発生率": "町名ごとの世帯数あたり犯罪発生率を教えて",
        "🍽️ 飲食業と犯罪件数の関係": "飲食業の事業所数が多い町ほど犯罪件数は多いですか？",
        "📊 犯罪件数の推移": "2015年から2024年の間で犯罪件数が増えた町を教えて",
        "🏢 犯罪と産業構造": "犯罪件数が多い地域ではどの産業の事業所が多いですか？",
        "🏙️ 犯罪と世帯数の関係": "世帯数が多い地域ほど犯罪件数が多い傾向はありますか？",
        "🔎 治安改善エリア": "2020年から2024年の間で犯罪件数が減少している地域とその比率を教えて",
        "💻 IT企業の増加": "2015年から2024年で、情報通信業の事業所数が増加した町は？",
        "💪 働き手の多い町": "人口あたりの従業者数が多い町はどこ？",
        "🛍️ 小売と万引き": "卸売業・小売業の事業所数と万引きの件数に関係はありますか？",
    }

    cols = st.columns(4)
    for i, (key, value) in enumerate(sample_questions.items()):
        with cols[i % 4]:
            if st.button(key, key=f"sample_q_{i}", help=value):
                st.session_state.user_question = value
                st.rerun()


def render_main_form():
    """ メインの質問入力フォームを表示 """
    st.text_input("🔍 分析したい内容を質問してください:", key="user_question")
    st.button("🚀 分析を実行", type="primary", key="run_analysis_button")

def render_results(result_df, generated_sql, user_question, model_name):
    """ SQLとクエリ結果のデータフレームを表示 """
    if generated_sql:
        with st.expander("📝 生成されたSQLクエリ", expanded=False):
            st.code(generated_sql, language="sql")

    if result_df is not None and not result_df.empty:
        st.success(f"✅ クエリ結果 ({len(result_df)}行)")
        
        # 表示用にカラム名を日本語に置換
        display_df = result_df.copy()
        rename_map = {
            'year': '年度',
            'town_name': '町名',
            'industry_name': '事業種別',
            'major_crime': '犯罪大分類',
            'minor_crime': '犯罪小分類',
            'num_offices': '事業所数',
            'num_employees': '従業者数',
            'num_households': '世帯数',
            'num_population': '人口',
            'crime_count': '犯罪件数'
        }
        
        new_columns = {}
        for col in display_df.columns:
            new_col = col
            for en, jp in rename_map.items():
                new_col = new_col.replace(en, jp)
            new_columns[col] = new_col
        
        display_df = display_df.rename(columns=new_columns)
        st.dataframe(display_df, use_container_width=True)
    elif result_df is not None:
        st.warning("⚠️ 結果が0件でした。質問を変えてみてください。")

    if result_df is not None and not result_df.empty:
        with st.spinner(f"🤖 AI ({model_name}) が結果を分析中..."):
            ai_comment = generate_ai_summary(result_df, user_question, model_name)
        if ai_comment:
            with st.expander("🤖 AIによる分析コメント", expanded=True):
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

def render_folium_map(df: pd.DataFrame, metric_to_map: str):
    """ Folium地図を生成・表示する共通関数 """
    with st.spinner("🗺️ 地図データを生成中..."):
        gdf = load_geojson_data()
        if gdf is None:
            st.error("❌ 地図データの読み込みに失敗しました。")
            return

        map_df = gdf.merge(df, on='town_name', how='inner')
        if map_df.empty:
            st.warning("⚠️ 地図データと結合できる町名が見つかりませんでした。")
            return

        m = folium.Map(
            location=[35.655, 139.33], 
            zoom_start=11,
            tiles='https://cyberjapandata.gsi.go.jp/xyz/std/{z}/{x}/{y}.png',
            attr='国土地理院'
        )
        
        values = map_df[metric_to_map].values
        vmin, vmax = values.min(), values.max()
        colormap = cm.LinearColormap(
            colors=['#d73027', '#fee08b', '#1a9850'],
            index=[vmin, (vmin + vmax) / 2, vmax],
            vmin=vmin,
            vmax=vmax,
            caption=f'{METRIC_NAME_MAPPING.get(metric_to_map, metric_to_map)} の値'
        )
        
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
                aliases=['町名:', f'{METRIC_NAME_MAPPING.get(metric_to_map, metric_to_map)}:'],
                style=('background-color: white; color: black; '
                    'font-family: courier new; font-size: 12px; padding: 10px;')
            )
        ).add_to(m)
        
        colormap.add_to(m)

        css_style = """
        <style>
        path.leaflet-interactive:focus {
            outline: none !important;
        }
        </style>
        """
        m.get_root().html.add_child(Element(css_style))                    
        
        st_folium(m, use_container_width=True, returned_objects=[], key="hachioji_map_stats")

def render_visualizations(result_df):
    """ グラフと地図を表示 """
    if result_df is None or result_df.empty:
        return
    
    st.markdown("--- ")
    st.subheader("📈 データ可視化")

    try:
        numeric_cols = [col for col in result_df.select_dtypes(include=['number']).columns if col != 'year']
        category_cols = result_df.select_dtypes(include=['object']).columns.tolist()
        if category_cols and numeric_cols:
            chart_df = result_df.set_index(category_cols[0])[numeric_cols[0]]
            st.bar_chart(chart_df)
        else:
            st.write("グラフ化に適したデータ（カテゴリと数値の組み合わせ）がありませんでした。")
    except Exception as e:
        st.warning(f"グラフ描画スキップ: {e}")

    numeric_cols = result_df.select_dtypes(include=['number']).columns.tolist()
    if 'town_name' in result_df.columns and len(numeric_cols) > 0:
        st.subheader("🗺️ 地図で結果を確認")
        metric_to_map = st.selectbox(
            "地図に表示する指標を選択してください:", 
            options=numeric_cols, 
            index=0,
            format_func=lambda x: METRIC_NAME_MAPPING.get(x, x),
            key="lang_query_map_metric"
        )
        render_folium_map(result_df, metric_to_map)

def render_basic_statistics_view():
    """ 基本統計データを表示する """
    st.subheader("八王子市 基本統計データ（年度別）")
    st.markdown("八王子市全体の年度別主要統計データの推移です。")

    st.markdown("--- ")
    st.subheader("🏢 事業所数・従業員数の推移")
    business_df = get_yearly_business_summary()
    if business_df is not None and not business_df.empty:
        business_df_chart = business_df.set_index('year')
        st.line_chart(business_df_chart)
        with st.expander("詳細データ表示"):
            st.dataframe(business_df.style.format({
                "total_offices": "{:,} 所",
                "total_employees": "{:,} 人"
            }), use_container_width=True, hide_index=True)
    else:
        st.warning("事業所データがありませんでした。")

    st.markdown("--- ")
    st.subheader("👨‍👩‍👧‍👦 世帯数・人口の推移")
    population_df = get_yearly_population_summary()
    if population_df is not None and not population_df.empty:
        population_df_chart = population_df.set_index('year')
        st.line_chart(population_df_chart)
        with st.expander("詳細データ表示"):
            st.dataframe(population_df.style.format({
                "total_households": "{:,} 世帯",
                "total_population": "{:,} 人"
            }), use_container_width=True, hide_index=True)
    else:
        st.warning("人口データがありませんでした。")

    st.markdown("--- ")
    st.subheader("🚓 犯罪件数の推移")
    crime_df = get_yearly_crime_summary()
    if crime_df is not None and not crime_df.empty:
        crime_df_chart = crime_df.set_index('year')
        st.line_chart(crime_df_chart)
        with st.expander("詳細データ表示"):
            st.dataframe(crime_df.style.format({
                "total_crimes": "{:,} 件"
            }), use_container_width=True, hide_index=True)
    else:
        st.warning("犯罪データがありませんでした。")

    # --- 地図表示機能 ---
    st.markdown("--- ")
    st.subheader("🗺️ 町名別データの地図表示")

    available_years = get_available_years()
    if not available_years:
        st.warning("地図表示に利用できるデータがありませんでした。")
        return

    col1, col2 = st.columns(2)
    with col1:
        selected_year = st.selectbox("表示する年度を選択", options=available_years, key="map_year")
    with col2:
        data_type = st.selectbox(
            "表示するデータの種類を選択", 
            options=["事業所データ", "人口データ", "犯罪データ"], 
            key="map_data_type"
        )

    df = None
    if data_type == "事業所データ":
        df = get_town_business_data(selected_year)
    elif data_type == "人口データ":
        df = get_town_population_data(selected_year)
    elif data_type == "犯罪データ":
        df = get_town_crime_data(selected_year)

    if df is not None and not df.empty:
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        if numeric_cols:
            metric_to_map = st.selectbox(
                "地図に表示する指標を選択", 
                options=numeric_cols,
                format_func=lambda x: METRIC_NAME_MAPPING.get(x, x),
                key="stats_map_metric"
            )
            render_folium_map(df, metric_to_map)
            with st.expander("地図表示データの詳細"):
                st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("このデータには地図に表示できる数値指標がありません。")
    else:
        st.info(f"{selected_year}年の{data_type}がありませんでした。")
    
def render_about_page():
    """「このサービスについて」ページを表示する"""

    # SVGファイルを読み込み
    with open("images/abt_logo.svg", "r") as f:
        svg_content = f.read()

    st.subheader("🏢 運営会社")
    st.markdown(
        f'''<a href="https://abt.jp" target="_blank" alt="株式会社アプト"><div style="width: 200px; height: 100px;margin: 1em;">{svg_content}</div></a>''',
        unsafe_allow_html=True
    )
    st.markdown("""
        **株式会社アプト**
        - Webサイト: [https://abt.jp](https://abt.jp)
        - 〒192-0075 東京都八王子市南新町4-14 TMライトハウス 104
        - 連絡先: [お問い合わせ|株式会社アプト](https://abt.jp/read.php?id=3)
    """ )

    st.markdown("---")

    st.subheader("📊 利用データ")
    st.markdown("""
        本サービスでは、以下のオープンデータを活用しています。

        **1. 統計八王子（各年版）**
        - **参照内容**: 事業所数、世帯数 (2015～2024年)
        - **ソース**: [統計八王子（各年版）｜八王子市公式ホームページ](https://www.city.hachioji.tokyo.jp/shisei/002/006/tokehachihkakunen/)

        **2. 区市町村の町丁別、罪種別及び手口別認知件数**
        - **参照内容**: 区市町村の町丁別、罪種別及び手口別認知件数 (2015～2024年)
        - **ソース**: [警視庁](https://www.keishicho.metro.tokyo.lg.jp/about_mpd/jokyo_tokei/jokyo/ninchikensu.html)

        **3. 統計地理情報システム（e-Stat）**
        - **参照内容**: 八王子市の市区町村境界データ (2020年)
        - **ソース**: [政府統計の総合窓口](https://www.e-stat.go.jp/gis/statmap-search?page=1&type=2&aggregateUnitForBoundary=A&toukeiCode=00200521&toukeiYear=2020&serveyId=A002005212020&datum=2000)
    """)