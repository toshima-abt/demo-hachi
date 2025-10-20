import streamlit as st
import google.generativeai as genai
import logging
from utils import (
    generate_sql,
    execute_query,
    detect_metric_question,
    extract_query_parameters,
    get_all_data,
    calculate_derived_metrics
)
from view import (
    render_header,
    render_sample_questions,
    render_main_form,
    render_results,
    render_metrics_and_insights,
    render_visualizations
)

# ロギング設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- ページ設定とAPIキー設定 ---
st.set_page_config(page_title="八王子市 事業者数分析", layout="wide")
try:
    genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])
except Exception as e:
    st.error("⚠️ APIキーが設定されていません。StreamlitのSecretsに 'GOOGLE_API_KEY' を設定してください。")
    logger.error(f"API設定エラー: {e}")
    st.stop()

# --- セッション状態の初期化 ---
def initialize_session_state():
    if "user_question" not in st.session_state:
        st.session_state.user_question = "町名毎に2021年の全事業所密度（事業所数÷世帯数）を比較して"
    if "generated_sql" not in st.session_state:
        st.session_state.generated_sql = None
    if "result_df" not in st.session_state:
        st.session_state.result_df = None
    if "metrics_df" not in st.session_state:
        st.session_state.metrics_df = None
    if "is_metric_question" not in st.session_state:
        st.session_state.is_metric_question = False
    if "query_params" not in st.session_state:
        st.session_state.query_params = {}

# --- メインの実行ロジック ---
def main():
    initialize_session_state()
    render_header()
    st.markdown("---")
    render_sample_questions()
    render_main_form()

    # 分析実行ボタンが押された、またはサンプル質問が選択された場合
    if st.session_state.get("run_analysis_button"):
        user_question = st.session_state.user_question
        if not user_question:
            st.warning("⚠️ 質問を入力してください。")
            st.stop()

        with st.spinner("🤖 AIがSQLを生成中..."):
            generated_sql = generate_sql(user_question)
        st.session_state.generated_sql = generated_sql

        if generated_sql:
            with st.spinner("💾 データベースでクエリを実行中..."):
                result_df = execute_query(generated_sql)
            st.session_state.result_df = result_df
            st.session_state.is_metric_question = detect_metric_question(user_question)

            # 派生指標の計算
            if st.session_state.is_metric_question and result_df is not None:
                with st.spinner("📊 派生指標を計算中..."):
                    query_params = extract_query_parameters(generated_sql, user_question)
                    population_df = get_all_data('population')
                    business_df = get_all_data('business_stats')
                    if population_df is not None and business_df is not None:
                        metrics_df = calculate_derived_metrics(
                            business_df, population_df,
                            year=query_params.get('year'),
                            industry=query_params.get('industry'),
                            town=query_params.get('town')
                        )
                        st.session_state.metrics_df = metrics_df
                        st.session_state.query_params = query_params
                    else:
                        st.session_state.metrics_df = None
            else:
                st.session_state.metrics_df = None
                st.session_state.query_params = {}

    # --- 結果の表示 ---
    render_results(st.session_state.result_df, st.session_state.generated_sql, st.session_state.user_question)
    
    if st.session_state.is_metric_question:
        render_metrics_and_insights(
            st.session_state.metrics_df, 
            st.session_state.user_question, 
            st.session_state.query_params
        )

    render_visualizations(st.session_state.result_df)

    st.markdown("---")
    st.caption("💡 Powered by Google Gemini & DuckDB | 八王子市オープンデータを活用")

if __name__ == "__main__":
    main()