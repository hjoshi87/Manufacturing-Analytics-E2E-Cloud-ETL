

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
from datetime import datetime
import numpy as np
from pathlib import Path
import urllib.parse
import os
from dotenv import load_dotenv

current_dir = Path(__file__).parent
env_path = current_dir.parent / 'config' / '.env'
load_dotenv(dotenv_path=env_path)

# ============================================================================
# PAGE CONFIG - DARK BOTTLE GREEN THEME (#093624)
# ============================================================================
st.set_page_config(
    page_title="Manufacturing Analytics",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded"
)
 
# Custom theme styling
st.markdown("""
    <style>
    :root {
        --primary-color: #093624;  /* Dark bottle green */
        --accent-color: #00D4AA;   /* Teal accent */
        --background-color: #F5F5F5;
        --text-color: #1A1A1A;
    }
    
    .main {
        background-color: #FFFFFF;
    }
    
    .stMetric {
        background-color: #F8F9FA;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #093624;
    }
    
    h1 {
        color: #093624;
        font-weight: 700;
    }
    
    h2, h3 {
        color: #093624;
        font-weight: 600;
    }
    
    .insight-box {
        background: linear-gradient(135deg, #093624 0%, #0D4A2E 100%);
        color: white;
        padding: 20px;
        border-radius: 10px;
        margin: 15px 0;
    }
    
    .critical-alert {
        background-color: #FFE5E5;
        border-left: 5px solid #FF4444;
        padding: 15px;
        border-radius: 5px;
        margin: 10px 0;
    }
    
    .high-alert {
        background-color: #FFF4E5;
        border-left: 5px solid #FF9800;
        padding: 15px;
        border-radius: 5px;
        margin: 10px 0;
    }
    
    .medium-alert {
        background-color: #FFFDE5;
        border-left: 5px solid #FFC107;
        padding: 15px;
        border-radius: 5px;
        margin: 10px 0;
    }
    
    .low-alert {
        background-color: #E8F5E9;
        border-left: 5px solid #4CAF50;
        padding: 15px;
        border-radius: 5px;
        margin: 10px 0;
    }
    </style>
""", unsafe_allow_html=True)

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

@st.cache_resource
def get_db_engine():
    """SQLAlchemy engine for Redshift Serverless"""
    try:
        # Fetch variables from .env
        user = os.getenv('REDSHIFT_USER')
        password = os.getenv('REDSHIFT_PASSWORD')
        host = os.getenv('REDSHIFT_HOST')
        if host is None:
            print("❌ ERROR: .env file not found or REDSHIFT_HOST is missing!")
        port = os.getenv('REDSHIFT_PORT', '5439')
        dbname = os.getenv('REDSHIFT_DB')

        connection_str = f"redshift+redshift_connector://{user}:{password}@{host}:{port}/{dbname}"

        engine = create_engine(
            connection_str,
            echo=False,
            pool_size=5,
            max_overflow=10,
            pool_recycle=3600
        )

        # Test connection
        with engine.connect() as connection:
            connection.execute("SELECT 1")
        return engine
    
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        st.error("Check:")
        st.error("1. Endpoint is correct")
        st.error("2. Password is correct")
        st.error("3. Your IP is in security group (port 5439)")
        return None

@st.cache_data(ttl=600)
def load_summary_data():
    """Load fct_machine_summary data"""
    engine = get_db_engine()
    if engine is None:
        return None
    
    try:
        query = """
        SELECT 
            machine_id,
            operation,
            days_with_data,
            total_measurements,
            total_anomalies,
            overall_anomaly_rate,
            overall_avg_vibration_x,
            overall_avg_vibration_y,
            overall_avg_vibration_z,
            peak_vibration_x,
            peak_vibration_y,
            peak_vibration_z,
            avg_vibration_magnitude,
            avg_energy_price_overall,
            total_energy_cost,
            avg_power_consumption_overall,
            avg_industrial_index_overall,
            risk_level
        FROM public.fct_machine_summary
        ORDER BY machine_id, operation
        """
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Error loading summary data: {e}")
        return None

@st.cache_data(ttl=600)
def load_daily_data(machine_id=None, operation=None):
    """Load fct_machine_daily data with optional filters"""
    engine = get_db_engine()
    if engine is None:
        return None
    
    try:
        query = """
        SELECT 
            measurement_date,
            machine_id,
            operation,
            total_measurements,
            anomaly_count,
            anomaly_rate_pct,
            avg_vibration_x,
            avg_vibration_y,
            avg_vibration_z,
            max_vibration_x,
            max_vibration_y,
            max_vibration_z,
            vibration_magnitude,
            avg_energy_price,
            daily_energy_cost,
            avg_power_consumption,
            avg_industrial_index,
            machine_health_status,
            vibration_stability,
            cost_per_kwh
        FROM public.fct_machine_daily
        WHERE 1=1
        """
        
        if machine_id:
            query += f" AND machine_id = '{machine_id}'"
        if operation:
            query += f" AND operation = '{operation}'"
        
        query += " ORDER BY measurement_date DESC"
        
        df = pd.read_sql(query, engine)
        
        if df is None or len(df) == 0:
            return None
        
        return df
        
    except Exception as e:
        st.error(f"Query error: {e}")
        return None

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_risk_color(risk_level):
    """Return color based on risk level"""
    colors = {
        'CRITICAL': '#FF4444',
        'HIGH_RISK': '#FF9800',
        'MEDIUM_RISK': '#FFC107',
        'LOW_RISK': '#4CAF50'
    }
    return colors.get(risk_level, '#999999')
 
def get_recommendation(risk_level, anomaly_rate, vibration_mag, energy_cost_change):
    """Generate actionable recommendation"""
    if risk_level == 'CRITICAL':
        return "🚨 CRITICAL: Immediate maintenance required. Vibration levels indicate advanced degradation. Check bearings and alignment immediately."
    elif risk_level == 'HIGH_RISK':
        return "⚠️ HIGH RISK: Schedule maintenance within 1-2 weeks. Monitor vibration trends closely."
    elif risk_level == 'MEDIUM_RISK':
        return "⏱️ MEDIUM RISK: Increase monitoring frequency. Plan maintenance for next scheduled downtime."
    else:
        return "✔️ LOW RISK: Continue routine monitoring. Normal operating condition."
 
def generate_insight_box(summary_data, daily_data):
    """Generate executive summary insights"""
    insights = []
    
    if summary_data is not None:
        critical_count = len(summary_data[summary_data['risk_level'] == 'CRITICAL'])
        high_risk_count = len(summary_data[summary_data['risk_level'] == 'HIGH_RISK'])
        avg_anomaly = summary_data['overall_anomaly_rate'].mean()
        avg_vibration = summary_data['avg_vibration_magnitude'].mean()
        total_cost = summary_data['total_energy_cost'].sum()
        
        insights.append(f"📊 **Overview**: {critical_count} critical, {high_risk_count} high-risk machines detected")
        insights.append(f"📈 **Average Anomaly Rate**: {avg_anomaly:.2f}%")
        insights.append(f"📉 **Average Vibration**: {avg_vibration:.2f} RMS")
        insights.append(f"💰 **Total Energy Cost**: €{total_cost:,.0f}")
    
    return " | ".join(insights)
 
# ============================================================================
# PAGE LAYOUT
# ============================================================================
 
st.markdown("""
    <div class="insight-box">
        <h2>🏭 Manufacturing Equipment Health & Cost Analysis</h2>
        <p><strong>Project Aim:</strong> Detect equipment degradation through vibration analysis and correlate with electricity prices</p>
    </div>
""", unsafe_allow_html=True)
 
# ============================================================================
# LOAD DATA
# ============================================================================
 
summary_df = load_summary_data()
 
if summary_df is not None:
    
    # ====================================================================
    # EXECUTIVE INSIGHTS (TOP)
    # ====================================================================
    
    st.markdown("### 📊 Executive Summary")
    
    col1, col2, col3, col4 = st.columns(4)
    
    critical_count = len(summary_df[summary_df['risk_level'] == 'CRITICAL'])
    high_risk_count = len(summary_df[summary_df['risk_level'] == 'HIGH_RISK'])
    total_cost = summary_df['total_energy_cost'].sum()
    avg_anomaly = summary_df['overall_anomaly_rate'].mean()
    
    with col1:
        st.metric(
            "🚨 Critical Machines",
            critical_count,
            delta="Needs immediate attention",
            delta_color="inverse"
        )
    
    with col2:
        st.metric(
            "⚠️ High Risk",
            high_risk_count,
            delta="Schedule maintenance soon",
            delta_color="inverse"
        )
    
    with col3:
        st.metric(
            "💰 Total Energy Cost",
            f"€{total_cost/1000:.1f}K",
            delta="Monitor degradation impact"
        )
    
    with col4:
        st.metric(
            "📈 Avg Anomaly Rate",
            f"{avg_anomaly:.1f}%",
            delta="Equipment health indicator"
        )
    
    st.divider()
    
    # ====================================================================
    # FILTERS
    # ====================================================================
    
    st.sidebar.header("🔍 Filters")
    machines = sorted(summary_df['machine_id'].unique())
    selected_machine = st.sidebar.selectbox("Select Machine", machines)
    
    operations = sorted(summary_df[summary_df['machine_id'] == selected_machine]['operation'].unique())
    selected_operation = st.sidebar.selectbox("Select Operation", operations)
    
    # Get data for selected machine/operation
    daily_df = load_daily_data(selected_machine, selected_operation)
    
    if daily_df is not None and len(daily_df) > 0:
        
        machine_summary = summary_df[
            (summary_df['machine_id'] == selected_machine) & 
            (summary_df['operation'] == selected_operation)
        ].iloc[0]
        
        # ====================================================================
        # MACHINE-SPECIFIC INSIGHTS & RECOMMENDATIONS
        # ====================================================================
        
        st.markdown(f"### {selected_machine} - Operation {selected_operation}")
        
        # Risk alert box
        risk_level = machine_summary['risk_level']
        recommendation = get_recommendation(
            risk_level,
            machine_summary['overall_anomaly_rate'],
            machine_summary['avg_vibration_magnitude'],
            0
        )
        
        risk_class = f"{risk_level.lower()}-alert"
        st.markdown(f'<div class="{risk_class}">{recommendation}</div>', unsafe_allow_html=True)
        
        st.divider()
        
        # ====================================================================
        # KEY METRICS (CRISP KPI CARDS)
        # ====================================================================
        
        st.markdown("### 📊 Machine Metrics")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Anomaly Rate",
                f"{machine_summary['overall_anomaly_rate']:.1f}%",
                "Degradation signal"
            )
        
        with col2:
            st.metric(
                "Vibration Magnitude",
                f"{machine_summary['avg_vibration_magnitude']:.0f}",
                "RMS combined"
            )
        
        with col3:
            st.metric(
                "Energy Cost",
                f"€{machine_summary['total_energy_cost']:,.0f}",
                "Total period"
            )
        
        with col4:
            st.metric(
                "Cost per kWh",
                f"€{machine_summary['total_energy_cost'] / (machine_summary['avg_power_consumption_overall'] * machine_summary['days_with_data']):.2f}",
                "Efficiency metric"
            )
        
        st.divider()
        
        # ====================================================================
        # VISUALIZATIONS
        # ====================================================================
        
        st.markdown("### 📈 Detailed Analysis")
        
        # Chart 1: Anomaly Rate Over Time
        col1, col2 = st.columns(2)
        
        with col1:
            daily_df_sorted = daily_df.sort_values('measurement_date')
            fig_anomaly = px.line(
                daily_df_sorted,
                x='measurement_date',
                y='anomaly_rate_pct',
                title='Anomaly Rate Trend',
                markers=True,
                labels={'measurement_date': 'Date', 'anomaly_rate_pct': 'Anomaly Rate (%)'}
            )
            fig_anomaly.add_hline(
                y=machine_summary['overall_anomaly_rate'],
                line_dash="dash",
                line_color="#093624",
                annotation_text="Average"
            )
            fig_anomaly.update_layout(
                hovermode='x unified',
                template='plotly_white',
                margin=dict(l=0, r=0, t=30, b=0)
            )
            st.plotly_chart(fig_anomaly, use_container_width=True)
            st.caption("📍 **Trend**: Rising anomaly rate indicates equipment degradation.")
        
        # Chart 2: Vibration Magnitude Over Time
        with col2:
            fig_vibration = px.line(
                daily_df_sorted,
                x='measurement_date',
                y='vibration_magnitude',
                title='Vibration Magnitude Trend',
                markers=True,
                labels={'measurement_date': 'Date', 'vibration_magnitude': 'Magnitude (RMS)'}
            )
            fig_vibration.add_hline(
                y=machine_summary['avg_vibration_magnitude'],
                line_dash="dash",
                line_color="#093624",
                annotation_text="Average"
            )
            fig_vibration.update_layout(
                hovermode='x unified',
                template='plotly_white',
                margin=dict(l=0, r=0, t=30, b=0)
            )
            st.plotly_chart(fig_vibration, use_container_width=True)
            st.caption("📍 **Trend**: Increasing vibration suggests mechanical wear or misalignment.")
        
        # Chart 3: Daily Energy Cost Over Time
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            fig_cost = px.bar(
                daily_df_sorted,
                x='measurement_date',
                y='daily_energy_cost',
                title='Daily Energy Cost',
                labels={'measurement_date': 'Date', 'daily_energy_cost': 'Cost (€)'}
            )
            fig_cost.update_layout(
                hovermode='x unified',
                template='plotly_white',
                margin=dict(l=0, r=0, t=30, b=0)
            )
            st.plotly_chart(fig_cost, use_container_width=True)
            st.caption("💰 **Impact**: Higher costs correlate with increased power consumption from degradation.")
        
        # Chart 4: Machine Health Status Distribution
        with col2:
            health_counts = daily_df['machine_health_status'].value_counts()
            fig_health = px.pie(
                values=health_counts.values,
                names=health_counts.index,
                title='Machine Health Status Distribution',
                color_discrete_map={
                    'CRITICAL': '#FF4444',
                    'HIGH_RISK': '#FF9800',
                    'MEDIUM_RISK': '#FFC107',
                    'LOW_RISK': '#4CAF50'
                }
            )
            fig_health.update_layout(
                template='plotly_white',
                margin=dict(l=0, r=0, t=30, b=0)
            )
            st.plotly_chart(fig_health, use_container_width=True)
            st.caption("⚠️ **Alert**: Percentage of days at each risk level indicates maintenance urgency.")
        
        # KPI 5: Vibration Stability Status
        with col3:
            vibration_stability_dominant = daily_df['vibration_stability'].mode()[0] if len(daily_df) > 0 else 'N/A'
            vibration_stability_pct = (daily_df['vibration_stability'].value_counts().get(vibration_stability_dominant, 0) / len(daily_df) * 100) if len(daily_df) > 0 else 0
            
            st.metric(
                "Vibration Stability",
                vibration_stability_dominant,
                f"{vibration_stability_pct:.0f}% of days"
            )
        
        # KPI 6: Average Industrial Index
        with col4:
            st.metric(
                "Industrial Index",
                f"{machine_summary['avg_industrial_index_overall']:.2f}",
                "External factor"
            )
        
        st.divider()
        
        # ====================================================================
        # CORRELATION ANALYSIS
        # ====================================================================
        
        st.markdown("### 🔗 Correlations & Insights")
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_energy_vibration = px.scatter(
                daily_df_sorted,
                x='vibration_magnitude',
                y='daily_energy_cost',
                color='anomaly_rate_pct',
                size='avg_power_consumption',
                hover_data=['measurement_date', 'machine_health_status'],
                title='Energy Cost vs Vibration',
                labels={
                    'vibration_magnitude': 'Vibration Magnitude (RMS)',
                    'daily_energy_cost': 'Energy Cost (€)',
                    'anomaly_rate_pct': 'Anomaly Rate (%)'
                },
                color_continuous_scale='RdYlGn_r'
            )
            fig_energy_vibration.update_layout(
                template='plotly_white',
                margin=dict(l=0, r=0, t=30, b=0)
            )
            st.plotly_chart(fig_energy_vibration, use_container_width=True)
            st.caption("📊 **Correlation**: High vibration machines consume more energy, increasing operational costs.")
        
        with col2:
            fig_anomaly_cost = px.scatter(
                daily_df_sorted,
                x='anomaly_rate_pct',
                y='daily_energy_cost',
                color='vibration_stability',
                size='vibration_magnitude',
                title='Anomaly Rate vs Energy Cost',
                labels={
                    'anomaly_rate_pct': 'Anomaly Rate (%)',
                    'daily_energy_cost': 'Energy Cost (€)',
                    'vibration_stability': 'Vibration Status'
                }
            )
            fig_anomaly_cost.update_layout(
                template='plotly_white',
                margin=dict(l=0, r=0, t=30, b=0)
            )
            st.plotly_chart(fig_anomaly_cost, use_container_width=True)
            st.caption("💡 **Insight**: Equipment degradation (anomalies) directly impacts electricity consumption and cost.")
        
        st.divider()
        
        # ====================================================================
        # DETAILED DATA TABLE
        # ====================================================================
        
        st.markdown("### 📋 Daily Data Records")
        
        display_cols = [
            'measurement_date', 'anomaly_rate_pct', 'vibration_magnitude',
            'daily_energy_cost', 'avg_power_consumption', 'machine_health_status'
        ]
        
        display_df = daily_df[display_cols].copy()
        display_df['measurement_date'] = pd.to_datetime(display_df['measurement_date']).dt.strftime('%Y-%m-%d')
        display_df['anomaly_rate_pct'] = display_df['anomaly_rate_pct'].round(2)
        display_df['vibration_magnitude'] = display_df['vibration_magnitude'].round(2)
        display_df['daily_energy_cost'] = display_df['daily_energy_cost'].round(2)
        display_df['avg_power_consumption'] = display_df['avg_power_consumption'].round(2)
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        st.divider()
        
        # ====================================================================
        # ACTIONABLE SUMMARY
        # ====================================================================
        
        st.markdown("### 🎯 Key Findings & Recommendations")
        
        findings = f"""
        **Equipment Status:** {machine_summary['risk_level']}
        
        **Data Period:** {machine_summary['days_with_data']:.0f} days monitored
        
        **Anomalies Detected:** {machine_summary['total_anomalies']:.0f} out of {machine_summary['total_measurements']:.0f} measurements ({machine_summary['overall_anomaly_rate']:.1f}%)
        
        **Vibration Analysis:** 
        - X-axis average: {machine_summary['overall_avg_vibration_x']:.2f} RMS
        - Y-axis average: {machine_summary['overall_avg_vibration_y']:.2f} RMS
        - Z-axis average: {machine_summary['overall_avg_vibration_z']:.2f} RMS
        - Combined magnitude: {machine_summary['avg_vibration_magnitude']:.2f} RMS
        
        **Energy Impact:**
        - Total energy cost: €{machine_summary['total_energy_cost']:,.0f}
        - Average cost per kWh: €{(machine_summary['total_energy_cost'] / (machine_summary['avg_power_consumption_overall'] * machine_summary['days_with_data'])):.3f}
        - Average power consumption: {machine_summary['avg_power_consumption_overall']:.2f} kWh
        
        **Recommendation:** {recommendation}
        """
        
        st.info(findings)
    
    else:
        st.warning("No daily data available for this selection")
 
else:
    st.error("Failed to load data. Check database connection.")