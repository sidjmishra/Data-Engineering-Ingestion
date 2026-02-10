"""
File Ingestion Pipeline - Streamlit Dashboard
Monitoring, analytics, and log viewer for the ingestion system

Run with: streamlit run dashboard.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import glob
from pathlib import Path
import json
import sys

# Add src directory to path
sys.path.insert(0, os.path.dirname(__file__))

from src.database.factory import DatabaseFactory
from src.utils.logger import LoggerConfig
from src.utils.config import ConfigManager


# Page configuration
st.set_page_config(
    page_title="File Ingestion Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    .success-card {
        background-color: #d4edda;
        padding: 20px;
        border-radius: 10px;
    }
    .error-card {
        background-color: #f8d7da;
        padding: 20px;
        border-radius: 10px;
    }
    .warning-card {
        background-color: #fff3cd;
        padding: 20px;
        border-radius: 10px;
    }
    </style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_database_handler():
    """Get database handler with caching"""
    try:
        config = ConfigManager.load_config("config/config.yaml")
        db = DatabaseFactory.create_handler()
        if db.health_check():
            return db
        else:
            st.error("Database health check failed")
            return None
    except Exception as e:
        st.error(f"Failed to connect to database: {str(e)}")
        return None


def load_logs(days=1):
    """Load logs from the last N days"""
    logs = []
    log_dir = "logs"
    
    if not os.path.exists(log_dir):
        return pd.DataFrame()
    
    now = datetime.now()
    start_date = now - timedelta(days=days)
    
    # Get log files
    log_files = glob.glob(os.path.join(log_dir, "*.log"))
    log_files.sort(reverse=True)
    
    for log_file in log_files:
        try:
            # Parse filename for date
            filename = os.path.basename(log_file)
            if filename.startswith("ingestion_"):
                date_str = filename.replace("ingestion_", "").replace(".log", "")
                file_date = datetime.strptime(date_str, "%Y%m%d")
                
                if file_date >= start_date:
                    with open(log_file, 'r') as f:
                        for line in f:
                            logs.append({
                                'timestamp': line.split(' - ')[0] if ' - ' in line else '',
                                'level': 'INFO' if 'INFO' in line else 'ERROR' if 'ERROR' in line else 'WARNING' if 'WARNING' in line else 'DEBUG',
                                'message': line.strip()
                            })
        except Exception as e:
            st.warning(f"Error reading log file {log_file}: {str(e)}")
    
    return pd.DataFrame(logs)


def get_file_statistics():
    """Get statistics about processed files"""
    stats = {
        'total_validated': 0,
        'total_raw': 0,
        'total_failed': 0,
        'by_type': {'csv': 0, 'image': 0, 'video': 0}
    }
    
    config = ConfigManager.get_config()
    
    # Count files in directories
    for folder in ['validated', 'raw', 'failed']:
        folder_path = config['folders'][folder]
        if os.path.exists(folder_path):
            files = []
            for root, dirs, filenames in os.walk(folder_path):
                files.extend(filenames)
            
            if folder == 'validated':
                stats['total_validated'] = len(files)
            elif folder == 'raw':
                stats['total_raw'] = len(files)
            elif folder == 'failed':
                stats['total_failed'] = len(files)
            
            # Count by type
            for file in files:
                ext = Path(file).suffix.lower()
                if ext == '.csv':
                    stats['by_type']['csv'] += 1
                elif ext in ['.jpg', '.jpeg', '.png', '.bmp', '.gif', '.tiff']:
                    stats['by_type']['image'] += 1
                elif ext in ['.mp4', '.avi', '.mov', '.mkv', '.flv', '.wmv', '.webm']:
                    stats['by_type']['video'] += 1
    
    return stats


def get_database_statistics(db):
    """Get statistics from database"""
    stats = {
        'total_files': 0,
        'by_type': {'csv': 0, 'image': 0, 'video': 0},
        'by_status': {'validated': 0, 'processing': 0, 'failed': 0},
        'total_logs': 0,
        'recent_logs': []
    }
    
    try:
        # This would depend on database implementation
        # For now, we'll return empty stats
        return stats
    except Exception as e:
        st.error(f"Error getting database statistics: {str(e)}")
        return stats


def main():
    # Sidebar configuration
    st.sidebar.title("⚙️ Configuration")
    
    # Load configuration
    try:
        config = ConfigManager.load_config("config/config.yaml")
    except Exception as e:
        st.error(f"Failed to load configuration: {str(e)}")
        return
    
    # Database connection status
    db = get_database_handler()
    db_status = "✅ Connected" if db else "❌ Disconnected"
    st.sidebar.markdown(f"**Database Status:** {db_status}")
    
    # Navigation
    page = st.sidebar.radio(
        "📍 Navigation",
        [
            "📊 Dashboard",
            "📝 Logs",
            "📁 Files",
            "💾 Database",
            "📈 Analytics",
            "⚙️ Settings"
        ]
    )
    
    st.sidebar.divider()
    st.sidebar.markdown("**System Info**")
    st.sidebar.info(f"""
    - **Database Type:** {config.get('database', {}).get('type', 'Unknown')}
    - **Scheduler Interval:** {config.get('scheduler', {}).get('interval_minutes', 'N/A')} min
    - **Logging Level:** {config.get('logging', {}).get('level', 'INFO')}
    """)
    
    # Page routing
    if page == "📊 Dashboard":
        show_dashboard(config)
    elif page == "📝 Logs":
        show_logs()
    elif page == "📁 Files":
        show_files(config)
    elif page == "💾 Database":
        show_database(db, config)
    elif page == "📈 Analytics":
        show_analytics(db)
    elif page == "⚙️ Settings":
        show_settings(config)


def show_dashboard(config):
    """Main dashboard view"""
    st.title("📊 File Ingestion Pipeline Dashboard")
    
    # Get statistics
    file_stats = get_file_statistics()
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="✅ Validated Files",
            value=file_stats['total_validated'],
            delta=None
        )
    
    with col2:
        st.metric(
            label="💾 Raw Files",
            value=file_stats['total_raw'],
            delta=None
        )
    
    with col3:
        st.metric(
            label="❌ Failed Files",
            value=file_stats['total_failed'],
            delta=None
        )
    
    with col4:
        total = file_stats['total_validated'] + file_stats['total_raw'] + file_stats['total_failed']
        st.metric(
            label="📦 Total Files",
            value=total,
            delta=None
        )
    
    st.divider()
    
    # File type distribution
    st.subheader("📊 Files by Type")
    col1, col2 = st.columns(2)
    
    with col1:
        # Pie chart
        fig_pie = go.Figure(data=[go.Pie(
            labels=['CSV', 'Images', 'Videos'],
            values=[
                file_stats['by_type']['csv'],
                file_stats['by_type']['image'],
                file_stats['by_type']['video']
            ],
            marker=dict(colors=['#1f77b4', '#ff7f0e', '#2ca02c'])
        )])
        fig_pie.update_layout(height=400)
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        # Bar chart
        fig_bar = go.Figure(data=[go.Bar(
            x=['CSV', 'Images', 'Videos'],
            y=[
                file_stats['by_type']['csv'],
                file_stats['by_type']['image'],
                file_stats['by_type']['video']
            ],
            marker=dict(color=['#1f77b4', '#ff7f0e', '#2ca02c'])
        )])
        fig_bar.update_layout(
            height=400,
            yaxis_title="Count",
            xaxis_title="File Type"
        )
        st.plotly_chart(fig_bar, use_container_width=True)
    
    st.divider()
    
    # Recent logs
    st.subheader("📋 Recent Activity")
    logs = load_logs(days=1)
    
    if len(logs) > 0:
        # Display last 20 logs
        recent_logs = logs.tail(20).copy()
        recent_logs = recent_logs.iloc[::-1]  # Reverse to show newest first
        
        for idx, row in recent_logs.iterrows():
            if 'SUCCESS' in row['message'] or '✓' in row['message']:
                st.success(row['message'])
            elif 'ERROR' in row['message'] or '✗' in row['message']:
                st.error(row['message'])
            elif 'WARNING' in row['message'] or '⊗' in row['message']:
                st.warning(row['message'])
            else:
                st.info(row['message'])
    else:
        st.info("No logs found")


def show_logs():
    """Log viewer"""
    st.title("📝 Application Logs")
    
    # Log filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        days = st.slider("Days to show:", 1, 30, 1)
    
    with col2:
        log_level = st.multiselect(
            "Log Level:",
            ["INFO", "DEBUG", "WARNING", "ERROR"],
            default=["INFO", "WARNING", "ERROR"]
        )
    
    with col3:
        search_term = st.text_input("Search logs:", "")
    
    # Load and filter logs
    logs = load_logs(days=days)
    
    if len(logs) > 0:
        # Filter by level
        if log_level:
            logs = logs[logs['level'].isin(log_level)]
        
        # Filter by search term
        if search_term:
            logs = logs[logs['message'].str.contains(search_term, case=False, na=False)]
        
        st.write(f"**Found {len(logs)} log entries**")
        
        # Display logs as table
        st.dataframe(logs, use_container_width=True, height=500)
        
        # Download logs
        csv = logs.to_csv(index=False)
        st.download_button(
            label="Download as CSV",
            data=csv,
            file_name=f"logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    else:
        st.info("No logs found for selected filters")


def show_files(config):
    """File explorer"""
    st.title("📁 File Explorer")
    
    col1, col2 = st.columns(2)
    
    with col1:
        folder_type = st.selectbox(
            "Select Folder:",
            ["Validated", "Raw", "Failed", "Incoming"]
        )
    
    folder_map = {
        "Validated": "validated",
        "Raw": "raw",
        "Failed": "failed",
        "Incoming": "incoming"
    }
    
    folder_path = config['folders'][folder_map[folder_type]]
    
    st.write(f"**Path:** `{folder_path}`")
    
    # List files
    if os.path.exists(folder_path):
        files = []
        for root, dirs, filenames in os.walk(folder_path):
            for filename in filenames:
                filepath = os.path.join(root, filename)
                size = os.path.getsize(filepath)
                modified = os.path.getmtime(filepath)
                modified_date = datetime.fromtimestamp(modified)
                
                files.append({
                    'Filename': filename,
                    'Path': filepath,
                    'Size (KB)': round(size / 1024, 2),
                    'Modified': modified_date.strftime('%Y-%m-%d %H:%M:%S'),
                    'Type': Path(filename).suffix.lower()
                })
        
        if files:
            df_files = pd.DataFrame(files)
            
            # Statistics
            st.subheader("📊 Statistics")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total Files", len(df_files))
            with col2:
                total_size = df_files['Size (KB)'].sum()
                st.metric("Total Size (MB)", round(total_size / 1024, 2))
            with col3:
                st.metric("Avg Size (KB)", round(df_files['Size (KB)'].mean(), 2))
            
            st.divider()
            
            # File list
            st.subheader("📋 Files")
            st.dataframe(df_files, use_container_width=True, height=500)
            
            # File type breakdown
            st.subheader("📊 File Types")
            type_counts = df_files['Type'].value_counts()
            fig = px.bar(
                x=type_counts.index,
                y=type_counts.values,
                labels={'x': 'File Type', 'y': 'Count'},
                title="Files by Type"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info(f"No files in {folder_type} folder")
    else:
        st.warning(f"Folder not found: {folder_path}")


def show_database(db, config):
    """Database viewer"""
    st.title("💾 Database")
    
    if not db:
        st.error("Database not connected")
        return
    
    db_type = config.get('database', {}).get('type', 'unknown')
    st.write(f"**Database Type:** {db_type}")
    st.write(f"**Status:** ✅ Connected" if db.health_check() else "❌ Disconnected")
    
    st.divider()
    
    # Tab selection
    tab1, tab2, tab3 = st.tabs(["📊 Statistics", "📋 File Metadata", "📝 Process Logs"])
    
    with tab1:
        st.subheader("Database Statistics")
        
        if db_type == "mongodb":
            try:
                # Get collection stats
                col1, col2 = st.columns(2)
                
                with col1:
                    file_count = db.db['file_metadata'].count_documents({})
                    st.metric("File Metadata Records", file_count)
                
                with col2:
                    log_count = db.db['process_logs'].count_documents({})
                    st.metric("Process Log Entries", log_count)
            except Exception as e:
                st.error(f"Error fetching stats: {str(e)}")
    
    with tab2:
        st.subheader("File Metadata Records")
        
        if db_type == "mongodb":
            try:
                # Get metadata records
                records = list(db.db['file_metadata'].find().limit(100))
                
                if records:
                    # Convert to dataframe
                    df_records = pd.DataFrame([
                        {
                            'File Name': r.get('file_name'),
                            'Type': r.get('file_type'),
                            'Status': r.get('status'),
                            'Size (KB)': round(r.get('file_size', 0) / 1024, 2),
                            'Ingested': r.get('ingested_at')
                        }
                        for r in records
                    ])
                    
                    st.dataframe(df_records, use_container_width=True, height=400)
                else:
                    st.info("No metadata records found")
            except Exception as e:
                st.error(f"Error fetching records: {str(e)}")
    
    with tab3:
        st.subheader("Process Logs")
        
        if db_type == "mongodb":
            try:
                # Get recent logs
                logs = list(db.db['process_logs'].find().sort('timestamp', -1).limit(100))
                
                if logs:
                    df_logs = pd.DataFrame([
                        {
                            'File': l.get('file_name'),
                            'Status': l.get('status'),
                            'Message': l.get('message'),
                            'Timestamp': l.get('timestamp')
                        }
                        for l in logs
                    ])
                    
                    st.dataframe(df_logs, use_container_width=True, height=400)
                else:
                    st.info("No process logs found")
            except Exception as e:
                st.error(f"Error fetching logs: {str(e)}")


def show_analytics(db):
    """Analytics and metrics"""
    st.title("📈 Analytics")
    
    # Load logs
    logs = load_logs(days=7)
    
    if len(logs) == 0:
        st.info("No logs available for analysis")
        return
    
    st.subheader("Last 7 Days Activity")
    
    # Parse timestamps and create timeline
    try:
        logs['timestamp'] = pd.to_datetime(logs['timestamp'], errors='coerce')
        logs['date'] = logs['timestamp'].dt.date
        
        # Count by date and level
        daily_stats = pd.crosstab(logs['date'], logs['level'])
        
        fig = go.Figure()
        
        for level in daily_stats.columns:
            fig.add_trace(go.Scatter(
                x=daily_stats.index,
                y=daily_stats[level],
                mode='lines+markers',
                name=level,
                fill='tozeroy'
            ))
        
        fig.update_layout(
            title="Log Entries by Day and Level",
            xaxis_title="Date",
            yaxis_title="Count",
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error creating analytics: {str(e)}")


def show_settings(config):
    """Settings page"""
    st.title("⚙️ Settings")
    
    st.subheader("Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Database Settings**")
        st.json({
            "type": config.get('database', {}).get('type'),
            "mongodb": config.get('database', {}).get('mongodb', {})
        })
    
    with col2:
        st.write("**Scheduler Settings**")
        st.json(config.get('scheduler', {}))
    
    st.divider()
    
    st.write("**Logging Settings**")
    st.json(config.get('logging', {}))
    
    st.divider()
    
    st.subheader("Configuration File")
    
    with open("config/config.yaml", "r") as f:
        config_content = f.read()
    
    st.text_area(
        "config.yaml",
        value=config_content,
        height=400,
        disabled=True
    )


if __name__ == "__main__":
    main()
