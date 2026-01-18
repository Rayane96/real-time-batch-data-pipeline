import streamlit as st
import pandas as pd
import plotly.express as px
import glob
import os
from streamlit_autorefresh import st_autorefresh

# --------------------------------------------
# Force refresh of cached data
# --------------------------------------------
st.cache_data.clear()

# --------------------------------------------
# Page Setup
# --------------------------------------------
st.set_page_config(page_title="Climate Sentiment Monitor", layout="wide")
st.title("🌍 Real-Time Climate Change Sentiment Tracker")

# --------------------------------------------
# Refresh Control
# --------------------------------------------
refresh_sec = st.sidebar.slider("Refresh every (seconds)", 5, 60, 15)
st_autorefresh(interval=refresh_sec * 1000, key="dashboard_refresh")

# --------------------------------------------
# Load Latest Parquet Files
# --------------------------------------------
def load_data():
    base = r"C:\Users\Rayane\StreamingProject\stream_out"
    files = glob.glob(os.path.join(base, "**", "*.parquet"), recursive=True)

    if not files:
        return pd.DataFrame()

    # Load only the most recent 120 parquet files
    files = sorted(files, key=os.path.getmtime)[-120:]

    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_parquet(f))
        except:
            pass  # skip corrupted or empty files

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)

df = load_data()

if df.empty:
    st.warning("⏳ Waiting for streaming data…")
    st.stop()

# --------------------------------------------
# Time Cleanup
# --------------------------------------------
df = df.dropna(subset=["time"])
df["time"] = pd.to_datetime(df["time"], errors="coerce")

# Normalize timezone safely
if df["time"].dt.tz is not None:
    df["time"] = df["time"].dt.tz_convert(None)

now = pd.Timestamp.utcnow().tz_localize(None)

# Sort by time
df = df.sort_values("time").reset_index(drop=True)

# --------------------------------------------
# METRICS (FIXED FOR FUTURE TIMESTAMPS)
# --------------------------------------------
window_start = now - pd.Timedelta(minutes=2)

# Fix: treat ANY future timestamp as “recent”
# If Spark writes delays or your OS clock is off, this still works
time_adj = df["time"].clip(upper=now)  # clamp future to 'now'

recent_count = (time_adj >= window_start).sum()

# --------------------------------------------
# Display Metrics
# --------------------------------------------
st.info(f"⏱ Last Updated: {now.strftime('%H:%M:%S')}")

colA, colB = st.columns(2)
colA.metric("Total Records Processed", f"{len(df):,}")
colB.metric("New Data (Last 2 Minutes)", f"{recent_count:,}")

# --------------------------------------------
# Sentiment Distribution
# --------------------------------------------
st.subheader("💬 Sentiment Distribution")
sent = df["sentiment"].value_counts().reset_index()
sent.columns = ["sentiment", "count"]

fig1 = px.bar(sent, x="sentiment", y="count", text="count", color="sentiment")
st.plotly_chart(fig1, use_container_width=True)

# --------------------------------------------
# Average Sentiment by Subreddit
# --------------------------------------------
st.subheader("🏛️ Top Subreddits by Average Sentiment")

sub_avg = df.groupby("subreddit")["vader_score"].mean().reset_index()
sub_avg = sub_avg.sort_values("vader_score", ascending=False).head(10)

fig2 = px.bar(sub_avg, x="subreddit", y="vader_score")
st.plotly_chart(fig2, use_container_width=True)

# --------------------------------------------
# Real-Time Sentiment Trend (Last 90 min)
# --------------------------------------------
st.subheader("📈 Sentiment Trend (Last 90 Minutes)")

df_recent = df[df["time"] > (now - pd.Timedelta(minutes=90))]

if len(df_recent) > 5:
    trend = (
        df_recent.set_index("time")
        .resample("1min")["vader_score"]
        .mean()
        .dropna()
        .reset_index()
    )

    fig3 = px.line(trend, x="time", y="vader_score", markers=True)
    fig3.update_layout(yaxis_title="Sentiment Score")
    st.plotly_chart(fig3, use_container_width=True)
else:
    st.info("⏳ Gathering enough recent data to plot trend…")

# --------------------------------------------
# Top Positive / Negative Posts
# --------------------------------------------
st.subheader("⭐ Top Posts")

col1, col2 = st.columns(2)

top_pos = df[df["sentiment"] == "positive"].nlargest(5, "vader_score")
top_neg = df[df["sentiment"] == "negative"].nsmallest(5, "vader_score")

col1.write("### 😊 Most Positive")
col1.table(top_pos[["time", "subreddit", "title", "vader_score"]])

col2.write("### 😡 Most Negative")
col2.table(top_neg[["time", "subreddit", "title", "vader_score"]])

# --------------------------------------------
# Latest Incoming Posts
# --------------------------------------------
st.subheader("📝 Latest Incoming Posts")

st.dataframe(
    df.sort_values("time", ascending=False)[
        ["time", "subreddit", "title", "sentiment"]
    ].head(20)
)
