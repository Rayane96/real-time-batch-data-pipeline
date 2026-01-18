"""
Batch sentiment analysis for climate-related Reddit posts.
"""
import streamlit as st
import pandas as pd
import altair as alt

# ----------------------------------------------------------
# PAGE SETTINGS
# ----------------------------------------------------------
st.set_page_config(page_title="Climate Sentiment Dashboard", layout="wide")

# ----------------------------------------------------------
# LOAD DATA
# ----------------------------------------------------------
df = pd.read_csv("C:/Users/Rayane/OneDrive/Desktop/semster1/7012_pro/adjusted_time_sentiment_analysis.csv")

# Fix column name if needed
if "final_sentir" in df.columns:
    df.rename(columns={"final_sentir": "final_sentiment"}, inplace=True)

# Convert created_date → datetime automatically
df["created_date"] = pd.to_datetime(df["created_date"], format="mixed", dayfirst=True)

# Extract year-month for grouping
df["YearMonth"] = df["created_date"].dt.to_period("M").astype(str)


# ----------------------------------------------------------
# SIDEBAR FILTERS
# ----------------------------------------------------------
st.sidebar.header("Filters")

# Subreddit filter
subreddits = ["All"] + sorted(df["subreddit"].unique())
selected_sub = st.sidebar.selectbox("Choose Subreddit", subreddits)

if selected_sub != "All":
    df = df[df["subreddit"] == selected_sub]

# Date range filter
min_d, max_d = df["created_date"].min(), df["created_date"].max()
date_range = st.sidebar.date_input("Date Range", [min_d, max_d])

df = df[
    (df["created_date"] >= pd.to_datetime(date_range[0])) &
    (df["created_date"] <= pd.to_datetime(date_range[1]))
]

# ----------------------------------------------------------
# KPI METRICS
# ----------------------------------------------------------
st.title("🌍 Climate / Environment Sentiment Dashboard")

col1, col2, col3, col4 = st.columns(4)

total_posts = len(df)
pos = df["final_sentiment"].eq("positive").mean() * 100
neu = df["final_sentiment"].eq("neutral").mean() * 100
neg = df["final_sentiment"].eq("negative").mean() * 100

col1.metric("Total Posts", f"{total_posts:,}")
col2.metric("Positive %", f"{pos:.2f}%")
col3.metric("Neutral %", f"{neu:.2f}%")
col4.metric("Negative %", f"{neg:.2f}%")


# ----------------------------------------------------------
# SENTIMENT TREND OVER TIME
# ----------------------------------------------------------
st.subheader("📈 Sentiment Trend Over Time")

line = (
    alt.Chart(df)
    .mark_line()
    .encode(
        x="created_date:T",
        y="count():Q",
        color="final_sentiment:N"
    )
    .properties(height=400)
)

st.altair_chart(line, use_container_width=True)


# ----------------------------------------------------------
# SENTIMENT BY SUBREDDIT
# ----------------------------------------------------------
st.subheader("📊 Sentiment by Subreddit")

sub_bar = (
    df.groupby(["subreddit", "final_sentiment"])
    .size()
    .reset_index(name="count")
)

bar = (
    alt.Chart(sub_bar)
    .mark_bar()
    .encode(
        x="subreddit:N",
        y="count:Q",
        color="final_sentiment:N",
        tooltip=["subreddit", "final_sentiment", "count"]
    )
).properties(width=900)

st.altair_chart(bar, use_container_width=True)


# ----------------------------------------------------------
# TOP SUBREDDITS (Ranking)
# ----------------------------------------------------------
st.subheader("🏆 Top Subreddits by Activity")

top_subs = df["subreddit"].value_counts().reset_index()
top_subs.columns = ["subreddit", "post_count"]
top_subs = top_subs.head(10)

ranking = (
    alt.Chart(top_subs)
    .mark_bar()
    .encode(
        x="post_count:Q",
        y=alt.Y("subreddit:N", sort="-x"),
        tooltip=["subreddit", "post_count"]
    )
)

st.altair_chart(ranking, use_container_width=True)
