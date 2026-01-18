import json
import time
import praw
import os
import datetime

# ==== YOUR REDDIT CREDENTIALS ====
client_id = "GgMlRkv2TebIGeDCvfiEzA"
client_secret = "_X0EWu8HxjcF62OYf-ila_yE_vuztg"
username = "ZealousidealPower479"
password = "Bigdata7012"
user_agent = "ClimateSentimentApp"

reddit = praw.Reddit(
    client_id=client_id,
    client_secret=client_secret,
    username=username,
    password=password,
    user_agent=user_agent
)

subreddits = [
    "climatechange",
    "environment",
    "worldnews",
    "science",
    "renewableenergy",
    "sustainability",
    "climate",
    "climateaction"
]

# --- FIX TIMESTAMP (Minimal Change) ---
def fix_timestamp(utc_ts):
    utc_dt = datetime.datetime.utcfromtimestamp(utc_ts).replace(tzinfo=datetime.timezone.utc)
    local_dt = utc_dt.astimezone()
    return int(local_dt.timestamp())

# Output directory
output_dir = os.path.join(os.path.expanduser("~"), "StreamingProject", "stream_in")
os.makedirs(output_dir, exist_ok=True)

seen_posts = set()
seen_comments = set()

print("✅ Reddit producer started.")
print("Writing files to:", output_dir)

while True:
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_dir, f"reddit_{timestamp}.jsonl")

    new = 0

    with open(file_path, "w", encoding="utf-8") as f:

        # ---- Posts ----
        for sub in subreddits:
            for post in reddit.subreddit(sub).new(limit=10):
                if post.id in seen_posts:
                    continue
                seen_posts.add(post.id)

                data = {
                    "type": "post",
                    "subreddit": sub,
                    "title": post.title,
                    "text": post.selftext,
                    "created_utc": fix_timestamp(post.created_utc),  # PATCHED
                    "score": int(post.score)
                }
                f.write(json.dumps(data) + "\n")
                new += 1

        # ---- Comments ----
        for sub in subreddits:
            for comment in reddit.subreddit(sub).comments(limit=20):
                if comment.id in seen_comments:
                    continue
                seen_comments.add(comment.id)

                data = {
                    "type": "comment",
                    "subreddit": sub,
                    "title": None,
                    "text": comment.body,
                    "created_utc": fix_timestamp(comment.created_utc),  # PATCHED
                    "score": int(comment.score)
                }
                f.write(json.dumps(data) + "\n")
                new += 1

    if new == 0:
        print("⚠️ No new data this cycle.")
        os.remove(file_path)
    else:
        print(f"📄 Saved {new} records:", file_path)

    time.sleep(20)
