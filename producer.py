from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timedelta

# Content phrases for variety
phrases = [
    "Just had an amazing day at the park!",
    "Loving this new tech gadget I got.",
    "Anyone seen the latest blockbuster movie?",
    "Cooking a new recipe tonight, wish me luck!",
    "Weekend vibes are the best, right?",
    "Thoughts on the new game release?",
    "Travel plans are shaping up nicely!",
    "Fitness goals are tough but worth it.",
    "Music festival was epic last night!",
    "Fashion trends this season are wild."
]

# Hashtag pool (some more common for realism)
hashtags_pool = ["#fun", "#tech", "#movies", "#food", "#weekend", "#gaming", "#travel", "#fitness", "#music", "#fashion"]

# Kafka topics
topics = ["top1", "top2", "top3"]

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def generate_post():
    post_id = str(random.randint(100000, 999999))
    user_id = f"user_{random.randint(1, 1000)}"
    content = random.choice(phrases)
    num_hashtags = random.randint(1, 4)  # 1 to 4 hashtags
    hashtags = random.sample(hashtags_pool, num_hashtags)
    # Simulate post time with slight delay (up to 2 minutes in the past)
    post_time = datetime.now() - timedelta(seconds=random.uniform(0, 120))
    likes = random.randint(0, 150)
    return {
        "post_id": post_id,
        "user_id": user_id,
        "content": content,
        "hashtags": hashtags,
        "post_timestamp": post_time.isoformat(),
        "likes": likes
    }

print("Starting producer...")
while True:
    post = generate_post()
    topic = random.choice(topics)
    producer.send(topic, post)
    print(f"Sent to {topic}: {post}")
    time.sleep(random.uniform(0.2, 1.5))  # Random delay between posts
