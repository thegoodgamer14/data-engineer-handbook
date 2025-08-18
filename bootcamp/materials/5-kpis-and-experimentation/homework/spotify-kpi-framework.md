# Spotify KPI Framework

## North Star Metric
**Monthly Active Users (MAU) with >30 minutes listening time**

This metric captures both user retention and engagement depth, reflecting Spotify's core mission of creating lasting musical relationships.

## Primary KPIs

### 1. Premium Conversion Rate
**What it measures:** Percentage of free users who convert to premium within 90 days
**Why it's important:** Direct revenue driver and indicates product value perception
**Calculation:** (Premium conversions in period / New free users 90 days prior) × 100
**Target:** 15-20% quarterly conversion rate

### 2. User Engagement Score
**What it measures:** Composite score of listening time, skip rate, and discovery actions
**Why it's important:** Indicates content satisfaction and algorithm effectiveness
**Calculation:** 
```
Engagement Score = (Daily listening minutes × 0.4) + 
                  ((1 - Skip rate) × 100 × 0.3) + 
                  (Discovery actions per session × 10 × 0.3)
```
**Target:** Score >70 for active users

### 3. Discovery Success Rate
**What it measures:** Percentage of recommended songs that users save or add to playlists
**Why it's important:** Core differentiator and driver of user satisfaction
**Calculation:** (Songs saved from recommendations / Total recommended songs played >30s) × 100
**Target:** 8-12% for personalized recommendations

### 4. Churn Rate (Monthly)
**What it measures:** Percentage of users who cancel or don't renew subscriptions
**Why it's important:** Retention is more cost-effective than acquisition
**Calculation:** (Users who churned in month / Total users at start of month) × 100
**Target:** <5% monthly for premium, <15% for free tier

### 5. Time to First Save
**What it measures:** Average time from account creation to first saved song/playlist
**Why it's important:** Early engagement predictor and onboarding effectiveness
**Calculation:** Average hours between account creation and first save action
**Target:** <24 hours for 70% of new users

## Supporting Metrics
- Daily Active Users (DAU)
- Average session duration
- Playlist creation rate
- Social sharing frequency
- Customer Lifetime Value (CLV)
- Cost per acquisition (CPA)