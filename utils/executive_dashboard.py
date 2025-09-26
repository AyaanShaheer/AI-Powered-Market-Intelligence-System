import pandas as pd
import json
from datetime import datetime

def generate_executive_dashboard():
    """Generate executive dashboard summary"""

    print("🚀 AI-POWERED MARKET INTELLIGENCE - EXECUTIVE DASHBOARD")
    print("=" * 70)
    print(f"Generated: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}")

    try:
        # Load data
        unified_data = pd.read_csv('data/processed/unified_app_data_free_api.csv')

        with open('reports/phase3_llm_insights_report.json', 'r') as f:
            insights_data = json.load(f)

        print("\n📊 MARKET OVERVIEW")
        print("-" * 30)
        print(f"Total Apps Analyzed: {len(unified_data):,}")
        print(f"Android Apps: {(unified_data['platform'] == 'Android').sum():,}")
        print(f"iOS Apps: {(unified_data['platform'] == 'iOS').sum():,}")
        print(f"Categories Covered: {unified_data['unified_category'].nunique()}")
        print(f"Data Completeness: {((unified_data.notna().sum().sum() / (unified_data.shape[0] * unified_data.shape[1])) * 100):.1f}%")

        print("\n⭐ QUALITY METRICS")
        print("-" * 30)
        print(f"Average Rating: {unified_data['rating'].mean():.2f}/5.0")
        print(f"High-Quality Apps (4.0+): {(unified_data['rating'] >= 4.0).sum():,} ({(unified_data['rating'] >= 4.0).mean()*100:.1f}%)")
        print(f"Excellent Apps (4.5+): {(unified_data['rating'] >= 4.5).sum():,} ({(unified_data['rating'] >= 4.5).mean()*100:.1f}%)")
        print(f"Popular Apps (10K+ reviews): {(unified_data['review_count'] >= 10000).sum():,}")

        print("\n🏪 PLATFORM COMPARISON")
        print("-" * 30)
        android_rating = unified_data[unified_data['platform'] == 'Android']['rating'].mean()
        ios_rating = unified_data[unified_data['platform'] == 'iOS']['rating'].mean()
        android_reviews = unified_data[unified_data['platform'] == 'Android']['review_count'].mean()
        ios_reviews = unified_data[unified_data['platform'] == 'iOS']['review_count'].mean()

        print(f"Android Avg Rating: {android_rating:.2f}/5.0")
        print(f"iOS Avg Rating: {ios_rating:.2f}/5.0")
        print(f"Quality Gap: iOS +{(ios_rating - android_rating):.2f} stars")
        print(f"Engagement Ratio: iOS {(ios_reviews/android_reviews):.1f}x more reviews")

        print("\n💰 MONETIZATION INSIGHTS")
        print("-" * 30)
        free_pct = (unified_data['app_type'] == 'Free').mean() * 100
        paid_pct = (unified_data['app_type'] == 'Paid').mean() * 100
        avg_price = unified_data['price_usd'].mean()

        print(f"Free Apps: {free_pct:.1f}%")
        print(f"Paid Apps: {paid_pct:.1f}%")
        print(f"Average Price: ${avg_price:.2f}")
        print(f"Premium Apps (>$5): {(unified_data['price_usd'] > 5).sum():,}")

        print("\n📱 TOP CATEGORIES")
        print("-" * 30)
        top_cats = unified_data['unified_category'].value_counts().head(5)
        for i, (cat, count) in enumerate(top_cats.items(), 1):
            avg_rating = unified_data[unified_data['unified_category'] == cat]['rating'].mean()
            print(f"{i}. {cat}: {count:,} apps (avg rating: {avg_rating:.2f})")

        print("\n🎯 AI INSIGHTS CONFIDENCE")
        print("-" * 30)
        confidence_scores = insights_data.get('confidence_scores', {})
        for insight_type, score in confidence_scores.items():
            print(f"{insight_type.replace('_', ' ').title()}: {score:.0f}%")

        print("\n🚀 STRATEGIC PRIORITIES")
        print("-" * 30)
        print("1. Platform-Specific Strategy: iOS premium, Android volume")
        print("2. Quality Differentiation: Target 4.5+ rating benchmark")
        print("3. Category Opportunities: Focus on underserved segments")
        print("4. Cross-Platform Synergy: Leverage platform strengths")

        print("\n📂 GENERATED REPORTS")
        print("-" * 30)
        print("✅ Unified Dataset: data/processed/unified_app_data_free_api.csv")
        print("✅ AI Insights: reports/phase3_llm_insights_report.json")
        print("✅ Executive Report: reports/executive_market_intelligence_report.md")
        print("✅ Query Interface: phase4_query_interface.py")

        print("\n🔍 NEXT ACTIONS")
        print("-" * 30)
        print("• Run: python phase4_query_interface.py --interactive")
        print("• Explore specific categories and opportunities")
        print("• Use insights for strategic decision making")
        print("• Proceed to Phase 5 for D2C dataset analysis")

    except Exception as e:
        print(f"❌ Error loading dashboard data: {e}")
        print("💡 Make sure you completed Phases 1-3 successfully!")

if __name__ == "__main__":
    generate_executive_dashboard()
