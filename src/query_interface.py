import pandas as pd
import numpy as np
import json
import os
from datetime import datetime
import argparse
import sys

class MarketIntelligenceQueryEngine:
    def __init__(self):
        self.unified_data = None
        self.insights_data = None
        self.load_all_data()

    def load_all_data(self):
        """Load unified dataset and insights from previous phases"""
        try:
            # Load unified dataset
            self.unified_data = pd.read_csv('data/processed/unified_app_data_free_api.csv')
            print(f"✅ Loaded unified dataset: {self.unified_data.shape}")

            # Load insights report
            with open('reports/phase3_llm_insights_report.json', 'r', encoding='utf-8') as f:
                self.insights_data = json.load(f)
            print(f"✅ Loaded AI insights report")

            return True
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            print("💡 Make sure you completed Phases 1-3 first!")
            return False

    def query_top_categories(self, limit=10):
        """Query top app categories by volume"""
        top_cats = self.unified_data['unified_category'].value_counts().head(limit)

        print(f"\n📱 TOP {limit} APP CATEGORIES BY VOLUME:")
        print("=" * 50)

        for i, (category, count) in enumerate(top_cats.items(), 1):
            avg_rating = self.unified_data[self.unified_data['unified_category'] == category]['rating'].mean()
            avg_reviews = self.unified_data[self.unified_data['unified_category'] == category]['review_count'].mean()
            free_pct = (self.unified_data[self.unified_data['unified_category'] == category]['app_type'] == 'Free').mean() * 100

            print(f"{i:2d}. {category}")
            print(f"    📊 Apps: {count:,}")
            print(f"    ⭐ Avg Rating: {avg_rating:.2f}")
            print(f"    💬 Avg Reviews: {avg_reviews:,.0f}")
            print(f"    🆓 Free Apps: {free_pct:.1f}%")
            print()

        return top_cats.to_dict()

    def query_platform_comparison(self):
        """Compare Android vs iOS platforms"""
        android_data = self.unified_data[self.unified_data['platform'] == 'Android']
        ios_data = self.unified_data[self.unified_data['platform'] == 'iOS']

        comparison = {
            'android': {
                'total_apps': len(android_data),
                'avg_rating': android_data['rating'].mean(),
                'avg_reviews': android_data['review_count'].mean(),
                'avg_price': android_data['price_usd'].mean(),
                'avg_size_mb': android_data['size_mb'].mean(),
                'free_apps_pct': (android_data['app_type'] == 'Free').mean() * 100,
                'top_categories': android_data['unified_category'].value_counts().head(5).to_dict()
            },
            'ios': {
                'total_apps': len(ios_data),
                'avg_rating': ios_data['rating'].mean(),
                'avg_reviews': ios_data['review_count'].mean(),
                'avg_price': ios_data['price_usd'].mean(),
                'avg_size_mb': ios_data['size_mb'].mean(),
                'free_apps_pct': (ios_data['app_type'] == 'Free').mean() * 100,
                'top_categories': ios_data['unified_category'].value_counts().head(5).to_dict()
            }
        }

        print("\n🏪 PLATFORM COMPARISON: ANDROID vs iOS")
        print("=" * 60)

        print(f"📱 ANDROID MARKET:")
        print(f"   Apps: {comparison['android']['total_apps']:,}")
        print(f"   Avg Rating: {comparison['android']['avg_rating']:.2f}/5.0")
        print(f"   Avg Reviews: {comparison['android']['avg_reviews']:,.0f}")
        print(f"   Avg Price: ${comparison['android']['avg_price']:.2f}")
        print(f"   Avg Size: {comparison['android']['avg_size_mb']:.1f} MB")
        print(f"   Free Apps: {comparison['android']['free_apps_pct']:.1f}%")

        print(f"\n🍎 iOS MARKET:")
        print(f"   Apps: {comparison['ios']['total_apps']:,}")
        print(f"   Avg Rating: {comparison['ios']['avg_rating']:.2f}/5.0")
        print(f"   Avg Reviews: {comparison['ios']['avg_reviews']:,.0f}")
        print(f"   Avg Price: ${comparison['ios']['avg_price']:.2f}")
        print(f"   Avg Size: {comparison['ios']['avg_size_mb']:.1f} MB")
        print(f"   Free Apps: {comparison['ios']['free_apps_pct']:.1f}%")

        print(f"\n📊 KEY DIFFERENCES:")
        rating_diff = comparison['ios']['avg_rating'] - comparison['android']['avg_rating']
        review_ratio = comparison['ios']['avg_reviews'] / comparison['android']['avg_reviews']
        price_diff = comparison['ios']['avg_price'] - comparison['android']['avg_price']

        print(f"   Quality Gap: iOS +{rating_diff:.2f} stars higher rating")
        print(f"   Engagement: iOS {review_ratio:.1f}x more reviews per app")
        print(f"   Pricing: Android ${abs(price_diff):.2f} {'higher' if price_diff < 0 else 'lower'} average price")

        return comparison

    def query_category_analysis(self, category):
        """Analyze specific category in detail"""
        cat_data = self.unified_data[self.unified_data['unified_category'] == category]

        if len(cat_data) == 0:
            print(f"❌ Category '{category}' not found!")
            return None

        # Platform breakdown
        android_apps = len(cat_data[cat_data['platform'] == 'Android'])
        ios_apps = len(cat_data[cat_data['platform'] == 'iOS'])

        # Quality metrics
        high_rated = len(cat_data[cat_data['rating'] >= 4.0])
        popular_apps = len(cat_data[cat_data['review_count'] >= 10000])

        # Pricing analysis
        free_apps = len(cat_data[cat_data['app_type'] == 'Free'])
        paid_apps = len(cat_data[cat_data['app_type'] == 'Paid'])
        avg_paid_price = cat_data[cat_data['price_usd'] > 0]['price_usd'].mean()

        print(f"\n🎯 DEEP DIVE: {category.upper()} CATEGORY")
        print("=" * 60)

        print(f"📊 OVERVIEW:")
        print(f"   Total Apps: {len(cat_data):,}")
        print(f"   Android: {android_apps:,} apps")
        print(f"   iOS: {ios_apps:,} apps")

        print(f"\n⭐ QUALITY METRICS:")
        print(f"   Average Rating: {cat_data['rating'].mean():.2f}/5.0")
        print(f"   High-Rated (4.0+): {high_rated:,} apps ({high_rated/len(cat_data)*100:.1f}%)")
        print(f"   Popular (10K+ reviews): {popular_apps:,} apps ({popular_apps/len(cat_data)*100:.1f}%)")
        print(f"   Average Reviews: {cat_data['review_count'].mean():,.0f}")

        print(f"\n💰 MONETIZATION:")
        print(f"   Free Apps: {free_apps:,} ({free_apps/len(cat_data)*100:.1f}%)")
        print(f"   Paid Apps: {paid_apps:,} ({paid_apps/len(cat_data)*100:.1f}%)")
        if not pd.isna(avg_paid_price):
            print(f"   Avg Paid Price: ${avg_paid_price:.2f}")

        print(f"\n📱 SIZE & PERFORMANCE:")
        print(f"   Average Size: {cat_data['size_mb'].mean():.1f} MB")
        print(f"   Size Range: {cat_data['size_mb'].min():.1f} - {cat_data['size_mb'].max():.1f} MB")

        # Top apps in category
        top_apps = cat_data.nlargest(5, 'review_count')[['app_name', 'platform', 'rating', 'review_count', 'price_usd']]
        print(f"\n🏆 TOP APPS IN {category.upper()}:")
        for _, app in top_apps.iterrows():
            print(f"   • {app['app_name']} ({app['platform']})")
            print(f"     Rating: {app['rating']:.1f} | Reviews: {app['review_count']:,.0f} | Price: ${app['price_usd']:.2f}")

        return cat_data.to_dict('records')

    def query_pricing_insights(self):
        """Analyze pricing strategies across platforms and categories"""
        # Overall pricing distribution
        free_apps = (self.unified_data['app_type'] == 'Free').sum()
        paid_apps = (self.unified_data['app_type'] == 'Paid').sum()

        # Price ranges
        paid_data = self.unified_data[self.unified_data['price_usd'] > 0]
        price_ranges = {
            'under_1': len(paid_data[paid_data['price_usd'] < 1]),
            'range_1_5': len(paid_data[(paid_data['price_usd'] >= 1) & (paid_data['price_usd'] <= 5)]),
            'range_5_10': len(paid_data[(paid_data['price_usd'] > 5) & (paid_data['price_usd'] <= 10)]),
            'over_10': len(paid_data[paid_data['price_usd'] > 10])
        }

        # Category pricing
        cat_pricing = self.unified_data.groupby('unified_category').agg({
            'price_usd': ['mean', 'count'],
            'app_type': lambda x: (x == 'Paid').mean() * 100
        }).round(2)

        # Platform pricing comparison
        android_paid = self.unified_data[(self.unified_data['platform'] == 'Android') & (self.unified_data['price_usd'] > 0)]['price_usd']
        ios_paid = self.unified_data[(self.unified_data['platform'] == 'iOS') & (self.unified_data['price_usd'] > 0)]['price_usd']

        print("\n💰 COMPREHENSIVE PRICING ANALYSIS")
        print("=" * 60)

        print(f"📊 MARKET DISTRIBUTION:")
        print(f"   Free Apps: {free_apps:,} ({free_apps/len(self.unified_data)*100:.1f}%)")
        print(f"   Paid Apps: {paid_apps:,} ({paid_apps/len(self.unified_data)*100:.1f}%)")

        print(f"\n💵 PRICE RANGES (Paid Apps Only):")
        print(f"   Under $1: {price_ranges['under_1']:,} apps")
        print(f"   $1 - $5: {price_ranges['range_1_5']:,} apps")
        print(f"   $5 - $10: {price_ranges['range_5_10']:,} apps") 
        print(f"   Over $10: {price_ranges['over_10']:,} apps")

        print(f"\n🏪 PLATFORM COMPARISON (Paid Apps):")
        if len(android_paid) > 0:
            print(f"   Android Avg: ${android_paid.mean():.2f} (from {len(android_paid)} apps)")
        if len(ios_paid) > 0:
            print(f"   iOS Avg: ${ios_paid.mean():.2f} (from {len(ios_paid)} apps)")

        print(f"\n🎯 TOP PREMIUM CATEGORIES:")
        premium_cats = cat_pricing.sort_values(('price_usd', 'mean'), ascending=False).head(5)
        for cat, data in premium_cats.iterrows():
            print(f"   {cat}: ${data[('price_usd', 'mean')]:.2f} avg price ({data[('app_type', '<lambda>')]:.1f}% paid)")

        return {
            'price_distribution': price_ranges,
            'platform_comparison': {
                'android_avg': android_paid.mean() if len(android_paid) > 0 else 0,
                'ios_avg': ios_paid.mean() if len(ios_paid) > 0 else 0
            }
        }

    def query_market_opportunities(self):
        """Identify market opportunities based on data analysis"""
        # Low competition, high quality potential
        cat_stats = self.unified_data.groupby('unified_category').agg({
            'app_name': 'count',
            'rating': 'mean',
            'review_count': 'mean',
            'price_usd': 'mean'
        }).round(2)

        # Identify opportunities
        low_competition = cat_stats[cat_stats['app_name'] < 200]  # Less than 200 apps
        quality_gaps = cat_stats[cat_stats['rating'] < 4.0]  # Below 4.0 average
        premium_potential = cat_stats[cat_stats['price_usd'] > 2.0]  # Higher price tolerance

        print("\n🎯 MARKET OPPORTUNITY ANALYSIS")
        print("=" * 60)

        print(f"🔹 LOW COMPETITION CATEGORIES (<200 apps):")
        low_comp_sorted = low_competition.sort_values('rating', ascending=False).head(5)
        for cat, data in low_comp_sorted.iterrows():
            print(f"   • {cat}: {int(data['app_name'])} apps (avg rating: {data['rating']:.2f})")

        print(f"\n🔸 QUALITY GAP OPPORTUNITIES (<4.0 avg rating):")
        quality_gap_sorted = quality_gaps.sort_values('app_name', ascending=False).head(5)
        for cat, data in quality_gap_sorted.iterrows():
            print(f"   • {cat}: {int(data['app_name'])} apps (avg rating: {data['rating']:.2f})")

        print(f"\n💎 PREMIUM PRICING OPPORTUNITIES (>$2.0 avg):")
        premium_sorted = premium_potential.sort_values('price_usd', ascending=False).head(5)
        for cat, data in premium_sorted.iterrows():
            print(f"   • {cat}: ${data['price_usd']:.2f} avg price ({int(data['app_name'])} apps)")

        print(f"\n🚀 STRATEGIC RECOMMENDATIONS:")
        print(f"   1. Target low-competition categories with quality-first approach")
        print(f"   2. Improve UX/quality in saturated categories with rating gaps")
        print(f"   3. Consider premium positioning in categories with price tolerance")
        print(f"   4. Cross-platform strategy for maximum market penetration")

        return {
            'low_competition': low_comp_sorted.to_dict('index'),
            'quality_gaps': quality_gap_sorted.to_dict('index'),
            'premium_potential': premium_sorted.to_dict('index')
        }

    def query_ai_insights_summary(self):
        """Display AI-generated insights from Phase 3"""
        if not self.insights_data:
            print("❌ No AI insights available. Run Phase 3 first!")
            return None

        print("\n🧠 AI-GENERATED MARKET INSIGHTS SUMMARY")
        print("=" * 60)

        confidence_scores = self.insights_data.get('confidence_scores', {})
        ai_insights = self.insights_data.get('ai_insights', {})

        print(f"🎯 CONFIDENCE SCORES:")
        for insight_type, score in confidence_scores.items():
            print(f"   {insight_type.replace('_', ' ').title()}: {score:.0f}%")

        print(f"\n📊 MARKET TRENDS (Confidence: {confidence_scores.get('market_trends', 0):.0f}%):")
        trends = ai_insights.get('market_trends', 'No trends analysis available')
        print(f"   {trends[:300]}..." if len(trends) > 300 else f"   {trends}")

        print(f"\n🏆 COMPETITIVE ANALYSIS (Confidence: {confidence_scores.get('competitive_analysis', 0):.0f}%):")
        competitive = ai_insights.get('competitive_analysis', 'No competitive analysis available')
        print(f"   {competitive[:300]}..." if len(competitive) > 300 else f"   {competitive}")

        print(f"\n💰 PRICING STRATEGY (Confidence: {confidence_scores.get('pricing_strategy', 0):.0f}%):")
        pricing = ai_insights.get('pricing_strategy', 'No pricing strategy available')
        print(f"   {pricing[:300]}..." if len(pricing) > 300 else f"   {pricing}")

        return ai_insights

    def interactive_query_mode(self):
        """Interactive query interface"""
        print("\n🚀 INTERACTIVE MARKET INTELLIGENCE QUERY ENGINE")
        print("=" * 60)
        print("Type 'help' for available commands, 'quit' to exit")

        while True:
            try:
                query = input("\n🔍 Enter query: ").strip().lower()

                if query == 'quit' or query == 'exit':
                    print("👋 Goodbye! Thank you for using Market Intelligence Query Engine.")
                    break

                elif query == 'help':
                    print("\n📋 AVAILABLE COMMANDS:")
                    print("  categories [N]     - Show top N categories (default: 10)")
                    print("  platforms          - Compare Android vs iOS")
                    print("  category <name>    - Analyze specific category")
                    print("  pricing            - Pricing analysis across market")
                    print("  opportunities      - Identify market opportunities")
                    print("  insights           - Show AI-generated insights")
                    print("  summary            - Quick market overview")
                    print("  help               - Show this help")
                    print("  quit               - Exit the query engine")

                elif query.startswith('categories'):
                    try:
                        limit = int(query.split()[1]) if len(query.split()) > 1 else 10
                        self.query_top_categories(limit)
                    except:
                        self.query_top_categories()

                elif query == 'platforms':
                    self.query_platform_comparison()

                elif query.startswith('category'):
                    if len(query.split()) > 1:
                        category = ' '.join(query.split()[1:]).title()
                        self.query_category_analysis(category)
                    else:
                        print("❌ Please specify a category name: category Games")

                elif query == 'pricing':
                    self.query_pricing_insights()

                elif query == 'opportunities':
                    self.query_market_opportunities()

                elif query == 'insights':
                    self.query_ai_insights_summary()

                elif query == 'summary':
                    print(f"\n📊 QUICK MARKET OVERVIEW:")
                    print(f"   Total Apps: {len(self.unified_data):,}")
                    print(f"   Android: {(self.unified_data['platform'] == 'Android').sum():,}")
                    print(f"   iOS: {(self.unified_data['platform'] == 'iOS').sum():,}")
                    print(f"   Categories: {self.unified_data['unified_category'].nunique()}")
                    print(f"   Average Rating: {self.unified_data['rating'].mean():.2f}/5.0")
                    print(f"   High-Quality Apps (4.0+): {(self.unified_data['rating'] >= 4.0).sum():,}")

                else:
                    print("❌ Unknown command. Type 'help' for available commands.")

            except KeyboardInterrupt:
                print("\n\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"❌ Error processing query: {e}")

    def run_cli_mode(self):
        """Run in command-line argument mode"""
        parser = argparse.ArgumentParser(description='Market Intelligence Query Engine')
        parser.add_argument('--categories', type=int, help='Show top N categories')
        parser.add_argument('--platforms', action='store_true', help='Compare platforms')
        parser.add_argument('--category', type=str, help='Analyze specific category')
        parser.add_argument('--pricing', action='store_true', help='Show pricing analysis')
        parser.add_argument('--opportunities', action='store_true', help='Show market opportunities')
        parser.add_argument('--insights', action='store_true', help='Show AI insights')
        parser.add_argument('--interactive', action='store_true', help='Start interactive mode')

        args = parser.parse_args()

        if args.interactive:
            self.interactive_query_mode()
        elif args.categories:
            self.query_top_categories(args.categories)
        elif args.platforms:
            self.query_platform_comparison()
        elif args.category:
            self.query_category_analysis(args.category)
        elif args.pricing:
            self.query_pricing_insights()
        elif args.opportunities:
            self.query_market_opportunities()
        elif args.insights:
            self.query_ai_insights_summary()
        else:
            print("🚀 MARKET INTELLIGENCE QUERY ENGINE")
            print("=" * 50)
            print("Use --help for available options or --interactive for interactive mode")
            print("\nQuick commands:")
            print("  python query_engine.py --interactive")
            print("  python query_engine.py --categories 5")
            print("  python query_engine.py --platforms")
            print("  python query_engine.py --opportunities")

def main():
    """Main execution function"""
    engine = MarketIntelligenceQueryEngine()

    if len(sys.argv) == 1:
        # No arguments - start interactive mode
        engine.interactive_query_mode()
    else:
        # Command-line arguments provided
        engine.run_cli_mode()

if __name__ == "__main__":
    main()
