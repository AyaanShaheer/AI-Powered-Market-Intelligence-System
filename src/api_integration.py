import pandas as pd
import numpy as np
import json
import requests
import time
import os
from datetime import datetime
import logging

class FreeAPIIntegrationPipeline:
    def __init__(self):
        self.google_play_data = None
        self.itunes_data = None
        self.unified_data = None
        self.api_stats = {}
        self.setup_logging()

    def setup_logging(self):
        """Setup logging for API calls"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('api_integration.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def load_cleaned_googleplay_data(self):
        """Load cleaned Google Play Store data from Phase 1"""
        try:
            self.google_play_data = pd.read_csv('data/processed/googleplay_cleaned.csv')
            print(f"✅ Loaded Google Play data: {self.google_play_data.shape}")
            return True
        except Exception as e:
            print(f"❌ Error loading Google Play data: {e}")
            return False

    def fetch_itunes_apps_free_api(self):
        """Fetch iOS apps using FREE iTunes Search API"""
        print("🔄 Fetching iOS apps using iTunes Search API (FREE)...")

        # Popular app categories to search for
        search_terms = [
            'instagram', 'whatsapp', 'spotify', 'netflix', 'uber', 'airbnb',
            'youtube', 'facebook', 'twitter', 'tiktok', 'snapchat', 'linkedin',
            'zoom', 'teams', 'slack', 'discord', 'telegram', 'pinterest',
            'amazon', 'ebay', 'paypal', 'cashapp', 'venmo', 'banking',
            'fitness', 'meditation', 'calendar', 'notes', 'weather', 'maps',
            'games', 'puzzle', 'racing', 'rpg', 'strategy', 'action',
            'photo', 'camera', 'editor', 'music', 'podcast', 'news',
            'shopping', 'food', 'travel', 'booking', 'recipe', 'health'
        ]

        all_apps = []
        base_url = "https://itunes.apple.com/search"

        for term in search_terms:
            try:
                # Search for iOS apps only
                params = {
                    'term': term,
                    'country': 'us',
                    'media': 'software',  # iOS apps
                    'entity': 'software',
                    'limit': 20  # Get up to 20 apps per search term
                }

                response = requests.get(base_url, params=params)
                time.sleep(0.5)  # Be respectful to the free API

                if response.status_code == 200:
                    data = response.json()
                    apps = data.get('results', [])

                    for app in apps:
                        # Extract relevant app information
                        app_info = {
                            'trackId': app.get('trackId'),
                            'trackName': app.get('trackName', ''),
                            'artistName': app.get('artistName', ''),
                            'primaryGenreName': app.get('primaryGenreName', ''),
                            'averageUserRating': app.get('averageUserRating', 0),
                            'userRatingCount': app.get('userRatingCount', 0),
                            'price': app.get('price', 0),
                            'currency': app.get('currency', 'USD'),
                            'contentAdvisoryRating': app.get('contentAdvisoryRating', ''),
                            'fileSizeBytes': app.get('fileSizeBytes', 0),
                            'formattedPrice': app.get('formattedPrice', 'Free'),
                            'releaseDate': app.get('releaseDate', ''),
                            'currentVersionReleaseDate': app.get('currentVersionReleaseDate', ''),
                            'bundleId': app.get('bundleId', ''),
                            'trackViewUrl': app.get('trackViewUrl', ''),
                            'description': app.get('description', '')[:500],  # Limit description length
                            'version': app.get('version', ''),
                            'minimumOsVersion': app.get('minimumOsVersion', '')
                        }
                        all_apps.append(app_info)

                    print(f"  📱 Found {len(apps)} apps for '{term}'")
                else:
                    print(f"  ⚠️ Failed to search for '{term}': {response.status_code}")

            except Exception as e:
                print(f"  ❌ Error searching for '{term}': {e}")
                continue

        # Remove duplicates based on trackId
        unique_apps = {app['trackId']: app for app in all_apps if app['trackId']}
        self.itunes_data = pd.DataFrame(list(unique_apps.values()))

        self.api_stats['itunes_apps_fetched'] = len(self.itunes_data)
        self.api_stats['search_terms_used'] = len(search_terms)
        self.api_stats['api_calls_made'] = len(search_terms)

        print(f"✅ Collected {len(self.itunes_data)} unique iOS apps from iTunes API")
        return True

    def map_categories(self):
        """Map categories between Google Play and iTunes to unified categories"""
        category_mapping = {
            # Google Play -> Unified
            'ART_AND_DESIGN': 'Creative',
            'AUTO_AND_VEHICLES': 'Lifestyle',
            'BEAUTY': 'Lifestyle',
            'BOOKS_AND_REFERENCE': 'Education',
            'BUSINESS': 'Business',
            'COMICS': 'Entertainment',
            'COMMUNICATION': 'Social',
            'DATING': 'Social',
            'EDUCATION': 'Education',
            'ENTERTAINMENT': 'Entertainment',
            'EVENTS': 'Lifestyle',
            'FAMILY': 'Family',
            'FINANCE': 'Finance',
            'FOOD_AND_DRINK': 'Food & Drink',
            'GAME': 'Games',
            'HEALTH_AND_FITNESS': 'Health & Fitness',
            'HOUSE_AND_HOME': 'Lifestyle',
            'LIBRARIES_AND_DEMO': 'Developer Tools',
            'LIFESTYLE': 'Lifestyle',
            'MAPS_AND_NAVIGATION': 'Navigation',
            'MEDICAL': 'Medical',
            'MUSIC_AND_AUDIO': 'Music',
            'NEWS_AND_MAGAZINES': 'News',
            'PARENTING': 'Family',
            'PERSONALIZATION': 'Utilities',
            'PHOTOGRAPHY': 'Photo & Video',
            'PRODUCTIVITY': 'Productivity',
            'SHOPPING': 'Shopping',
            'SOCIAL': 'Social',
            'SPORTS': 'Sports',
            'TOOLS': 'Utilities',
            'TRAVEL_AND_LOCAL': 'Travel',
            'VIDEO_PLAYERS': 'Photo & Video',
            'WEATHER': 'Weather',

            # iTunes/iOS App Store -> Unified
            'Games': 'Games',
            'Business': 'Business',
            'Education': 'Education',
            'Entertainment': 'Entertainment',
            'Finance': 'Finance',
            'Health & Fitness': 'Health & Fitness',
            'Lifestyle': 'Lifestyle',
            'Music': 'Music',
            'News': 'News',
            'Photo & Video': 'Photo & Video',
            'Productivity': 'Productivity',
            'Social Networking': 'Social',
            'Sports': 'Sports',
            'Travel': 'Travel',
            'Utilities': 'Utilities',
            'Shopping': 'Shopping',
            'Food & Drink': 'Food & Drink',
            'Medical': 'Medical',
            'Navigation': 'Navigation',
            'Reference': 'Education',
            'Weather': 'Weather'
        }

        return category_mapping

    def safe_convert_to_float(self, value):
        """Safely convert value to float, handling various data types"""
        if pd.isna(value) or value is None or value == '':
            return 0.0

        try:
            # If it's already a number, return it
            if isinstance(value, (int, float)):
                return float(value)

            # If it's a string, try to convert
            if isinstance(value, str):
                # Remove any whitespace
                value = value.strip()
                if value == '' or value.lower() in ['nan', 'null', 'none']:
                    return 0.0
                return float(value)

            # For any other type, try direct conversion
            return float(value)

        except (ValueError, TypeError):
            return 0.0

    def safe_convert_to_int(self, value):
        """Safely convert value to int, handling various data types"""
        float_val = self.safe_convert_to_float(value)
        return int(float_val)

    def create_unified_schema(self):
        """Create unified schema combining Google Play and iTunes data"""
        print("🏗️ Creating unified schema...")

        category_mapping = self.map_categories()

        # Process Google Play data
        gp_unified = []
        for _, row in self.google_play_data.iterrows():
            unified_app = {
                'app_id': f'gp_{hash(str(row["app_name"])) % 1000000}',
                'app_name': str(row['app_name']),
                'platform': 'Android',
                'unified_category': category_mapping.get(row['category'], row['category']),
                'original_category': row['category'],
                'rating': self.safe_convert_to_float(row['rating']),
                'review_count': self.safe_convert_to_float(row['review_count']),
                'installs': self.safe_convert_to_int(row['installs']),
                'size_mb': self.safe_convert_to_float(row['size_mb']),
                'app_type': str(row['app_type']),
                'price_usd': self.safe_convert_to_float(row['price_usd']),
                'content_rating': str(row['content_rating']),
                'last_updated': row['last_updated'],
                'genres': str(row.get('genres', '')),
                'data_source': 'Google Play Store',
                'developer': '',
                'version': str(row.get('current_version', '')),
                'min_os_version': str(row.get('android_version', ''))
            }
            gp_unified.append(unified_app)

        # Process iTunes data with safe conversions
        ios_unified = []
        for _, row in self.itunes_data.iterrows():
            # Convert file size from bytes to MB safely
            file_size_bytes = self.safe_convert_to_float(row['fileSizeBytes'])
            size_mb = file_size_bytes / (1024 * 1024) if file_size_bytes > 0 else 0

            unified_app = {
                'app_id': f'ios_{self.safe_convert_to_int(row["trackId"])}',
                'app_name': str(row['trackName']),
                'platform': 'iOS',
                'unified_category': category_mapping.get(str(row['primaryGenreName']), str(row['primaryGenreName'])),
                'original_category': str(row['primaryGenreName']),
                'rating': self.safe_convert_to_float(row['averageUserRating']),
                'review_count': self.safe_convert_to_int(row['userRatingCount']),
                'installs': 0,  # iTunes API doesn't provide download count
                'size_mb': size_mb,
                'app_type': 'Free' if self.safe_convert_to_float(row['price']) == 0 else 'Paid',
                'price_usd': self.safe_convert_to_float(row['price']),
                'content_rating': str(row['contentAdvisoryRating']),
                'last_updated': str(row['currentVersionReleaseDate']),
                'genres': str(row['primaryGenreName']),
                'data_source': 'iTunes App Store',
                'developer': str(row['artistName']),
                'version': str(row['version']),
                'min_os_version': str(row['minimumOsVersion'])
            }
            ios_unified.append(unified_app)

        # Combine datasets
        all_unified = gp_unified + ios_unified
        self.unified_data = pd.DataFrame(all_unified)

        print(f"✅ Created unified dataset: {self.unified_data.shape}")
        return True

    def generate_market_insights(self):
        """Generate comprehensive market insights from unified data"""
        print("📊 Generating market insights...")

        insights = {
            'dataset_overview': {
                'total_apps': len(self.unified_data),
                'android_apps': (self.unified_data['platform'] == 'Android').sum(),
                'ios_apps': (self.unified_data['platform'] == 'iOS').sum(),
                'data_sources': {
                    'google_play': (self.unified_data['data_source'] == 'Google Play Store').sum(),
                    'itunes': (self.unified_data['data_source'] == 'iTunes App Store').sum()
                }
            },
            'platform_comparison': {
                'avg_rating_android': self.unified_data[self.unified_data['platform'] == 'Android']['rating'].mean(),
                'avg_rating_ios': self.unified_data[self.unified_data['platform'] == 'iOS']['rating'].mean(),
                'avg_reviews_android': self.unified_data[self.unified_data['platform'] == 'Android']['review_count'].mean(),
                'avg_reviews_ios': self.unified_data[self.unified_data['platform'] == 'iOS']['review_count'].mean(),
                'avg_size_android_mb': self.unified_data[self.unified_data['platform'] == 'Android']['size_mb'].mean(),
                'avg_size_ios_mb': self.unified_data[self.unified_data['platform'] == 'iOS']['size_mb'].mean()
            },
            'category_analysis': {
                'top_categories': self.unified_data['unified_category'].value_counts().head(10).to_dict(),
                'category_ratings': self.unified_data.groupby('unified_category')['rating'].mean().round(2).to_dict(),
                'category_distribution_android': self.unified_data[self.unified_data['platform'] == 'Android']['unified_category'].value_counts().head(10).to_dict(),
                'category_distribution_ios': self.unified_data[self.unified_data['platform'] == 'iOS']['unified_category'].value_counts().head(10).to_dict()
            },
            'pricing_analysis': {
                'free_vs_paid_total': self.unified_data['app_type'].value_counts().to_dict(),
                'free_vs_paid_android': self.unified_data[self.unified_data['platform'] == 'Android']['app_type'].value_counts().to_dict(),
                'free_vs_paid_ios': self.unified_data[self.unified_data['platform'] == 'iOS']['app_type'].value_counts().to_dict(),
                'avg_price_android': self.unified_data[self.unified_data['platform'] == 'Android']['price_usd'].mean(),
                'avg_price_ios': self.unified_data[self.unified_data['platform'] == 'iOS']['price_usd'].mean(),
                'expensive_apps': self.unified_data[self.unified_data['price_usd'] > 10].shape[0]
            },
            'quality_insights': {
                'high_rated_apps_4plus': (self.unified_data['rating'] >= 4.0).sum(),
                'high_rated_apps_45plus': (self.unified_data['rating'] >= 4.5).sum(),
                'apps_with_many_reviews': (self.unified_data['review_count'] >= 10000).sum(),
                'zero_rating_apps': (self.unified_data['rating'] == 0).sum()
            },
            'market_opportunities': {
                'underrated_categories': [],
                'high_competition_categories': [],
                'price_gaps': {}
            }
        }

        # Identify market opportunities
        category_stats = self.unified_data.groupby('unified_category').agg({
            'rating': 'mean',
            'app_name': 'count'
        }).round(2)

        # Categories with low average rating (opportunity for quality)
        insights['market_opportunities']['underrated_categories'] = category_stats[category_stats['rating'] < 3.5].index.tolist()

        # Categories with many apps (high competition)
        insights['market_opportunities']['high_competition_categories'] = category_stats[category_stats['app_name'] > 100].index.tolist()

        return insights

    def save_unified_data(self):
        """Save unified dataset and comprehensive insights"""
        # Ensure directories exist
        os.makedirs('data/processed', exist_ok=True)
        os.makedirs('reports', exist_ok=True)

        # Save unified dataset
        self.unified_data.to_csv('data/processed/unified_app_data_free_api.csv', index=False)
        print("✅ Saved unified dataset to: data/processed/unified_app_data_free_api.csv")

        # Save as JSON
        unified_json = self.unified_data.to_json(orient='records', date_format='iso')
        with open('data/processed/unified_app_data_free_api.json', 'w') as f:
            json.dump(json.loads(unified_json), f, indent=2)
        print("✅ Saved unified dataset to: data/processed/unified_app_data_free_api.json")

        # Generate and save insights
        insights = self.generate_market_insights()

        phase2_report = {
            "phase2_execution": {
                "timestamp": datetime.now().isoformat(),
                "google_play_apps": int((self.unified_data['platform'] == 'Android').sum()),
                "ios_apps": int((self.unified_data['platform'] == 'iOS').sum()),
                "total_unified_apps": len(self.unified_data),
                "apis_used": ["Google Play Store (processed dataset)", "iTunes Search API (FREE)"]
            },
            "api_integration_stats": self.api_stats,
            "data_schema": {
                "columns": list(self.unified_data.columns),
                "android_data_source": "Cleaned Google Play Store dataset from Phase 1",
                "ios_data_source": "iTunes Search API (FREE) - Real data from Apple",
                "category_mapping_applied": True
            },
            "market_insights": insights,
            "data_quality": {
                "missing_ratings": int(self.unified_data['rating'].isna().sum()),
                "missing_reviews": int(self.unified_data['review_count'].isna().sum()),
                "data_completeness_percent": round(((self.unified_data.notna().sum().sum() / (self.unified_data.shape[0] * self.unified_data.shape[1])) * 100), 2),
                "unique_apps": len(self.unified_data),
                "unique_categories": self.unified_data['unified_category'].nunique()
            }
        }

        with open('reports/phase2_free_api_integration_report.json', 'w') as f:
            json.dump(phase2_report, f, indent=2, default=str)
        print("✅ Saved integration report to: reports/phase2_free_api_integration_report.json")

        return insights

    def print_insights_summary(self, insights):
        """Print comprehensive market insights summary"""
        print("\n📈 COMPREHENSIVE MARKET INSIGHTS SUMMARY")
        print("=" * 60)

        # Dataset overview
        overview = insights['dataset_overview']
        print(f"\n📊 DATASET OVERVIEW:")
        print(f"  Total Apps: {overview['total_apps']:,}")
        print(f"  Android Apps: {overview['android_apps']:,}")
        print(f"  iOS Apps: {overview['ios_apps']:,}")

        # Platform comparison
        platform = insights['platform_comparison']
        print(f"\n🏪 PLATFORM COMPARISON:")
        print(f"  Average Rating - Android: {platform['avg_rating_android']:.2f}")
        print(f"  Average Rating - iOS: {platform['avg_rating_ios']:.2f}")
        print(f"  Average Reviews - Android: {platform['avg_reviews_android']:,.0f}")
        print(f"  Average Reviews - iOS: {platform['avg_reviews_ios']:,.0f}")
        print(f"  Average Size - Android: {platform['avg_size_android_mb']:.1f} MB")
        print(f"  Average Size - iOS: {platform['avg_size_ios_mb']:.1f} MB")

        # Top categories
        print(f"\n📱 TOP UNIFIED CATEGORIES:")
        for cat, count in list(insights['category_analysis']['top_categories'].items())[:5]:
            avg_rating = insights['category_analysis']['category_ratings'].get(cat, 0)
            print(f"  {cat}: {count:,} apps (avg rating: {avg_rating:.2f})")

        # Pricing insights
        pricing = insights['pricing_analysis']
        print(f"\n💰 PRICING ANALYSIS:")
        print(f"  Free Apps: {pricing['free_vs_paid_total'].get('Free', 0):,}")
        print(f"  Paid Apps: {pricing['free_vs_paid_total'].get('Paid', 0):,}")
        print(f"  Avg Price Android: ${pricing['avg_price_android']:.2f}")
        print(f"  Avg Price iOS: ${pricing['avg_price_ios']:.2f}")
        print(f"  Expensive Apps (>$10): {pricing['expensive_apps']:,}")

        # Quality metrics
        quality = insights['quality_insights']
        print(f"\n⭐ QUALITY INSIGHTS:")
        print(f"  High-rated Apps (4.0+): {quality['high_rated_apps_4plus']:,}")
        print(f"  Excellent Apps (4.5+): {quality['high_rated_apps_45plus']:,}")
        print(f"  Popular Apps (10K+ reviews): {quality['apps_with_many_reviews']:,}")

        # Market opportunities
        opportunities = insights['market_opportunities']
        print(f"\n🎯 MARKET OPPORTUNITIES:")
        if opportunities['underrated_categories']:
            print(f"  Underrated Categories: {', '.join(opportunities['underrated_categories'][:3])}")
        if opportunities['high_competition_categories']:
            print(f"  High Competition: {', '.join(opportunities['high_competition_categories'][:3])}")

    def execute_pipeline(self):
        """Execute complete FREE API integration pipeline"""
        print("🚀 AI-POWERED MARKET INTELLIGENCE - PHASE 2 (FREE APIs)")
        print("=" * 60)

        # Load Google Play data
        if not self.load_cleaned_googleplay_data():
            return False

        # Fetch iOS data using FREE iTunes Search API
        if not self.fetch_itunes_apps_free_api():
            return False

        # Create unified schema
        if not self.create_unified_schema():
            return False

        # Save results and generate insights
        insights = self.save_unified_data()

        # Print comprehensive summary
        self.print_insights_summary(insights)

        print("\n✅ PHASE 2 COMPLETE - FREE API Integration!")
        print("📂 Generated Files:")
        print("  - data/processed/unified_app_data_free_api.csv")
        print("  - data/processed/unified_app_data_free_api.json") 
        print("  - reports/phase2_free_api_integration_report.json")
        print("\n🔄 Ready for Phase 3: LLM Integration & AI Insights Generation")
        print("\n💡 APIS USED:")
        print("  ✅ Google Play Store: Processed dataset (Phase 1)")
        print("  ✅ iTunes Search API: FREE Apple API (Real iOS data)")

        return True

def main():
    """Main execution function"""
    pipeline = FreeAPIIntegrationPipeline()
    pipeline.execute_pipeline()

if __name__ == "__main__":
    main()
