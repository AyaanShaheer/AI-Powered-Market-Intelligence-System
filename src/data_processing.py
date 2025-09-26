import pandas as pd
import numpy as np
import json
import re
from datetime import datetime
import os

# Phase 1: Data Ingestion & Cleaning Pipeline

class DataIngestionPipeline:
    def __init__(self):
        self.raw_data = None
        self.cleaned_data = None
        self.cleaning_stats = {}

    def load_googleplay_data(self, file_path='googleplaystore.csv'):
        """Load Google Play Store dataset"""
        try:
            self.raw_data = pd.read_csv(file_path)
            print(f"✅ Loaded Google Play Store dataset: {self.raw_data.shape}")
            return True
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return False

    def clean_reviews_column(self, df):
        """Clean and convert Reviews column to numeric"""
        original_count = df.shape[0]

        # Convert 'M' and 'k' suffixes to numeric values
        def convert_reviews(review_str):
            if pd.isna(review_str) or review_str == 'NaN':
                return 0

            review_str = str(review_str).strip()

            if review_str == '0' or review_str == '':
                return 0

            # Handle 'M' (millions) and 'k' (thousands) suffixes
            if 'M' in review_str:
                return float(review_str.replace('M', '').replace(',', '')) * 1000000
            elif 'k' in review_str:
                return float(review_str.replace('k', '').replace(',', '')) * 1000
            else:
                # Remove commas and convert to float
                try:
                    return float(review_str.replace(',', ''))
                except:
                    return 0

        df['Reviews_Numeric'] = df['Reviews'].apply(convert_reviews)
        self.cleaning_stats['reviews_converted'] = (df['Reviews_Numeric'] > 0).sum()

        return df

    def clean_size_column(self, df):
        """Clean and convert Size column to numeric (MB)"""
        def convert_size(size_str):
            if pd.isna(size_str) or size_str == 'Varies with device':
                return np.nan

            size_str = str(size_str).strip()

            if 'M' in size_str:
                return float(size_str.replace('M', '').replace(',', ''))
            elif 'k' in size_str:
                return float(size_str.replace('k', '').replace(',', '')) / 1024  # Convert to MB
            else:
                try:
                    return float(size_str)
                except:
                    return np.nan

        df['Size_MB'] = df['Size'].apply(convert_size)
        self.cleaning_stats['size_converted'] = df['Size_MB'].notna().sum()

        return df

    def clean_installs_column(self, df):
        """Clean and convert Installs column to numeric"""
        def convert_installs(install_str):
            if pd.isna(install_str):
                return 0

            install_str = str(install_str).strip()

            # Remove '+' and ',' and convert to numeric
            install_str = install_str.replace('+', '').replace(',', '')

            try:
                return int(install_str)
            except:
                return 0

        df['Installs_Numeric'] = df['Installs'].apply(convert_installs)
        self.cleaning_stats['installs_converted'] = (df['Installs_Numeric'] > 0).sum()

        return df

    def clean_price_column(self, df):
        """Clean and convert Price column to numeric"""
        def convert_price(price_str):
            if pd.isna(price_str):
                return 0.0

            price_str = str(price_str).strip()

            if price_str == '0' or price_str == 'Free':
                return 0.0

            # Remove '$' symbol and convert to float
            price_str = price_str.replace('$', '')

            try:
                return float(price_str)
            except:
                return 0.0

        df['Price_USD'] = df['Price'].apply(convert_price)
        self.cleaning_stats['price_converted'] = df['Price_USD'].notna().sum()

        return df

    def clean_date_column(self, df):
        """Clean and standardize Last Updated column"""
        def convert_date(date_str):
            if pd.isna(date_str):
                return None

            try:
                # Try to parse the date
                return pd.to_datetime(date_str)
            except:
                return None

        df['Last_Updated_Date'] = df['Last Updated'].apply(convert_date)
        self.cleaning_stats['dates_converted'] = df['Last_Updated_Date'].notna().sum()

        return df

    def remove_duplicates(self, df):
        """Remove duplicate apps keeping the most recent one"""
        initial_count = df.shape[0]

        # Sort by Last_Updated_Date to keep most recent entries
        df_sorted = df.sort_values('Last_Updated_Date', ascending=False, na_position='last')

        # Remove duplicates based on App name
        df_deduplicated = df_sorted.drop_duplicates(subset=['App'], keep='first')

        final_count = df_deduplicated.shape[0]
        self.cleaning_stats['duplicates_removed'] = initial_count - final_count

        return df_deduplicated

    def validate_data_quality(self, df):
        """Validate data quality and create quality metrics"""
        quality_metrics = {
            'total_apps': df.shape[0],
            'missing_ratings': df['Rating'].isna().sum(),
            'missing_ratings_pct': (df['Rating'].isna().sum() / df.shape[0]) * 100,
            'free_apps': (df['Type'] == 'Free').sum(),
            'paid_apps': (df['Type'] == 'Paid').sum(),
            'categories': df['Category'].nunique(),
            'avg_rating': df['Rating'].mean(),
            'avg_reviews': df['Reviews_Numeric'].mean(),
            'avg_size_mb': df['Size_MB'].mean(),
            'rating_distribution': df['Rating'].value_counts().to_dict()
        }

        self.cleaning_stats['quality_metrics'] = quality_metrics
        return quality_metrics

    def create_unified_schema(self, df):
        """Create unified schema with standardized columns"""
        unified_columns = {
            'app_name': df['App'],
            'category': df['Category'],
            'rating': df['Rating'],
            'review_count': df['Reviews_Numeric'],
            'size_mb': df['Size_MB'],
            'installs': df['Installs_Numeric'],
            'app_type': df['Type'],
            'price_usd': df['Price_USD'],
            'content_rating': df['Content Rating'],
            'genres': df['Genres'],
            'last_updated': df['Last_Updated_Date'],
            'current_version': df['Current Ver'],
            'android_version': df['Android Ver']
        }

        unified_df = pd.DataFrame(unified_columns)
        return unified_df

    def convert_numpy_types(self, obj):
        """Convert numpy types to native Python types for JSON serialization"""
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, dict):
            return {key: self.convert_numpy_types(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_numpy_types(item) for item in obj]
        return obj

    def execute_pipeline(self):
        """Execute complete data cleaning pipeline"""
        if self.raw_data is None:
            print("❌ No raw data loaded. Call load_googleplay_data() first.")
            return None

        print("🔄 Starting data cleaning pipeline...")

        # Start with raw data copy
        df = self.raw_data.copy()

        # Execute cleaning steps
        print("  📊 Cleaning Reviews column...")
        df = self.clean_reviews_column(df)

        print("  📏 Cleaning Size column...")
        df = self.clean_size_column(df)

        print("  📱 Cleaning Installs column...")
        df = self.clean_installs_column(df)

        print("  💰 Cleaning Price column...")
        df = self.clean_price_column(df)

        print("  📅 Cleaning Date column...")
        df = self.clean_date_column(df)

        print("  🗑️ Removing duplicates...")
        df = self.remove_duplicates(df)

        print("  ✅ Validating data quality...")
        quality_metrics = self.validate_data_quality(df)

        print("  🏗️ Creating unified schema...")
        self.cleaned_data = self.create_unified_schema(df)

        print("✅ Data cleaning pipeline completed!")
        print(f"  📈 Final dataset shape: {self.cleaned_data.shape}")

        return self.cleaned_data

    def save_outputs(self):
        """Save all pipeline outputs"""
        # Create directories
        os.makedirs('data/raw', exist_ok=True)
        os.makedirs('data/processed', exist_ok=True)
        os.makedirs('data/output', exist_ok=True)
        os.makedirs('reports', exist_ok=True)

        # Save cleaned dataset
        self.cleaned_data.to_csv('data/processed/googleplay_cleaned.csv', index=False)
        print("✅ Saved cleaned dataset to: data/processed/googleplay_cleaned.csv")

        # Save as JSON
        cleaned_json = self.cleaned_data.to_json(orient='records', date_format='iso')
        with open('data/processed/googleplay_cleaned.json', 'w') as f:
            json.dump(json.loads(cleaned_json), f, indent=2)
        print("✅ Saved cleaned dataset to: data/processed/googleplay_cleaned.json")

        # Create validation report
        quality_metrics_converted = self.convert_numpy_types(self.cleaning_stats.get('quality_metrics', {}))

        validation_report = {
            "pipeline_execution": {
                "timestamp": datetime.now().isoformat(),
                "input_file": "googleplaystore.csv",
                "output_files": [
                    "data/processed/googleplay_cleaned.csv",
                    "data/processed/googleplay_cleaned.json"
                ]
            },
            "data_transformation": {
                "original_shape": [self.raw_data.shape[0], self.raw_data.shape[1]],
                "final_shape": list(self.cleaned_data.shape),
                "duplicates_removed": int(self.cleaning_stats.get('duplicates_removed', 0)),
                "columns_transformed": {
                    "reviews": "Reviews → review_count (handled M/k suffixes)",
                    "size": "Size → size_mb (normalized to MB)",
                    "installs": "Installs → installs (removed + and , symbols)",
                    "price": "Price → price_usd (removed $ symbol)",
                    "date": "Last Updated → last_updated (datetime format)"
                }
            },
            "data_quality": quality_metrics_converted,
            "schema": {
                "columns": list(self.cleaned_data.columns),
                "data_types": {col: str(dtype) for col, dtype in self.cleaned_data.dtypes.items()}
            },
            "conversion_stats": {
                "reviews_converted": int(self.cleaning_stats.get('reviews_converted', 0)),
                "sizes_converted": int(self.cleaning_stats.get('size_converted', 0)),
                "installs_converted": int(self.cleaning_stats.get('installs_converted', 0)),
                "prices_converted": int(self.cleaning_stats.get('price_converted', 0)),
                "dates_converted": int(self.cleaning_stats.get('dates_converted', 0))
            }
        }

        # Save validation report
        with open('reports/data_validation_report.json', 'w') as f:
            json.dump(validation_report, f, indent=2)
        print("✅ Saved validation report to: reports/data_validation_report.json")

    def print_summary(self):
        """Print pipeline execution summary"""
        cleaning_stats = self.cleaning_stats

        print("\n📊 CLEANING SUMMARY:")
        print(f"  Reviews converted: {cleaning_stats.get('reviews_converted', 0):,}")
        print(f"  Sizes converted: {cleaning_stats.get('size_converted', 0):,}")
        print(f"  Installs converted: {cleaning_stats.get('installs_converted', 0):,}")
        print(f"  Prices converted: {cleaning_stats.get('price_converted', 0):,}")
        print(f"  Dates converted: {cleaning_stats.get('dates_converted', 0):,}")
        print(f"  Duplicates removed: {cleaning_stats.get('duplicates_removed', 0):,}")

        if 'quality_metrics' in cleaning_stats:
            qm = cleaning_stats['quality_metrics']
            print(f"\n📈 QUALITY METRICS:")
            print(f"  Total apps: {qm['total_apps']:,}")
            print(f"  Missing ratings: {qm['missing_ratings']:,} ({qm['missing_ratings_pct']:.1f}%)")
            print(f"  Free apps: {qm['free_apps']:,}")
            print(f"  Paid apps: {qm['paid_apps']:,}")
            print(f"  Categories: {qm['categories']}")
            print(f"  Average rating: {qm['avg_rating']:.2f}")
            print(f"  Average reviews: {qm['avg_reviews']:,.0f}")
            print(f"  Average size: {qm['avg_size_mb']:.1f} MB")

def main():
    """Main execution function"""
    print("🚀 AI-POWERED MARKET INTELLIGENCE - PHASE 1")
    print("=" * 50)

    # Initialize pipeline
    pipeline = DataIngestionPipeline()

    # Load and process data
    if pipeline.load_googleplay_data('googleplaystore.csv'):
        # Execute cleaning pipeline
        cleaned_data = pipeline.execute_pipeline()

        if cleaned_data is not None:
            # Save outputs
            pipeline.save_outputs()

            # Print summary
            pipeline.print_summary()

            print("\n✅ PHASE 1 COMPLETE!")
            print("📂 Check the following folders for outputs:")
            print("  - data/processed/ - Cleaned datasets")
            print("  - reports/ - Validation reports")
            print("\n🔄 Ready for Phase 2: API Integration")
        else:
            print("❌ Pipeline execution failed!")
    else:
        print("❌ Failed to load data!")

if __name__ == "__main__":
    main()
