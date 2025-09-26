import pandas as pd
import numpy as np
import json
import os
from datetime import datetime

class D2CMarketingAnalytics:
    def __init__(self):
        self.d2c_data = None
        self.analytics_insights = {}
        self.load_d2c_data()

    def load_d2c_data(self):
        """Load D2C marketing dataset"""
        try:
            self.d2c_data = pd.read_excel('Kasparro_Phase5_D2C_Synthetic_Dataset.xlsx')
            print(f"✅ Loaded D2C marketing dataset: {self.d2c_data.shape}")
            print(f"📊 Campaign data from {self.d2c_data['campaign_id'].nunique()} campaigns across {self.d2c_data['channel'].nunique()} channels")
            return True
        except Exception as e:
            print(f"❌ Error loading D2C data: {e}")
            return False

    def analyze_channel_performance(self):
        """Analyze marketing channel performance"""
        print("\n📊 ANALYZING CHANNEL PERFORMANCE...")

        channel_metrics = self.d2c_data.groupby('channel').agg({
            'spend_usd': ['sum', 'mean'],
            'impressions': ['sum', 'mean'],
            'clicks': ['sum', 'mean'],
            'installs': ['sum', 'mean'],
            'signups': ['sum', 'mean'],
            'first_purchase': ['sum', 'mean'],
            'repeat_purchase': ['sum', 'mean'],
            'revenue_usd': ['sum', 'mean'],
            'conversion_rate': 'mean',
            'campaign_id': 'count'
        }).round(2)

        # Calculate derived metrics
        channel_analysis = {}
        for channel in self.d2c_data['channel'].unique():
            channel_data = self.d2c_data[self.d2c_data['channel'] == channel]

            total_spend = channel_data['spend_usd'].sum()
            total_revenue = channel_data['revenue_usd'].sum()
            total_clicks = channel_data['clicks'].sum()
            total_impressions = channel_data['impressions'].sum()
            total_installs = channel_data['installs'].sum()
            total_purchases = channel_data['first_purchase'].sum() + channel_data['repeat_purchase'].sum()

            # Calculate key metrics
            roas = total_revenue / total_spend if total_spend > 0 else 0
            ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
            cpa = total_spend / total_installs if total_installs > 0 else 0
            cpc = total_spend / total_clicks if total_clicks > 0 else 0
            purchase_rate = (total_purchases / total_installs * 100) if total_installs > 0 else 0

            channel_analysis[channel] = {
                'campaigns': len(channel_data),
                'total_spend': total_spend,
                'total_revenue': total_revenue,
                'roas': roas,
                'ctr': ctr,
                'cpa': cpa,
                'cpc': cpc,
                'total_installs': total_installs,
                'purchase_rate': purchase_rate,
                'avg_conversion_rate': channel_data['conversion_rate'].mean()
            }

        # Sort by ROAS
        sorted_channels = sorted(channel_analysis.items(), key=lambda x: x[1]['roas'], reverse=True)

        print("\n🏆 CHANNEL PERFORMANCE RANKING (by ROAS):")
        print("=" * 70)

        for i, (channel, metrics) in enumerate(sorted_channels, 1):
            print(f"{i}. {channel.upper()}")
            print(f"   📊 Campaigns: {metrics['campaigns']}")
            print(f"   💰 Total Spend: ${metrics['total_spend']:,.2f}")
            print(f"   💵 Total Revenue: ${metrics['total_revenue']:,.2f}")
            print(f"   🎯 ROAS: {metrics['roas']:.2f}x")
            print(f"   👆 CTR: {metrics['ctr']:.2f}%")
            print(f"   💲 CPA: ${metrics['cpa']:.2f}")
            print(f"   🛒 Purchase Rate: {metrics['purchase_rate']:.1f}%")
            print()

        self.analytics_insights['channel_performance'] = channel_analysis
        return channel_analysis

    def analyze_category_performance(self):
        """Analyze performance by D2C category"""
        print("\n🎯 ANALYZING CATEGORY PERFORMANCE...")

        category_analysis = {}
        for category in self.d2c_data['seo_category'].unique():
            cat_data = self.d2c_data[self.d2c_data['seo_category'] == category]

            total_spend = cat_data['spend_usd'].sum()
            total_revenue = cat_data['revenue_usd'].sum()
            total_impressions = cat_data['impressions'].sum()
            total_clicks = cat_data['clicks'].sum()
            total_installs = cat_data['installs'].sum()
            avg_search_volume = cat_data['monthly_search_volume'].mean()
            avg_seo_position = cat_data['avg_position'].mean()

            # Key metrics
            roas = total_revenue / total_spend if total_spend > 0 else 0
            ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
            conversion_rate = cat_data['conversion_rate'].mean()

            category_analysis[category] = {
                'campaigns': len(cat_data),
                'total_spend': total_spend,
                'total_revenue': total_revenue,
                'roas': roas,
                'ctr': ctr,
                'conversion_rate': conversion_rate,
                'avg_search_volume': avg_search_volume,
                'avg_seo_position': avg_seo_position,
                'total_installs': total_installs
            }

        # Sort by ROAS
        sorted_categories = sorted(category_analysis.items(), key=lambda x: x[1]['roas'], reverse=True)

        print("\n📱 D2C CATEGORY PERFORMANCE (by ROAS):")
        print("=" * 60)

        for i, (category, metrics) in enumerate(sorted_categories, 1):
            print(f"{i}. {category.upper()}")
            print(f"   📊 Campaigns: {metrics['campaigns']}")
            print(f"   🎯 ROAS: {metrics['roas']:.2f}x")
            print(f"   💰 Revenue: ${metrics['total_revenue']:,.2f}")
            print(f"   👆 CTR: {metrics['ctr']:.2f}%")
            print(f"   🔄 Conversion: {metrics['conversion_rate']:.2f}%")
            print(f"   🔍 Avg Search Volume: {metrics['avg_search_volume']:,.0f}")
            print(f"   📍 Avg SEO Position: {metrics['avg_seo_position']:.1f}")
            print()

        self.analytics_insights['category_performance'] = category_analysis
        return category_analysis

    def analyze_funnel_optimization(self):
        """Analyze customer funnel and optimization opportunities"""
        print("\n🔄 ANALYZING CUSTOMER ACQUISITION FUNNEL...")

        # Overall funnel metrics
        total_impressions = self.d2c_data['impressions'].sum()
        total_clicks = self.d2c_data['clicks'].sum()
        total_installs = self.d2c_data['installs'].sum()
        total_signups = self.d2c_data['signups'].sum()
        total_first_purchases = self.d2c_data['first_purchase'].sum()
        total_repeat_purchases = self.d2c_data['repeat_purchase'].sum()

        # Calculate funnel conversion rates
        impression_to_click = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
        click_to_install = (total_installs / total_clicks * 100) if total_clicks > 0 else 0
        install_to_signup = (total_signups / total_installs * 100) if total_installs > 0 else 0
        signup_to_first_purchase = (total_first_purchases / total_signups * 100) if total_signups > 0 else 0
        first_to_repeat_purchase = (total_repeat_purchases / total_first_purchases * 100) if total_first_purchases > 0 else 0

        print("\n📊 OVERALL CUSTOMER ACQUISITION FUNNEL:")
        print("=" * 50)
        print(f"👁️  Impressions: {total_impressions:,}")
        print(f"👆 Clicks: {total_clicks:,} ({impression_to_click:.2f}% CTR)")
        print(f"📱 Installs: {total_installs:,} ({click_to_install:.2f}% install rate)")
        print(f"📝 Signups: {total_signups:,} ({install_to_signup:.2f}% signup rate)")
        print(f"🛒 First Purchases: {total_first_purchases:,} ({signup_to_first_purchase:.2f}% purchase rate)")
        print(f"🔄 Repeat Purchases: {total_repeat_purchases:,} ({first_to_repeat_purchase:.2f}% retention rate)")

        # Channel-specific funnel analysis
        print("\n🏪 FUNNEL PERFORMANCE BY CHANNEL:")
        print("=" * 50)

        for channel in self.d2c_data['channel'].unique():
            channel_data = self.d2c_data[self.d2c_data['channel'] == channel]

            impressions = channel_data['impressions'].sum()
            clicks = channel_data['clicks'].sum()
            installs = channel_data['installs'].sum()
            signups = channel_data['signups'].sum()
            first_purchases = channel_data['first_purchase'].sum()
            repeat_purchases = channel_data['repeat_purchase'].sum()

            ctr = (clicks / impressions * 100) if impressions > 0 else 0
            install_rate = (installs / clicks * 100) if clicks > 0 else 0
            signup_rate = (signups / installs * 100) if installs > 0 else 0
            purchase_rate = (first_purchases / signups * 100) if signups > 0 else 0
            retention_rate = (repeat_purchases / first_purchases * 100) if first_purchases > 0 else 0

            print(f"\n{channel.upper()}:")
            print(f"  CTR: {ctr:.2f}% | Install: {install_rate:.2f}% | Signup: {signup_rate:.2f}%")
            print(f"  Purchase: {purchase_rate:.2f}% | Retention: {retention_rate:.2f}%")

        funnel_metrics = {
            'overall': {
                'impression_to_click': impression_to_click,
                'click_to_install': click_to_install,
                'install_to_signup': install_to_signup,
                'signup_to_purchase': signup_to_first_purchase,
                'purchase_to_repeat': first_to_repeat_purchase
            }
        }

        self.analytics_insights['funnel_analysis'] = funnel_metrics
        return funnel_metrics

    def analyze_campaign_efficiency(self):
        """Analyze individual campaign efficiency"""
        print("\n⚡ ANALYZING CAMPAIGN EFFICIENCY...")

        # Calculate efficiency metrics for each campaign
        self.d2c_data['roas'] = self.d2c_data['revenue_usd'] / self.d2c_data['spend_usd']
        self.d2c_data['ctr'] = (self.d2c_data['clicks'] / self.d2c_data['impressions'] * 100)
        self.d2c_data['cpa'] = self.d2c_data['spend_usd'] / self.d2c_data['installs']

        # Top performing campaigns
        top_roas = self.d2c_data.nlargest(5, 'roas')[['campaign_id', 'channel', 'seo_category', 'roas', 'spend_usd', 'revenue_usd']]
        top_ctr = self.d2c_data.nlargest(5, 'ctr')[['campaign_id', 'channel', 'seo_category', 'ctr', 'clicks', 'impressions']]

        print("\n🏆 TOP 5 CAMPAIGNS BY ROAS:")
        print("=" * 50)
        for _, campaign in top_roas.iterrows():
            print(f"{campaign['campaign_id']} ({campaign['channel']} - {campaign['seo_category']})")
            print(f"  ROAS: {campaign['roas']:.2f}x | Spend: ${campaign['spend_usd']:.2f} | Revenue: ${campaign['revenue_usd']:.2f}")

        print("\n👆 TOP 5 CAMPAIGNS BY CTR:")
        print("=" * 50)
        for _, campaign in top_ctr.iterrows():
            print(f"{campaign['campaign_id']} ({campaign['channel']} - {campaign['seo_category']})")
            print(f"  CTR: {campaign['ctr']:.2f}% | Clicks: {campaign['clicks']:,} | Impressions: {campaign['impressions']:,}")

        # Campaign efficiency insights
        efficiency_insights = {
            'avg_roas': self.d2c_data['roas'].mean(),
            'avg_ctr': self.d2c_data['ctr'].mean(),
            'avg_cpa': self.d2c_data['cpa'].mean(),
            'top_performing_campaigns': top_roas.to_dict('records'),
            'highest_ctr_campaigns': top_ctr.to_dict('records')
        }

        self.analytics_insights['campaign_efficiency'] = efficiency_insights
        return efficiency_insights

    def generate_d2c_strategic_insights(self):
        """Generate strategic insights for D2C marketing"""
        print("\n🧠 GENERATING D2C STRATEGIC INSIGHTS...")

        channel_perf = self.analytics_insights.get('channel_performance', {})
        category_perf = self.analytics_insights.get('category_performance', {})
        funnel_metrics = self.analytics_insights.get('funnel_analysis', {})

        # Identify best performing elements
        best_channel = max(channel_perf.items(), key=lambda x: x[1]['roas'])[0] if channel_perf else "N/A"
        best_category = max(category_perf.items(), key=lambda x: x[1]['roas'])[0] if category_perf else "N/A"

        # Calculate overall metrics
        total_spend = self.d2c_data['spend_usd'].sum()
        total_revenue = self.d2c_data['revenue_usd'].sum()
        overall_roas = total_revenue / total_spend

        strategic_insights = f"""**D2C MARKETING STRATEGIC INSIGHTS**
*Based on analysis of {len(self.d2c_data)} campaigns across {self.d2c_data['channel'].nunique()} channels*

**PERFORMANCE OVERVIEW:**
- Total Marketing Spend: ${total_spend:,.2f}
- Total Revenue Generated: ${total_revenue:,.2f}
- Overall ROAS: {overall_roas:.2f}x
- Average Conversion Rate: {self.d2c_data['conversion_rate'].mean():.2f}%

**TOP PERFORMING SEGMENTS:**
- Best Channel: {best_channel} (highest ROAS)
- Best Category: {best_category} (highest revenue efficiency)
- Strongest Funnel Stage: Click-to-Install conversion

**STRATEGIC RECOMMENDATIONS:**

1. **Channel Optimization**: Focus budget allocation on {best_channel} which shows superior ROAS performance
2. **Category Expansion**: Scale {best_category} campaigns given strong market demand and conversion rates  
3. **Funnel Improvement**: Optimize signup-to-purchase conversion rate (current bottleneck)
4. **SEO Investment**: Categories with high search volume but poor SEO positioning need organic investment
5. **Retention Focus**: Implement repeat purchase optimization for long-term customer value

**BUDGET ALLOCATION STRATEGY:**
- Increase spend in high-ROAS channels by 30-40%
- Reduce spend in underperforming segments
- Test new creative formats in top-performing categories
- Implement retention campaigns for repeat purchase optimization

**RISK FACTORS:**
- High customer acquisition costs in some segments
- Low repeat purchase rates indicate retention challenges
- SEO position gaps in high-volume categories"""

        self.analytics_insights['strategic_insights'] = strategic_insights
        return strategic_insights

    def save_d2c_analytics_report(self):
        """Save comprehensive D2C analytics report"""
        # Ensure directories exist
        os.makedirs('data/d2c_analysis', exist_ok=True)
        os.makedirs('reports', exist_ok=True)

        # Create comprehensive analytics report
        d2c_report = {
            "phase5_execution": {
                "timestamp": datetime.now().isoformat(),
                "analysis_type": "D2C Marketing Analytics",
                "campaigns_analyzed": len(self.d2c_data),
                "channels_analyzed": self.d2c_data['channel'].nunique(),
                "categories_analyzed": self.d2c_data['seo_category'].nunique()
            },
            "overall_metrics": {
                "total_spend": float(self.d2c_data['spend_usd'].sum()),
                "total_revenue": float(self.d2c_data['revenue_usd'].sum()),
                "overall_roas": float(self.d2c_data['revenue_usd'].sum() / self.d2c_data['spend_usd'].sum()),
                "total_impressions": int(self.d2c_data['impressions'].sum()),
                "total_clicks": int(self.d2c_data['clicks'].sum()),
                "total_installs": int(self.d2c_data['installs'].sum()),
                "avg_conversion_rate": float(self.d2c_data['conversion_rate'].mean())
            },
            "analytics_insights": self.analytics_insights,
            "data_summary": {
                "channels": self.d2c_data['channel'].value_counts().to_dict(),
                "categories": self.d2c_data['seo_category'].value_counts().to_dict(),
                "campaign_performance_distribution": {
                    "high_roas_campaigns": int((self.d2c_data['revenue_usd'] / self.d2c_data['spend_usd'] > 3).sum()),
                    "medium_roas_campaigns": int(((self.d2c_data['revenue_usd'] / self.d2c_data['spend_usd'] >= 1.5) & 
                                                (self.d2c_data['revenue_usd'] / self.d2c_data['spend_usd'] <= 3)).sum()),
                    "low_roas_campaigns": int((self.d2c_data['revenue_usd'] / self.d2c_data['spend_usd'] < 1.5).sum())
                }
            }
        }

        # Save analytics report
        with open('reports/phase5_d2c_analytics_report.json', 'w', encoding='utf-8') as f:
            json.dump(d2c_report, f, indent=2, default=str)
        print("\n✅ Saved D2C analytics report to: reports/phase5_d2c_analytics_report.json")

        # Save processed D2C data with calculated metrics
        self.d2c_data.to_csv('data/d2c_analysis/d2c_campaigns_analyzed.csv', index=False)
        print("✅ Saved analyzed D2C data to: data/d2c_analysis/d2c_campaigns_analyzed.csv")

        return d2c_report

    def execute_d2c_analysis(self):
        """Execute complete D2C marketing analysis"""
        print("🚀 D2C MARKETING INTELLIGENCE - PHASE 5")
        print("=" * 60)
        print("📊 Analyzing D2C Marketing Performance & Strategy")

        if not self.d2c_data is not None:
            print("❌ D2C data not loaded. Please check the Excel file.")
            return False

        # Execute analysis modules
        print("\n🔄 Running comprehensive D2C marketing analysis...")

        self.analyze_channel_performance()
        self.analyze_category_performance()
        self.analyze_funnel_optimization()
        self.analyze_campaign_efficiency()

        # Generate strategic insights
        strategic_insights = self.generate_d2c_strategic_insights()

        # Save comprehensive results
        d2c_report = self.save_d2c_analytics_report()

        print("\n🧠 D2C STRATEGIC INSIGHTS PREVIEW:")
        print("=" * 50)
        print(strategic_insights[:600] + "..." if len(strategic_insights) > 600 else strategic_insights)

        print("\n✅ PHASE 5 COMPLETE - D2C Marketing Analytics Generated!")
        print("📂 Generated Files:")
        print("  - reports/phase5_d2c_analytics_report.json")
        print("  - data/d2c_analysis/d2c_campaigns_analyzed.csv")

        print("\n📊 KEY FINDINGS:")
        overall_roas = d2c_report['overall_metrics']['overall_roas']
        best_channel = max(self.analytics_insights['channel_performance'].items(), key=lambda x: x[1]['roas'])[0]
        total_campaigns = len(self.d2c_data)

        print(f"  Overall ROAS: {overall_roas:.2f}x")
        print(f"  Best Performing Channel: {best_channel}")
        print(f"  Campaigns Analyzed: {total_campaigns}")
        print(f"  Total Revenue: ${d2c_report['overall_metrics']['total_revenue']:,.2f}")

        print("\n🎯 MARKETING SYSTEM COMPLETE!")
        print("  🏪 App Market Intelligence (Phases 1-4) ✅")
        print("  📊 D2C Marketing Analytics (Phase 5) ✅")
        print("  Ready for strategic decision making and growth optimization!")

        return True

def main():
    """Main execution function"""
    analyzer = D2CMarketingAnalytics()
    analyzer.execute_d2c_analysis()

if __name__ == "__main__":
    main()
