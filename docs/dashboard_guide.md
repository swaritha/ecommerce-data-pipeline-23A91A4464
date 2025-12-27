text

\# 📊 Dashboard User Guide



\## \*\*Analytics Generated\*\* (11 CSV Reports)



data/processed/analytics/

├── query1\_top\_products.csv # Top 10 products by revenue

├── query2\_monthly\_trend.csv # Monthly revenue trend

├── query3\_customer\_segmentation.csv # RFM analysis (VIP/Loyal)

├── query4\_category\_performance.csv # Category revenue/profit

├── query5\_weekly\_patterns.csv # Day-of-week sales patterns

├── query6\_geographic\_analysis.csv # Top states by orders

├── query7\_payment\_methods.csv # Payment method distribution

├── query8\_profit\_margins.csv # Product margin analysis

├── query9\_customer\_lifetime\_value.csv # CLV segments

├── query10\_repeat\_purchase\_rate.csv # Customer retention metrics

└── query11\_pareto\_analysis.csv # 80/20 revenue concentration



text



\## \*\*How to Create BI Dashboard\*\* (Tableau/PowerBI)



\### \*\*Step 1: Import CSVs\*\*

Open Tableau Public / PowerBI Desktop



Connect → "Text Files" → Select all 11 CSVs from data/processed/analytics/



Tableau: "Multiple Tables" | PowerBI: "Get Data → Folder"



text



\### \*\*Step 2: Key Visualizations\*\* (Copy-Paste Ready)



TOP PRODUCTS BAR CHART



X: product\_name - Y: total\_revenue - Top 10 filter



MONTHLY TREND LINE CHART



X: month\_year - Y: total\_revenue - Trend line



CUSTOMER SEGMENT PIE CHART



Dimension: customer\_segment - Measure: total\_revenue



WEEKLY HEATMAP



X: day\_of\_week - Y: hour\_of\_day - Color: transaction\_count



GEOGRAPHY MAP



Latitude/Longitude: state\_coords - Color: total\_orders



text



\### \*\*Step 3: Export Instructions\*\*

PowerBI: File → Export → PDF/PowerPoint



text



\## \*\*Sample Insights to Highlight\*\*

Electronics dominates (45% revenue)



Weekends generate 28% sales uplift



Top 10% customers = 35% revenue (Pareto)



Premium products yield 25%+ margins



Top 5 states = 68% orders



text



\*\*Dashboard Files:\*\* Save as `dashboards/ecommerce\_analytics.twb` (Tableau) or `.pbix` (PowerBI)



\*\*Author:\*\* Sai Kiran Ramayanam | \*\*Roll:\*\* 23A91A4451

