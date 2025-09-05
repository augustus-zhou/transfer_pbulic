import requests
from bs4 import BeautifulSoup
import json
import csv
import pandas as pd
import time
import logging
import re
from urllib.parse import urljoin
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AllIndustriesScraper:
    def __init__(self):
        self.base_url = "https://ised-isde.canada.ca/app/ixb/cis"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # All endpoints to try
        self.endpoints = {
            'businesses': 'businesses-entreprises',
            'summary': 'summary-sommaire', 
            'performance': 'performance',
            'gdp': 'gdp-pid',
            'trade': 'trade-commerce'
        }
        
        # Complete list of all NAICS codes
        self.all_naics_codes = self.get_all_naics_codes()
        
    def get_all_naics_codes(self):
        """Return complete dictionary of all available NAICS codes"""
        return {
            # 2-digit sectors
            "11": "Agriculture, forestry, fishing and hunting",
            "21": "Mining, quarrying, and oil and gas extraction",
            "22": "Utilities", 
            "23": "Construction",
            "31-33": "Manufacturing",
            "41": "Wholesale trade",
            "44-45": "Retail trade",
            "48-49": "Transportation and warehousing",
            "51": "Information and cultural industries",
            "52": "Finance and insurance",
            "53": "Real estate and rental and leasing",
            "54": "Professional, scientific and technical services",
            "55": "Management of companies and enterprises",
            "56": "Administrative and support, waste management and remediation services",
            "61": "Educational services",
            "62": "Health care and social assistance",
            "71": "Arts, entertainment and recreation",
            "72": "Accommodation and food services",
            "81": "Other services (except public administration)",
            "91": "Public administration",
            
            # 3-digit subsectors - Agriculture
            "111": "Crop production",
            "112": "Animal production and aquaculture", 
            "113": "Forestry and logging",
            "114": "Fishing, hunting and trapping",
            "115": "Support activities for agriculture and forestry",
            
            # 3-digit subsectors - Mining
            "211": "Oil and gas extraction",
            "212": "Mining and quarrying (except oil and gas)",
            "213": "Support activities for mining, and oil and gas extraction",
            
            # 3-digit subsectors - Utilities
            "221": "Utilities",
            
            # 3-digit subsectors - Construction
            "236": "Construction of buildings",
            "237": "Heavy and civil engineering construction", 
            "238": "Specialty trade contractors",
            
            # 3-digit subsectors - Manufacturing
            "311": "Food manufacturing",
            "312": "Beverage and tobacco product manufacturing",
            "313": "Textile mills",
            "314": "Textile product mills",
            "315": "Clothing manufacturing",
            "316": "Leather and allied product manufacturing",
            "321": "Wood product manufacturing",
            "322": "Paper manufacturing",
            "323": "Printing and related support activities",
            "324": "Petroleum and coal product manufacturing",
            "325": "Chemical manufacturing",
            "326": "Plastics and rubber products manufacturing",
            "327": "Non-metallic mineral product manufacturing",
            "331": "Primary metal manufacturing",
            "332": "Fabricated metal product manufacturing",
            "333": "Machinery manufacturing",
            "334": "Computer and electronic product manufacturing",
            "335": "Electrical equipment, appliance and component manufacturing",
            "336": "Transportation equipment manufacturing",
            "337": "Furniture and related product manufacturing",
            "339": "Miscellaneous manufacturing",
            
            # 3-digit subsectors - Transportation
            "481": "Air transportation",
            "482": "Rail transportation", 
            "483": "Water transportation",
            "484": "Truck transportation",
            "485": "Transit and ground passenger transportation",
            "486": "Pipeline transportation",
            "487": "Scenic and sightseeing transportation",
            "488": "Support activities for transportation",
            "491": "Postal service",
            "492": "Couriers and messengers",
            "493": "Warehousing and storage",
            
            # 3-digit subsectors - Information
            "511": "Publishing industries",
            "512": "Motion picture and sound recording industries",
            "515": "Broadcasting (except Internet)",
            "517": "Telecommunications",
            "518": "Data processing, hosting, and related services",
            "519": "Other information services",
            
            # 3-digit subsectors - Finance
            "521": "Monetary authorities - central bank",
            "522": "Credit intermediation and related activities",
            "523": "Securities, commodity contracts, and other financial investment and related activities",
            "524": "Insurance carriers and related activities",
            "526": "Funds and other financial vehicles",
            
            # 3-digit subsectors - Real Estate
            "531": "Real estate",
            "532": "Rental and leasing services",
            "533": "Lessors of non-financial intangible assets",
            
            # 3-digit subsectors - Professional Services
            "541": "Professional, scientific and technical services",
            
            # 3-digit subsectors - Administrative
            "561": "Administrative and support services",
            
            # 3-digit subsectors - Health Care
            "621": "Ambulatory health care services",
            "622": "Hospitals",
            "623": "Nursing and residential care facilities",
            "624": "Social assistance",
            
            # 3-digit subsectors - Arts
            "711": "Performing arts, spectator sports and related industries",
            "712": "Heritage institutions",
            "713": "Amusement, gambling and recreation industries",
            
            # 3-digit subsectors - Accommodation
            "721": "Accommodation services",
            "722": "Food services and drinking places",
            
            # 3-digit subsectors - Other Services
            "811": "Repair and maintenance",
            "812": "Personal and laundry services",
            "813": "Religious, grant-making, civic, professional and similar organizations",
            "814": "Private households",
            
            # 3-digit subsectors - Public Administration
            "911": "Federal government public administration",
            "912": "Provincial and territorial public administration",
            "913": "Local, municipal and regional public administration",
            "914": "Aboriginal public administration",
            "919": "International and other extra-territorial public administration",
            
            # 4-digit industry groups - Key ones
            "2211": "Electric power generation, transmission and distribution",
            "2212": "Natural gas distribution",
            "2213": "Water, sewage and other systems",
            
            # 4-digit - Food Manufacturing
            "3111": "Animal food manufacturing",
            "3112": "Grain and oilseed milling", 
            "3113": "Sugar and confectionery product manufacturing",
            "3114": "Fruit and vegetable preserving and specialty food manufacturing",
            "3115": "Dairy product manufacturing",
            "3116": "Meat product manufacturing",
            "3117": "Seafood product preparation and packaging",
            "3118": "Bakeries and tortilla manufacturing",
            "3119": "Other food manufacturing",
            
            # 4-digit - Professional Services
            "5411": "Legal services",
            "5412": "Accounting, tax preparation, bookkeeping and payroll services",
            "5413": "Architectural, engineering and related services",
            "5414": "Specialized design services",
            "5415": "Computer systems design and related services",
            "5416": "Management, scientific and technical consulting services",
            "5417": "Scientific research and development services",
            "5418": "Advertising, public relations, and related services",
            "5419": "Other professional, scientific and technical services",
            
            # 5-digit industry - Sample
            "31111": "Animal food manufacturing"
        }
    
    def get_page(self, url):
        """Get webpage with error handling"""
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except requests.RequestException as e:
            logger.debug(f"Could not access {url}: {e}")
            return None
    
    def clean_number(self, text):
        """Clean and convert text to number"""
        if not text or text == '':
            return 0
        
        # Handle percentage signs
        if '%' in text:
            cleaned = re.sub(r'[^\d.-]', '', text)
            try:
                return float(cleaned)
            except ValueError:
                return 0
        
        # Handle currency and large numbers
        cleaned = re.sub(r'[^\d.-]', '', text)
        try:
            return float(cleaned) if '.' in cleaned else int(float(cleaned)) if cleaned else 0
        except ValueError:
            return 0
    
    def extract_table_data(self, table, endpoint_type=""):
        """Extract data from any table"""
        data = []
        
        # Get headers
        headers = []
        header_rows = table.find_all('tr')[:2]
        
        for header_row in header_rows:
            row_headers = []
            for th in header_row.find_all(['th', 'td']):
                header_text = th.get_text(strip=True)
                if header_text and header_text not in ['', ' ']:
                    row_headers.append(header_text)
            
            if row_headers and len(row_headers) > len(headers):
                headers = row_headers
                break
        
        if not headers:
            return data
        
        # Get data rows
        start_row = 1 if len(header_rows) == 1 else 2
        rows = table.find_all('tr')[start_row:]
        
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= len(headers):
                row_data = {'endpoint': endpoint_type}
                
                for i, cell in enumerate(cells[:len(headers)]):
                    cell_text = cell.get_text(strip=True)
                    header = headers[i]
                    
                    # Smart type detection
                    if self.is_numeric_column(header, cell_text):
                        row_data[header] = self.clean_number(cell_text)
                    else:
                        row_data[header] = cell_text
                
                # Only add meaningful data
                if self.has_meaningful_data(row_data, headers):
                    data.append(row_data)
        
        return data
    
    def is_numeric_column(self, header, text):
        """Determine if column should be numeric"""
        numeric_indicators = [
            'revenue', 'profit', 'gdp', 'export', 'import', 'balance', 'growth',
            'percentage', '%', 'million', 'employer', 'business', 'establishment',
            'micro', 'small', 'medium', 'large', 'total', 'average', 'quartile'
        ]
        
        header_lower = header.lower()
        text_lower = text.lower()
        
        if any(indicator in header_lower for indicator in numeric_indicators):
            return True
        
        if re.search(r'[\d$%,.]', text) and 'province' not in header_lower and 'territory' not in header_lower:
            return True
        
        return False
    
    def has_meaningful_data(self, row_data, headers):
        """Check if row has meaningful data"""
        meaningful_keys = [k for k in row_data.keys() if k != 'endpoint']
        
        if not meaningful_keys:
            return False
        
        first_column = headers[0] if headers else None
        if first_column and first_column in row_data:
            identifier = row_data[first_column]
            if identifier and identifier not in ['', 'Total', 'Canada', 'All']:
                return True
        
        return False
    
    def scrape_single_industry(self, naics_code, industry_name):
        """Scrape all available data for a single industry"""
        logger.info(f"🔍 Scraping {industry_name} ({naics_code})")
        
        industry_data = {
            'metadata': {
                'naics_code': naics_code,
                'industry_name': industry_name,
                'scrape_date': pd.Timestamp.now().isoformat()
            },
            'endpoints': {}
        }
        
        # Try all endpoints
        for endpoint_name, endpoint_path in self.endpoints.items():
            url = f"{self.base_url}/{endpoint_path}/{naics_code}"
            
            soup = self.get_page(url)
            if soup:
                # Extract all tables
                tables = soup.find_all('table')
                table_data = []
                
                for i, table in enumerate(tables):
                    extracted_data = self.extract_table_data(table, endpoint_name)
                    if extracted_data:
                        table_data.extend(extracted_data)
                
                if table_data:
                    industry_data['endpoints'][endpoint_name] = {
                        'url': url,
                        'tables_count': len(tables),
                        'data': table_data,
                        'records_count': len(table_data)
                    }
                    logger.info(f"  ✅ {endpoint_name}: {len(table_data)} records")
            
            time.sleep(0.5)  # Small delay between endpoints
        
        success = len(industry_data['endpoints']) > 0
        if success:
            total_records = sum(ep['records_count'] for ep in industry_data['endpoints'].values())
            logger.info(f"  📊 Total: {len(industry_data['endpoints'])} endpoints, {total_records} records")
        else:
            logger.info(f"  ❌ No data found")
            
        return industry_data if success else None
    
    def scrape_all_industries(self, batch_size=10, start_from=0):
        """Scrape all industries in batches with progress tracking"""
        all_codes = list(self.all_naics_codes.items())[start_from:]
        total_codes = len(all_codes)
        
        logger.info(f"🚀 Starting comprehensive scrape of {total_codes} industries")
        logger.info(f"📦 Processing in batches of {batch_size}")
        
        all_data = {}
        processed = 0
        successful = 0
        
        # Create progress file
        progress_file = "scraping_progress.json"
        
        for i in range(0, len(all_codes), batch_size):
            batch = all_codes[i:i+batch_size]
            
            logger.info(f"\n📦 BATCH {i//batch_size + 1}: Processing codes {processed + start_from + 1}-{min(processed + start_from + batch_size, len(self.all_naics_codes))}")
            
            for naics_code, industry_name in batch:
                try:
                    industry_data = self.scrape_single_industry(naics_code, industry_name)
                    
                    if industry_data:
                        all_data[naics_code] = industry_data
                        successful += 1
                    
                    processed += 1
                    
                    # Progress update
                    if processed % 5 == 0:
                        success_rate = (successful / processed) * 100
                        logger.info(f"📈 Progress: {processed}/{total_codes} ({success_rate:.1f}% success rate)")
                    
                    time.sleep(1)  # Respectful delay between industries
                    
                except Exception as e:
                    logger.error(f"❌ Error processing {naics_code}: {e}")
                    processed += 1
                    continue
            
            # Save progress after each batch
            self.save_progress(all_data, f"batch_{i//batch_size + 1}_data.json")
            
            # Save progress metadata
            progress = {
                'processed': processed + start_from,
                'successful': successful,
                'total': len(self.all_naics_codes),
                'last_batch': i//batch_size + 1,
                'success_rate': (successful / processed) * 100 if processed > 0 else 0
            }
            
            with open(progress_file, 'w') as f:
                json.dump(progress, f, indent=2)
            
            logger.info(f"💾 Batch {i//batch_size + 1} complete. Saved progress.")
            
            # Longer delay between batches
            if i + batch_size < len(all_codes):
                logger.info("⏳ Resting 10 seconds before next batch...")
                time.sleep(10)
        
        logger.info(f"\n🎉 COMPLETE: {successful}/{processed} industries successfully scraped")
        
        return all_data
    
    def save_progress(self, data, filename):
        """Save progress data"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    
    def save_all_data(self, data, base_filename="all_canadian_industries"):
        """Save comprehensive data with multiple formats"""
        logger.info(f"💾 Saving comprehensive dataset...")
        
        # Full JSON
        json_file = f"{base_filename}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        
        # Flatten for CSV
        flattened_data = []
        
        for naics_code, industry_data in data.items():
            metadata = industry_data['metadata']
            
            for endpoint_name, endpoint_info in industry_data['endpoints'].items():
                for record in endpoint_info['data']:
                    flat_record = {
                        'naics_code': metadata['naics_code'],
                        'industry_name': metadata['industry_name'],
                        'data_source': endpoint_name,
                        'scrape_date': metadata['scrape_date']
                    }
                    flat_record.update(record)
                    flattened_data.append(flat_record)
        
        # Save main CSV
        if flattened_data:
            csv_file = f"{base_filename}.csv"
            df = pd.DataFrame(flattened_data)
            df.to_csv(csv_file, index=False)
            
            logger.info(f"📊 Saved {len(flattened_data)} total records")
            logger.info(f"📁 Files: {json_file}, {csv_file}")
        
        # Create summary report
        self.create_final_report(data)
        
        return len(flattened_data) if flattened_data else 0
    
    def create_final_report(self, data):
        """Create final comprehensive report"""
        total_industries = len(data)
        total_records = sum(
            sum(ep['records_count'] for ep in industry['endpoints'].values())
            for industry in data.values()
        )
        
        # Count by data source
        endpoint_counts = {}
        for industry in data.values():
            for endpoint_name in industry['endpoints'].keys():
                endpoint_counts[endpoint_name] = endpoint_counts.get(endpoint_name, 0) + 1
        
        print("\n" + "="*100)
        print("🇨🇦 COMPLETE CANADIAN INDUSTRY STATISTICS EXTRACTION REPORT")
        print("="*100)
        
        print(f"📊 SUMMARY:")
        print(f"   🏭 Total Industries Scraped: {total_industries}")
        print(f"   📝 Total Records Extracted: {total_records:,}")
        print(f"   📅 Coverage: All available NAICS codes (2-digit to 5-digit)")
        
        print(f"\n📈 DATA SOURCES:")
        for endpoint, count in sorted(endpoint_counts.items()):
            print(f"   📋 {endpoint.title()}: {count} industries")
        
        print(f"\n🎯 TOP INDUSTRIES BY DATA VOLUME:")
        industry_record_counts = []
        for naics_code, industry in data.items():
            record_count = sum(ep['records_count'] for ep in industry['endpoints'].values())
            industry_record_counts.append((record_count, naics_code, industry['metadata']['industry_name']))
        
        # Show top 10
        for count, code, name in sorted(industry_record_counts, reverse=True)[:10]:
            print(f"   {code:>8} - {name[:60]:<60} ({count:,} records)")
        
        print("="*100)
        print("🎉 EXTRACTION COMPLETE - All available Canadian industry data captured!")
        print("="*100)

if __name__ == "__main__":
    scraper = AllIndustriesScraper()
    
    # Check if we should resume from previous progress
    if os.path.exists("scraping_progress.json"):
        with open("scraping_progress.json", 'r') as f:
            progress = json.load(f)
        
        resume = input(f"Resume from industry {progress['processed']}? (y/n): ").lower() == 'y'
        start_from = progress['processed'] if resume else 0
    else:
        start_from = 0
    
    print(f"🚀 Starting comprehensive scrape of ALL Canadian industries")
    print(f"📋 Total industries to process: {len(scraper.all_naics_codes)}")
    
    # Scrape all industries
    all_data = scraper.scrape_all_industries(batch_size=5, start_from=start_from)
    
    if all_data:
        # Save comprehensive dataset
        record_count = scraper.save_all_data(all_data)
        print(f"\n✅ SUCCESS: Scraped {len(all_data)} industries with {record_count:,} total records!")
    else:
        print("❌ No data was scraped.")