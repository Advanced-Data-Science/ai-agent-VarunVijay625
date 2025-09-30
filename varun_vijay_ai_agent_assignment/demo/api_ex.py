"""
Part 2: API Fundamentals Exercises - Complete Solution
File: demo/api_exercises.py

Run this to complete Part 2 of the assignment.
"""

import requests
import json
import time
from datetime import datetime
import os

# Create data directory
os.makedirs('data/raw', exist_ok=True)

print("="*70)
print("PART 2: API FUNDAMENTALS EXERCISES")
print("="*70)

# ============================================================================
# EXERCISE 2.2: Your First API Call - Get 5 Cat Facts
# ============================================================================

def exercise_2_2():
    print("\n" + "="*70)
    print("EXERCISE 2.2: Getting 5 Cat Facts")
    print("="*70)
    
    url = "https://catfact.ninja/fact"
    facts = []
    
    for i in range(5):
        try:
            print(f"\nRequest {i+1}/5...")
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            fact = data['fact']
            facts.append({
                'fact_number': i+1,
                'fact': fact,
                'collected_at': datetime.now().isoformat()
            })
            
            print(f"SUCCESS: {fact[:60]}...")
            time.sleep(1)  # Respectful delay
            
        except requests.exceptions.RequestException as e:
            print(f"ERROR on request {i+1}: {e}")
            facts.append({
                'fact_number': i+1,
                'fact': None,
                'error': str(e),
                'collected_at': datetime.now().isoformat()
            })
    
    # Save to JSON file
    output = {
        'exercise': '2.2 - Cat Facts',
        'total_collected': len([f for f in facts if f['fact']]),
        'collection_time': datetime.now().isoformat(),
        'facts': facts
    }
    
    with open('data/raw/cat_facts.json', 'w') as f:
        json.dump(output, f, indent=2)
    
    print(f"\nSaved {len([f for f in facts if f['fact']])} facts to data/raw/cat_facts.json")
    return facts


# ============================================================================
# EXERCISE 2.3: API with Parameters - Compare Holidays
# ============================================================================

def exercise_2_3():
    print("\n" + "="*70)
    print("EXERCISE 2.3: Public Holidays API with Parameters")
    print("="*70)
    
    countries = ['US', 'CA', 'MX']  # USA, Canada, Mexico
    year = 2025
    all_data = {}
    
    for country in countries:
        try:
            url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/{country}"
            print(f"\nGetting holidays for {country}...")
            print(f"URL: {url}")
            
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            holidays = response.json()
            
            # Extract just names and dates
            holiday_list = [
                {
                    'name': h['name'],
                    'date': h['date'],
                    'local_name': h.get('localName', h['name'])
                }
                for h in holidays
            ]
            
            all_data[country] = {
                'country_code': country,
                'year': year,
                'holiday_count': len(holidays),
                'holidays': holiday_list
            }
            
            print(f"SUCCESS: Found {len(holidays)} holidays")
            print(f"First 3: {[h['name'] for h in holidays[:3]]}")
            
            time.sleep(0.5)  # Respectful delay
            
        except requests.exceptions.RequestException as e:
            print(f"ERROR for {country}: {e}")
            all_data[country] = {
                'country_code': country,
                'error': str(e)
            }
    
    # Create comparison summary
    print("\n" + "-"*70)
    print("HOLIDAY COMPARISON SUMMARY")
    print("-"*70)
    
    comparison = []
    for country, data in all_data.items():
        if 'holiday_count' in data:
            count = data['holiday_count']
            comparison.append({'country': country, 'holidays': count})
            print(f"{country}: {count} public holidays in {year}")
    
    # Save results
    output = {
        'exercise': '2.3 - Holiday Comparison',
        'year': year,
        'collection_time': datetime.now().isoformat(),
        'countries': all_data,
        'summary': comparison
    }
    
    with open('data/raw/holidays_comparison.json', 'w') as f:
        json.dump(output, f, indent=2)
    
    print(f"\nSaved to data/raw/holidays_comparison.json")
    return all_data


# ============================================================================
# BONUS: Census API Preview (What you'll use for your project!)
# ============================================================================

def bonus_census_preview():
    print("\n" + "="*70)
    print("BONUS: Census API Preview")
    print("="*70)
    
    try:
        # Get state population data (no API key needed for this endpoint)
        url = "https://api.census.gov/data/2019/pep/population"
        params = {
            'get': 'NAME,POP',
            'for': 'state:*'
        }
        
        print("\nFetching state population from Census API...")
        print(f"URL: {url}")
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        headers = data[0]
        states = data[1:6]  # First 5 states
        
        print("\nSUCCESS! Sample data:")
        print(f"\n{headers[0]:30} {headers[1]:>15}")
        print("-"*50)
        for state in states:
            print(f"{state[0]:30} {int(state[1]):>15,}")

        
    except Exception as e:
        print(f"ERROR: {e}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    print("\n" + "🚀"*35)
    print("STARTING API FUNDAMENTALS EXERCISES")
    print("🚀"*35)
    
    # Exercise 2.2
    cat_facts = exercise_2_2()
    
    # Exercise 2.3
    holidays = exercise_2_3()
    
    # Bonus
    bonus_census_preview()
    