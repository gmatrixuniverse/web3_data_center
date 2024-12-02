import sys
import os
import asyncio
import json
from datetime import datetime, timedelta

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from web3_data_center.clients.twitter_monitor_client import TwitterMonitorClient

async def main():
    # Initialize the client
    client = TwitterMonitorClient()
    
    print("\n1. Initializing Twitter Monitor Client...")
    await client.initialize()
    
    # Add some crypto-related accounts to monitor
    # test_accounts = [
    #     "solana",           # Solana blockchain
    #     "BNBCHAIN",         # BNB Chain
    # ]
    
    # print("\n2. Adding accounts to monitor...")
    # for account in test_accounts:
    #     success = await client.add_account(account)
    #     print(f"Adding {account}: {'Success' if success else 'Failed'}")
    
    # # Get current list members
    # print("\n3. Current list members:")
    # try:
    #     members = await client.get_list_members()
    #     if isinstance(members, dict) and 'data' in members:
    #         # Parse the complex Twitter API response
    #         entries = members['data']['list']['members_timeline']['timeline']['instructions'][2]['entries']
    #         for entry in entries:
    #             if 'content' in entry and 'itemContent' in entry['content']:
    #                 user = entry['content']['itemContent']['user_results']['result']['legacy']
    #                 print(f"- @{user['screen_name']} ({user['name']})")
    #     elif isinstance(members, list):
    #         for member in members:
    #             print(f"- {member}")
    #     else:
    #         print("No members found or unexpected response format")
    # except Exception as e:
    #     print(f"Error getting list members: {str(e)}")
    
    # Monitor tweets for a short period
    print("\n4. Monitoring tweets for 60 seconds...")
    start_time = datetime.now()
    while (datetime.now() - start_time).seconds < 1000:
        try:
            tweets = await client.check_for_new_posts()
            if tweets:
                print("\nNew tweets found:")
                for tweet in tweets:
                    print(f"\nFrom: {tweet.get('author', 'Unknown')}")
                    print(f"Text: {tweet.get('text', 'No text')}")
                    print(f"Time: {tweet.get('created_at', 'No timestamp')}")
                    if 'entities' in tweet and 'urls' in tweet['entities']:
                        for url in tweet['entities']['urls']:
                            print(f"URL: {url.get('expanded_url', 'No URL')}")
                    print("-" * 50)
        except Exception as e:
            print(f"Error checking tweets: {str(e)}")
        await asyncio.sleep(10)  # Check every 10 seconds
    
    # Remove test accounts
    # print("\n5. Cleaning up - removing test accounts...")
    # for account in test_accounts:
    #     try:
    #         success = await client.remove_account(account)
    #         print(f"Removing {account}: {'Success' if success else 'Failed'}")
    #     except Exception as e:
    #         print(f"Error removing {account}: {str(e)}")
    
    # # Verify cleanup
    # print("\n6. Final list members:")
    # try:
    #     members = await client.get_list_members()
    #     if isinstance(members, dict) and 'data' in members:
    #         entries = members['data']['list']['members_timeline']['timeline']['instructions'][2]['entries']
    #         for entry in entries:
    #             if 'content' in entry and 'itemContent' in entry['content']:
    #                 user = entry['content']['itemContent']['user_results']['result']['legacy']
    #                 print(f"- @{user['screen_name']} ({user['name']})")
    #     elif isinstance(members, list):
    #         for member in members:
    #             print(f"- {member}")
    #     else:
    #         print("List is empty or unexpected response format")
    # except Exception as e:
    #     print(f"Error getting final list members: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
