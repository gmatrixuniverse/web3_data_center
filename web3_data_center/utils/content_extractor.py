from dataclasses import dataclass, field
import re
import aiohttp
from typing import Dict, List, Any, Set
from bs4 import BeautifulSoup
from enum import Enum, auto
from urllib.parse import urljoin, urlparse, parse_qs
import ssl
from web3_data_center.models.source import Source, SourceType
import pytesseract
from PIL import Image
import io
import asyncio



@dataclass
class ExtractedData:
    contracts: Set[str] = field(default_factory=set)
    tickers: Set[str] = field(default_factory=set)
    urls: Set[str] = field(default_factory=set)
    handles: Set[str] = field(default_factory=set)
    tweets: Set[str] = field(default_factory=set)

class ContentExtractor:
    def __init__(self, http_client: aiohttp.ClientSession, use_proxy=False):
        from web3_data_center.clients.twitter_monitor_client import TwitterMonitorClient
        self.twitter_client = TwitterMonitorClient(use_proxy=use_proxy)
        self.http_client = http_client
        self.contract_pattern = re.compile(r'\b(?:0x[a-fA-F0-9]{40}|[1-9A-HJ-NP-Za-km-z]{32,44})\b')
        self.ticker_pattern = re.compile(r'\$[A-Za-z]{2,}')
        self.url_pattern = re.compile(r'https?://\S+')
        self.handle_pattern = re.compile(r'@\w+')

    async def extract_from_source(self, source: Source, depth: int = 2) -> ExtractedData:
        if depth <= 0:
            return ExtractedData()

        if source.type == SourceType.TWEET:
            return await self._extract_from_tweet(source.content, depth)
        elif source.type == SourceType.HANDLE:
            return await self._extract_from_handle(source.content, depth)
        elif source.type == SourceType.URL:
            return await self._extract_from_url(source.content, depth)
        elif source.type == SourceType.IMAGE_URL:
            return await self._extract_from_image_url(source.content, depth)
        elif source.type == SourceType.TEXT:
            result = ExtractedData()
            self._extract_from_text(source.content, result)
            return result
        elif source.type == SourceType.IMAGE_DATA:
            return await self._extract_from_image(source.content, depth)
        else:
            return ExtractedData()

    async def _extract_from_tweet(self, tweet_id: str, depth: int) -> ExtractedData:
        tweet_data = await self.twitter_client.get_tweet_by_rest_id(tweet_id)
        if not tweet_data:
            return ExtractedData()

        result = ExtractedData()
        result.tweets.add(tweet_id)

        # Extract from tweet text
        text = tweet_data.get('full_text', '')
        self._extract_from_text(text, result)

        # Extract from URLs in tweet
        urls = tweet_data.get('websites', [])
        url_tasks = [self.extract_from_source(Source(SourceType.URL, url), depth - 1) for url in urls]
        url_results = await asyncio.gather(*url_tasks)
        for url_result in url_results:
            self._merge_results(result, url_result)

        # Extract from user mentions
        mentions = tweet_data.get('handles', [])
        mention_tasks = [self.extract_from_source(Source(SourceType.HANDLE, mention), depth - 1) for mention in mentions]
        mention_results = await asyncio.gather(*mention_tasks)
        for mention_result in mention_results:
            self._merge_results(result, mention_result)

        # Extract from media
        media = tweet_data.get('pic_links', [])
        media_tasks = []
        for item in media:
            media_tasks.append(self.extract_from_source(Source(SourceType.IMAGE_URL, item), depth - 1))
        media_results = await asyncio.gather(*media_tasks)
        for media_result in media_results:
            self._merge_results(result, media_result)

        return result

    async def _extract_from_handle(self, handle: str, depth: int) -> ExtractedData:
        user_data = await self.twitter_client.get_user_by_username(handle.lstrip('@'))
        print(user_data)
        if not user_data:
            return ExtractedData()

        user_id = user_data.get('rest_id')
        if not user_id:
            return ExtractedData()

        result = ExtractedData()
        result.handles.add(f"{handle}")

        # Extract from user profile
        profile_text = f"{user_data.get('legacy', {}).get('name', '')} {user_data.get('legacy', {}).get('description', '')}"
        self._extract_from_text(profile_text, result)

        # Extract websites from profile
        profile_url = user_data.get('legacy', {}).get('url')
        if profile_url:
            profile_result = await self.extract_from_source(Source(SourceType.URL, profile_url), depth - 1)
            self._merge_results(result, profile_result)

        # Extract entities (links, mentions) from profile description
        entities = user_data.get('legacy', {}).get('entities', {})
        description_urls = entities.get('description', {}).get('urls', [])
        for url_data in description_urls:
            expanded_url = url_data.get('expanded_url')
            if expanded_url:
                url_result = await self.extract_from_source(Source(SourceType.URL, expanded_url), depth - 1)
                self._merge_results(result, url_result)

        # Extract from pinned tweet
        pinned_tweet_ids = user_data.get('legacy', {}).get('pinned_tweet_ids_str', [])
        if pinned_tweet_ids:
            pinned_tweet_result = await self.extract_from_source(Source(SourceType.TWEET, pinned_tweet_ids[0]), depth - 1)
            self._merge_results(result, pinned_tweet_result)

        # Extract from latest tweet
        latest_tweets = await self.twitter_client.get_user_tweets(user_id, limit=1)
        if latest_tweets:
            latest_tweet_result = await self.extract_from_source(Source(SourceType.TWEET, latest_tweets[0]['id_str']), depth - 1)
            self._merge_results(result, latest_tweet_result)

        # Search tweets from user with keywords
        keywords = ["ca", "contract"]
        tweets = await self.twitter_client.search_tweets_from_user_with_keywords(handle[1:], keywords, count=100)
        
        if tweets:
            all_full_text = ' '.join(tweet['full_text'] for tweet in tweets)
            tweet_result = await self.extract_from_source(Source(SourceType.TEXT, all_full_text), depth - 1)
            self._merge_results(result, tweet_result)

        return result

    async def _extract_from_url(self, url: str, depth: int) -> ExtractedData:
        try:
            async with self.http_client.get(url, timeout=10) as response:
                if response.status != 200:
                    return ExtractedData()
                content = await response.text()
        except Exception as e:
            print(f"Error fetching URL {url}: {str(e)}")
            return ExtractedData()

        result = ExtractedData()
        result.urls.add(url)

        soup = BeautifulSoup(content, 'html.parser')
        text = soup.get_text()

        # Extract all href links as websites
        for a in soup.find_all('a', href=True):
            href = a['href']
            if not href.startswith('http'):
                href = urljoin(url, href)
            result.urls.add(href)
            text = text + " " +href

        self._extract_from_text(text, result)

        # Extract from images on the page
        img_tasks = []
        for img in soup.find_all('img', src=True):
            img_url = img['src']
            if not img_url.startswith('http'):
                img_url = urljoin(url, img_url)
            img_tasks.append(self.extract_from_source(Source(SourceType.IMAGE_URL, img_url), depth - 1))
        img_results = await asyncio.gather(*img_tasks)
        for img_result in img_results:
            self._merge_results(result, img_result)

        return result

    async def _extract_from_image_url(self, url: str, depth: int) -> ExtractedData:
        try:
            async with self.http_client.get(url, timeout=10) as response:
                if response.status != 200:
                    return ExtractedData()
                image_data = await response.read()
        except Exception as e:
            print(f"Error fetching image URL {url}: {str(e)}")
            return ExtractedData()

        return await self._extract_from_image(image_data, depth)

    async def _extract_from_image(self, image_data: bytes, depth: int) -> ExtractedData:
        result = ExtractedData()

        try:
            image = Image.open(io.BytesIO(image_data))
            text = pytesseract.image_to_string(image)
            self._extract_from_text(text, result)
        except Exception as e:
            print(f"Error extracting from image: {str(e)}")

        return result

    def _extract_from_text(self, text: str, result: ExtractedData) -> None:
        result.contracts.update(contract.lower() for contract in self.contract_pattern.findall(text))
        result.tickers.update(self.ticker_pattern.findall(text))
        result.urls.update(self.url_pattern.findall(text))
        result.handles.update(self.handle_pattern.findall(text))

    def _merge_results(self, target: ExtractedData, source: ExtractedData) -> None:
        target.contracts.update(source.contracts)
        target.tickers.update(source.tickers)
        target.urls.update(source.urls)
        target.handles.update(source.handles)
        target.tweets.update(source.tweets)

# Usage example:
# async def main():
#     twitter_client = TwitterMonitorClient()
#     async with aiohttp.ClientSession() as http_client:
#         extractor = ContentExtractor(twitter_client, http_client)
        
#         handle = Source(SourceType.HANDLE, "@example_user")
#         result = await extractor.extract_from_source(handle, depth=2)
#         print("Extraction Result from handle:", result)
        
#         tweet_data = {
#             'id_str': '1234567890',
#             'full_text': 'Check out this $TICKER and ETH address 0x742d35Cc6634C0532925a3b844Bc454e4438f44e',
#             'entities': {
#                 'urls': [{'expanded_url': 'https://example.com'}],
#                 'user_mentions': [{'screen_name': 'mentioned_user'}],
#                 'media': [{'media_url_https': 'https://pbs.twimg.com/media/example.jpg'}]
#             }
#         }
#         tweet_source = Source(SourceType.TWEET, tweet_data['id_str'])
#         result = await extractor.extract_from_source(tweet_source, depth=2)
#         print("Extraction Result from tweet:", result)

# if __name__ == "__main__":
#     asyncio.run(main())