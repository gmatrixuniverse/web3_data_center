import asyncio
from datetime import datetime, timedelta, timezone
from collections import deque
import json
from typing import Dict, Any, Optional, List
from .base_client import BaseClient
from ..utils.logger import get_logger

logger = get_logger(__name__)

class TwitterMonitorClient(BaseClient):
    def __init__(self, config_path: str = "config.yml", use_proxy: bool = True):
        super().__init__(api_name="x", config_path=config_path, use_proxy=use_proxy)
        self.monitored_accounts: Dict[str, str] = {}
        self.processed_tweets = deque(maxlen=1000)
        self.headers.update({
            "x-rapidapi-host": "twitter-v1-1-v2-api.p.rapidapi.com",
        })
        self.initialization_time: Optional[datetime] = None
        self.last_check_time: Optional[datetime] = None
        self.user_auth_token = self.config['api']['x']['auth_token']
        self.list_id = self.config['api']['x']['list_id']
        self.add_route = self.config['api']['x']['add_route']
        self.remove_route = self.config['api']['x']['remove_route']
        self.list_route = self.config['api']['x']['list_route']
        self.timeline_route = self.config['api']['x']['timeline_route']
        self.query_id = self.config['api']['x']['query_id']

    def set_parameters(self, **kwargs):
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                logger.warning(f"Attribute {key} does not exist in TwitterMonitorClient")

    async def initialize(self):
        self.initialization_time = datetime.now(timezone.utc)
        self.last_check_time = self.initialization_time
        logger.info(f"Initialized XMonitor. Last check time set to {self.last_check_time}")
        await self.initialize_list_members()

    async def add_account(self, screen_name: str) -> bool:
        user_id = await self.get_user_id(screen_name)
        if not user_id:
            logger.warning(f"Failed to add {screen_name} to monitored accounts. User ID not found.")
            return False
        print(f"{screen_name} Monitored accounts: {self.monitored_accounts}")
        if screen_name in self.monitored_accounts:
            logger.info(f"{screen_name} is already in monitored accounts with ID: {self.monitored_accounts[screen_name]}")
            return False
        
        result = await self._add_member_to_list(user_id)
        if self._is_operation_successful(result):
            self.monitored_accounts[screen_name] = user_id
            logger.info(f"Added {screen_name} (ID: {user_id}) to monitored accounts and the Twitter list.")
            return True
        return False

    async def remove_account(self, screen_name: str) -> bool:
        if screen_name not in self.monitored_accounts:
            logger.warning(f"{screen_name} is not in monitored accounts.")
            return False

        user_id = self.monitored_accounts[screen_name]
        result = await self._remove_member_from_list(user_id)
        if self._is_operation_successful(result):
            del self.monitored_accounts[screen_name]
            logger.info(f"Removed {screen_name} (ID: {user_id}) from monitored accounts and the Twitter list.")
            return True
        return False

    async def _add_member_to_list(self, user_id: str) -> Dict[str, Any]:
        if not self.add_route:
            raise ValueError("add_route is not initialized")
        if not self.user_auth_token:
            raise ValueError("user_auth_token is not initialized")
        if not self.query_id:
            raise ValueError("query_id is not initialized")
        if not self.list_id:
            raise ValueError("list_id is not initialized")
        endpoint = f"/graphql/{self.add_route}/ListAddMember"
        payload = self._get_list_operation_payload(user_id)
        headers = self.headers.copy()
        headers["AuthToken"] = self.user_auth_token
        return await self._make_request(method="POST", endpoint=endpoint, data=payload, headers=headers)

    async def _remove_member_from_list(self, user_id: str) -> Dict[str, Any]:
        if not self.remove_route:
            raise ValueError("remove_route is not initialized")
        if not self.user_auth_token:
            raise ValueError("user_auth_token is not initialized")
        if not self.query_id:
            raise ValueError("query_id is not initialized")
        if not self.list_id:
            raise ValueError("list_id is not initialized")
        endpoint = f"/graphql/{self.remove_route}/ListRemoveMember"
        payload = self._get_list_operation_payload(user_id)
        headers = self.headers.copy()
        headers["AuthToken"] = self.user_auth_token
        return await self._make_request(method="POST", endpoint=endpoint, data=payload, headers=headers)

    def _get_list_operation_payload(self, user_id: str) -> Dict[str, Any]:
        return {
            "variables": {"listId": self.list_id, "userId": user_id},
            "features": {
                "rweb_tipjar_consumption_enabled": True,
                "responsive_web_graphql_exclude_directive_enabled": True,
                "verified_phone_label_enabled": False,
                "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
                "responsive_web_graphql_timeline_navigation_enabled": True
            },
            "queryId": self.query_id
        }

    def _is_operation_successful(self, result: Optional[Dict[str, Any]]) -> bool:
        if result is None:
            return False
        if isinstance(result, dict):
            if 'error' in result:
                logger.error(f"API Error: {result['error']}")
                return False
            if 'data' in result and isinstance(result['data'], dict) and 'list' in result['data']:
                return True
        logger.warning(f"Unexpected API response: {result}")
        return False

    async def check_for_new_posts(self):
        if not self.timeline_route:
            raise ValueError("timeline_route is not initialized")
        if not self.list_id:
            raise ValueError("list_id is not initialized")
        logger.info("Checking for new posts...")
        current_time = datetime.now(timezone.utc)
        new_posts = []
        try:
            endpoint = f"/graphql/{self.timeline_route}/ListLatestTweetsTimeline"
            variables = {
                "listId": self.list_id,
                "count": 5,
                "cursor": None  # Add cursor handling if needed
            }
            features = {
                "rweb_tipjar_consumption_enabled": True,
                "responsive_web_graphql_exclude_directive_enabled": True,
                "verified_phone_label_enabled": False,
                "creator_subscriptions_tweet_preview_api_enabled": True,
                "responsive_web_graphql_timeline_navigation_enabled": True,
                "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
                "communities_web_enable_tweet_community_results_fetch": True,
                "c9s_tweet_anatomy_moderator_badge_enabled": True,
                "articles_preview_enabled": True,
                "responsive_web_edit_tweet_api_enabled": True,
                "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
                "view_counts_everywhere_api_enabled": True,
                "longform_notetweets_consumption_enabled": True,
                "responsive_web_twitter_article_tweet_consumption_enabled": True,
                "tweet_awards_web_tipping_enabled": False,
                "creator_subscriptions_quote_tweet_preview_enabled": False,
                "freedom_of_speech_not_reach_fetch_enabled": True,
                "standardized_nudges_misinfo": True,
                "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
                "rweb_video_timestamps_enabled": True,
                "longform_notetweets_rich_text_read_enabled": True,
                "longform_notetweets_inline_media_enabled": True,
                "responsive_web_enhance_cards_enabled": False
            }
            params = {
                "variables": json.dumps(variables),
                "features": json.dumps(features)
            }
            
            response = await self._make_request(method="GET", endpoint=endpoint, params=params)
            if response:
                instructions = response.get('data', {}).get('list', {}).get('tweets_timeline', {}).get('timeline', {}).get('instructions', [])
                for instruction in instructions:
                    if instruction.get('type') == 'TimelineAddEntries':
                        entries = instruction.get('entries', [])
                        for entry in entries:
                            tweet = entry.get('content', {}).get('itemContent', {}).get('tweet_results', {}).get('result', {})
                            if tweet:
                                tweet_id = tweet.get('rest_id')
                                if tweet_id is None:
                                    continue
                                created_at = datetime.strptime(tweet.get('legacy', {}).get('created_at', 'Tue Sep 03 23:22:54 +0000 2024'), '%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=timezone.utc)
                                # print(created_at, self.initialization_time, tweet_id)
                                if created_at > self.last_check_time and tweet_id not in self.processed_tweets:
                                    self.processed_tweets.append(tweet_id)
                                    legacy = tweet.get('legacy', {})
                                    user = tweet.get('core', {}).get('user_results', {}).get('result', {}).get('legacy', {})
                                                                        # Get the tweet text and URLs
                                    text = legacy.get('full_text', '')
                                    urls = legacy.get('entities', {}).get('urls', [])
                                    
                                    # Replace shortened URLs with expanded ones
                                    for url in urls:
                                        short_url = url.get('url', '')
                                        expanded_url = url.get('expanded_url', '')
                                        if short_url and expanded_url:
                                            text = text.replace(short_url, expanded_url)
                                    
                                    new_post = {
                                        'id': tweet_id,
                                        'text': text,
                                        'user': user.get('screen_name'),
                                        'created_at': created_at
                                    }
                                    new_posts.append(new_post)
                                    logger.debug(f"New post added: {new_post}")
                                    self.last_check_time = current_time
            else:
                logger.error("Failed to fetch new posts.")
            
            logger.info(f"Found {len(new_posts)} new posts")
        except Exception as e:
            logger.error(f"Error checking for new posts: {str(e)}")
            logger.exception("Exception details:")
        
        return new_posts

    def _parse_search_results(self, data):
        search_results = []
        instructions = data.get('data', {}).get('search_by_raw_query', {}).get('search_timeline', {}).get('timeline', {}).get('instructions', [])
        
        for instruction in instructions:
            if instruction.get('type') == 'TimelineAddEntries':
                entries = instruction.get('entries', [])
                for entry in entries:
                    tweet = entry.get('content', {}).get('itemContent', {}).get('tweet_results', {}).get('result', {})
                    if tweet:
                        legacy = tweet.get('legacy', {})
                        user = tweet.get('core', {}).get('user_results', {}).get('result', {}).get('legacy', {})
                        entities = legacy.get('entities', {})
                        extended_entities = legacy.get('extended_entities', {})
                        
                        search_results.append({
                            'id': legacy.get('id_str'),
                            'full_text': legacy.get('full_text'),
                            'user': user.get('screen_name'),
                            'created_at': legacy.get('created_at'),
                            'handles': [f"@{mention['screen_name']}" for mention in entities.get('user_mentions', [])],
                            'websites': [url['expanded_url'] for url in entities.get('urls', [])],
                            'pic_links': [media['media_url_https'] for media in extended_entities.get('media', []) if media['type'] == 'photo']
                        })

        return search_results

    async def search_tweets(self, query: str, count: int = 40, cursor: str = "") -> Optional[Dict[str, Any]]:
        endpoint = "/graphql/SearchTimeline"
        variables = {
            "rawQuery": query,
            "count": count,
            "cursor": cursor,
            "querySource": "typed_query",
            "product": "Latest",
            "includePromotedContent": False
        }
        params = {
            "variables": json.dumps(variables)
        }

        try:
            data = await self._make_request(method="GET", endpoint=endpoint, params=params)
            return self._parse_search_results(data)
        except Exception as e:
            logger.error(f"Error searching tweets: {str(e)}")
            return None

    async def search_tweets_with_date_logic(self, handle: str, min_faves: int = 30) -> Optional[Dict[str, Any]]:
        since_date, until_date = self.get_date_range_for_search()
        query = f"from:{handle} min_faves:{min_faves} since:{since_date} until:{until_date}"
        return await self.search_tweets(query)

    async def search_tweets_from_user_with_keywords(self, handle: str, keywords: List[str], count: int = 40) -> Optional[Dict[str, Any]]:
        query = f"from:{handle}"
        if keywords:
            query += f" ({' OR '.join(keywords)})"
        return await self.search_tweets(query, count)

    async def get_user_id(self, screen_name: str) -> Optional[str]:
        endpoint = "/graphql/UserByScreenName"
        variables = {
            "screen_name": screen_name,
            "withSafetyModeUserFields": True,
            "withHighlightedLabel": True
        }
        params = {"variables": json.dumps(variables)}
        
        try:
            data = await self._make_request(method="GET", endpoint=endpoint, params=params)
            # print(data)
            user_data = data.get('data', {}).get('user', {}).get('result', {})
            user_id = user_data.get('rest_id')
            if user_id:
                logger.info(f"Found user ID for {screen_name}: {user_id}")
                return user_id
            else:
                logger.warning(f"User ID not found for {screen_name}")
                return None
        except Exception as e:
            logger.error(f"Error getting user ID for {screen_name}: {str(e)}")
            return None

    async def get_user_by_username(self, screen_name: str) -> Optional[str]:
        endpoint = "/graphql/UserByScreenName"
        variables = {
            "screen_name": screen_name,
            "withSafetyModeUserFields": True,
            "withHighlightedLabel": True
        }
        params = {"variables": json.dumps(variables)}
        
        try:
            data = await self._make_request(method="GET", endpoint=endpoint, params=params)
            user_data = data.get('data', {}).get('user', {}).get('result', {})
            if user_data:
                logger.info(f"Found user data for {screen_name}")
                return user_data
            else:
                logger.warning(f"User data not found for {screen_name}")
                return None
        except Exception as e:
            logger.error(f"Error getting user data for {screen_name}: {str(e)}")
            return None


    async def get_tweet_by_rest_id(self, tweet_id: str) -> Optional[Dict[str, Any]]:
        endpoint = "/graphql/TweetResultByRestId"
        variables = {
            "tweetId": tweet_id,
            "withHighlightedLabel": True,
            "withTweetQuoteCount": True,
            "includePromotedContent": True,
            "withBirdwatchPivots": True,
            "withVoice": True,
            "withReactions": True
        }
        params = {"variables": json.dumps(variables)}
        
        try:
            data = await self._make_request(method="GET", endpoint=endpoint, params=params)
            tweet_result = data.get('data', {}).get('tweetResult', {}).get('result', {})
            print(f"Tweet result: {tweet_result}")
            if tweet_result and tweet_result.get('__typename') == 'Tweet':
                logger.info(f"Found tweet data for ID {tweet_id}")
                legacy = tweet_result.get('legacy', {})
                entities = legacy.get('entities', {})
                extended_entities = legacy.get('extended_entities', {})
                
                return {
                    'full_text': legacy.get('full_text', ''),
                    'handles': [f"@{mention['screen_name']}" for mention in entities.get('user_mentions', [])],
                    'websites': [url['expanded_url'] for url in entities.get('urls', [])],
                    'pic_links': [media['media_url_https'] for media in extended_entities.get('media', []) if media['type'] == 'photo']
                }
            else:
                logger.warning(f"Tweet data not found for ID {tweet_id}")
                return None
        except Exception as e:
            logger.error(f"Error getting tweet data for ID {tweet_id}: {str(e)}")
            return None

    async def get_user_tweets(self, user_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        endpoint = "/graphql/UserTweets"
        variables = {
            "userId": user_id,
            "count": limit,
            "includePromotedContent": True,
            "withBirdwatchPivots": True,
            "withVoice": True,
            "withReactions": True
        }
        params = {"variables": json.dumps(variables)}
        
        try:
            data = await self._make_request(method="GET", endpoint=endpoint, params=params)
            # print(f"Data: {data['data'][]}")
            instructions = data.get('data', {}).get('user', {}).get('result', {}).get('timeline', {}).get('timeline', {}).get('instructions', [])
            # print(f"Instructions: {instructions}")
            tweets = []
            for instruction in instructions:
                if instruction.get('type') == 'TimelineAddEntries':
                    entries = instruction.get('entries', [])
                    for entry in entries:
                        if entry.get('content', {}).get('entryType') == 'TimelineTimelineItem':
                            tweet_result = entry.get('content', {}).get('itemContent', {}).get('tweet_results', {}).get('result', {})
                            if tweet_result:
                                legacy = tweet_result.get('legacy', {})
                                tweets.append(legacy)
                                if len(tweets) >= limit:
                                    return tweets
            return tweets
        except Exception as e:
            logger.error(f"Error getting user tweets for ID {user_id}: {str(e)}")
            return []
    

    def get_date_range_for_search() -> tuple[str, str]:
        today = datetime.now().date()
        current_weekday = today.weekday()
        days_since_sunday = (current_weekday + 1) % 7
        most_recent_sunday = today - timedelta(days=days_since_sunday)
        
        if current_weekday < 2:
            since_date = most_recent_sunday - timedelta(days=7)
        else:
            since_date = most_recent_sunday
        
        return since_date.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')
    
    async def initialize_list_members(self):
        print(f"Initializing list members for {self.list_id}, with {self.list_route}")
        endpoint = f"/graphql/{self.list_route}/ListMembers"
        variables = {
            "listId": self.list_id,
            "count": 20,
            "withSafetyModeUserFields": True
        }
        features = {
            "rweb_tipjar_consumption_enabled": True,
            "responsive_web_graphql_exclude_directive_enabled": True,
            "verified_phone_label_enabled": False,
            "creator_subscriptions_tweet_preview_api_enabled": True,
            "responsive_web_graphql_timeline_navigation_enabled": True,
            "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
            "communities_web_enable_tweet_community_results_fetch": True,
            "c9s_tweet_anatomy_moderator_badge_enabled": True,
            "tweetypie_unmention_optimization_enabled": True,
            "responsive_web_edit_tweet_api_enabled": True,
            "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
            "view_counts_everywhere_api_enabled": True,
            "longform_notetweets_consumption_enabled": True,
            "responsive_web_twitter_article_tweet_consumption_enabled": True,
            "tweet_awards_web_tipping_enabled": False,
            "freedom_of_speech_not_reach_fetch_enabled": True,
            "standardized_nudges_misinfo": True,
            "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
            "rweb_video_timestamps_enabled": True,
            "longform_notetweets_rich_text_read_enabled": True,
            "longform_notetweets_inline_media_enabled": True,
            "responsive_web_enhance_cards_enabled": False,
            "articles_preview_enabled": True,
            "creator_subscriptions_quote_tweet_preview_enabled": False
        }
        params = {
            "variables": json.dumps(variables),
            "features": json.dumps(features)
        }
        
        try:
            data = await self._make_request(method="GET", endpoint=endpoint, params=params)
            
            if not data:
                logger.error("Failed to retrieve list members data")
                return
            
            # Clear existing monitored accounts before updating
            self.monitored_accounts.clear()
            
            instructions = data.get('data', {}).get('list', {}).get('members_timeline', {}).get('timeline', {}).get('instructions', [])
            for instruction in instructions:
                if instruction.get('type') == 'TimelineAddEntries':
                    entries = instruction.get('entries', [])
                    for entry in entries:
                        if entry.get('content', {}).get('entryType') == 'TimelineTimelineItem':
                            user_results = entry.get('content', {}).get('itemContent', {}).get('user_results', {}).get('result', {})
                            if user_results:
                                screen_name = user_results.get('legacy', {}).get('screen_name')
                                user_id = user_results.get('rest_id')
                                if screen_name and user_id:
                                    self.monitored_accounts[screen_name] = user_id
                                    logger.debug(f"Added {screen_name}: {user_id} to monitored accounts")
            
            logger.info(f"Initialized and updated {len(self.monitored_accounts)} monitored accounts from the list.")
        except Exception as e:
            logger.error(f"Error initializing list members: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

