import asyncio
import time
from typing import AsyncGenerator, TypeVar

import pycountry
from twscrape import Tweet, User, gather

from config import TwitterConfig
from constants.time_constant import TimeConstants
from constants.twitter_constant import Follow, Tweets, TwitterUser
from databases.mongodb_cdp import MongoDBCDP
from databases.mongodb_main import MongoDBMain
from src.crawler.new_api import NewAPI
from utils.logger_utils import get_logger
from utils.time_utils import round_timestamp

T = TypeVar("T")
logger = get_logger("Twitter Crawling Job")


class TwitterProjectCrawlingJob:
    def __init__(
        self,
        period: int,
        limit: int,
        mongodb_main: MongoDBMain,
        mongodb_cdp: MongoDBCDP,
        user_name: str = TwitterConfig.USERNAME,
        password: str = TwitterConfig.PASSWORD,
        email: str = TwitterConfig.EMAIL,
        email_password: str = TwitterConfig.EMAIL_PASSWORD,
    ):
        self.data_types = ["profiles", "tweets"]
        self.period = period
        self.limit = limit
        self.email_password = email_password
        self.email = email
        self.password = password
        self.user_name = user_name
        self.api = None
        self.mongodb_main = mongodb_main
        self.mongodb_cdp = mongodb_cdp

    @staticmethod
    def convert_user_to_dict(user: User) -> dict:
        text = user.location
        country_name = ""
        for country in pycountry.countries:
            if country.name.lower() in text.lower():
                country_name = country.name
                break

        return {
            TwitterUser.id_: str(user.id),
            TwitterUser.user_name: user.username,
            TwitterUser.display_name: user.displayname,
            TwitterUser.url: user.url,
            TwitterUser.blue: user.blue,
            TwitterUser.blue_type: user.blueType,
            TwitterUser.created_at: str(user.created),
            TwitterUser.timestamp: int(user.created.timestamp()),
            TwitterUser.description_links: [str(i.url) for i in user.descriptionLinks],
            TwitterUser.favourites_count: user.favouritesCount,
            TwitterUser.friends_count: user.friendsCount,
            TwitterUser.listed_count: user.listedCount,
            TwitterUser.media_count: user.mediaCount,
            TwitterUser.followers_count: user.followersCount,
            TwitterUser.statuses_count: user.statusesCount,
            TwitterUser.raw_description: user.rawDescription,
            TwitterUser.verified: user.verified,
            TwitterUser.profile_image_url: user.profileImageUrl,
            TwitterUser.profile_banner_url: user.profileBannerUrl,
            TwitterUser.protected: user.protected,
            TwitterUser.location: user.location,
            TwitterUser.country: country_name,
            TwitterUser.count_logs: {
                round_timestamp(time.time()): {
                    TwitterUser.favourites_count: user.favouritesCount,
                    TwitterUser.friends_count: user.friendsCount,
                    TwitterUser.listed_count: user.listedCount,
                    TwitterUser.media_count: user.mediaCount,
                    TwitterUser.followers_count: user.followersCount,
                    TwitterUser.statuses_count: user.statusesCount,
                }
            },
        }

    @staticmethod
    def convert_tweets_to_dict(self, tweet: Tweet) -> dict:
        if not tweet:
            return {}
        result = {
            Tweets.id_: str(tweet.id),
            Tweets.author: str(tweet.user.id),
            Tweets.author_name: tweet.user.username,
            Tweets.created_at: str(tweet.date),
            Tweets.timestamp: tweet.date.timestamp(),
            Tweets.url: str(tweet.url),
            Tweets.user_mentions: {
                str(user.id): user.username for user in tweet.mentionedUsers
            },
            Tweets.views: tweet.viewCount,
            Tweets.likes: tweet.likeCount,
            Tweets.hash_tags: tweet.hashtags,
            Tweets.reply_counts: tweet.replyCount,
            Tweets.retweet_counts: tweet.retweetCount,
            Tweets.retweeted_tweet: self.convert_tweets_to_dict(tweet.retweetedTweet),
            Tweets.text: tweet.rawContent,
            Tweets.quoted_tweet: self.convert_tweets_to_dict(tweet.quotedTweet),
        }
        if time.time() - result.get(Tweets.timestamp) < self.period:
            result[Tweets.impression_logs] = {
                str(int(time.time())): {
                    Tweets.views: tweet.viewCount,
                    Tweets.likes: tweet.likeCount,
                    Tweets.reply_counts: tweet.replyCount,
                    Tweets.retweet_counts: tweet.retweetCount,
                }
            }

        return result

    @staticmethod
    def get_relationship(project, user):
        return {
            Follow.id_: f"{user}_{project}",
            Follow.from_: str(user),
            Follow.to: str(project),
        }

    async def gather(
        self,
        gen: AsyncGenerator[T, None],
        project,
        time_sleep: int = 1,
        n_items: int = 1000,
    ) -> int:
        tmp = 0
        async for x in gen:
            self.mongodb_main.update_docs(
                "twitter_users", [self.convert_user_to_dict(x)]
            )
            self.mongodb_main.update_docs(
                "twitter_follows", [self.get_relationship(project, x.id)]
            )
            tmp += 1
            if tmp and not (tmp % n_items):
                time.sleep(time_sleep)
        return tmp

    async def execute(self):
        api = NewAPI()
        await api.pool.add_account(
            self.user_name, self.password, self.email, self.email_password
        )
        await api.pool.login_all()

        list_projects = self.mongodb_cdp.get_docs(collection="projects_social_media")
        list_account = []
        for project in list_projects:
            if "twitter" in project:
                list_account.append(project.get("twitter").get("id"))

        tmp = 0
        for account in list_account:
            tmp += 1
            try:
                if "profiles" in self.data_types:
                    logger.info(f"Crawling {account} info")
                    project_info = await api.user_by_login(account)
                    self.mongodb_main.update_docs(
                        "twitter_users", [self.convert_user_to_dict(project_info)]
                    )

                    logger.info(f"Crawled {tmp}/{len(list_account)} projects")

                if "tweets" in self.data_types:
                    logger.info(f"Crawling {account} tweets info")
                    project_info = await api.user_by_login(account)
                    if project_info is None:
                        continue
                    if self.limit is None:
                        # Get all tweet, limit = -1
                        tweets = await gather(api.user_tweets(project_info.id))
                    else:
                        tweets = await gather(
                            api.user_tweets(project_info.id, limit=self.limit)
                        )
                    count = 0
                    _period = (
                        round_timestamp(time.time()) - self.period + TimeConstants.A_DAY
                    )
                    for tweet in tweets:
                        if self.convert_tweets_to_dict(tweet)["timestamp"] > _period:
                            count += 1
                            self.mongodb_main.update_docs(
                                "twitter_tweets", [self.convert_tweets_to_dict(tweet)]
                            )

                    logger.info(f"Crawled {count} tweets of {account}")

            except Exception as e:
                logger.warn(f"Get err {e}")
                logger.info("Continuing in 3 seconds...")
                await asyncio.sleep(3)
