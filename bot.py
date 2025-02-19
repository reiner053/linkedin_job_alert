import asyncio
import logging
import os
import platform
import random
import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from jobspy import scrape_jobs

# Load environment variables
load_dotenv()

# --- Logging Setup ---
class LoggingFormatter(logging.Formatter):
    black = "\x1b[30m"
    red = "\x1b[31m"
    green = "\x1b[32m"
    yellow = "\x1b[33m"
    blue = "\x1b[34m"
    gray = "\x1b[38m"
    reset = "\x1b[0m"
    bold = "\x1b[1m"

    COLORS = {
        logging.DEBUG: gray + bold,
        logging.INFO: blue + bold,
        logging.WARNING: yellow + bold,
        logging.ERROR: red,
        logging.CRITICAL: red + bold,
    }

    def format(self, record):
        log_color = self.COLORS[record.levelno]
        format = "(black){asctime}(reset) (levelcolor){levelname:<8}(reset) (green){name}(reset) {message}"
        format = format.replace("(black)", self.black + self.bold)
        format = format.replace("(reset)", self.reset)
        format = format.replace("(levelcolor)", log_color)
        format = format.replace("(green)", self.green + self.bold)
        formatter = logging.Formatter(format, "%Y-%m-%d %H:%M:%S", style="{")
        return formatter.format(record)

logger = logging.getLogger("discord_bot")
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(LoggingFormatter())
file_handler = logging.FileHandler(filename="discord.log", encoding="utf-8", mode="w")
file_handler_formatter = logging.Formatter(
    "[{asctime}] [{levelname:<8}] {name}: {message}", "%Y-%m-%d %H:%M:%S", style="{"
)
file_handler.setFormatter(file_handler_formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

# --- Database Setup ---
Base = declarative_base()
engine = create_engine("sqlite:///jobs.db", echo=False) # Database for generic job scraper
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

# --- Generic Job Models ---
class FullTimeJob(Base):
    __tablename__ = "full_time_jobs"

    id = Column(Integer, primary_key=True)
    description = Column(String)
    job_id = Column(String, unique=True)
    application_url = Column(String)
    job_title = Column(String)
    company_name = Column(String)
    company_url = Column(String)
    location = Column(String)

class BlockchainJob(Base):
    __tablename__ = "blockchain_jobs"

    id = Column(Integer, primary_key=True)
    description = Column(String)
    job_id = Column(String, unique=True)
    application_url = Column(String)
    job_title = Column(String)
    company_name = Column(String)
    company_url = Column(String)
    location = Column(String)

# ... (other generic job models: MobileJob, MachineLearningJob, InternJob, NG2025Job, NG2024Job) ...
class MobileJob(Base):
    __tablename__ = "mobile_jobs"

    id = Column(Integer, primary_key=True)
    description = Column(String)
    job_id = Column(String, unique=True)
    application_url = Column(String)
    job_title = Column(String)
    company_name = Column(String)
    company_url = Column(String)
    location = Column(String)

class MachinLearningJob(Base):
    __tablename__ = "machin_learning_jobs"

    id = Column(Integer, primary_key=True)
    description = Column(String)
    job_id = Column(String, unique=True)
    application_url = Column(String)
    job_title = Column(String)
    company_name = Column(String)
    company_url = Column(String)
    location = Column(String)

class InternJob(Base):
    __tablename__ = "intern_jobs"

    id = Column(Integer, primary_key=True)
    description = Column(String)
    job_id = Column(String, unique=True)
    application_url = Column(String)
    job_title = Column(String)
    company_name = Column(String)
    company_url = Column(String)
    location = Column(String)

class NG2025Job(Base):
    __tablename__ = "ng_2025_jobs"

    id = Column(Integer, primary_key=True)
    description = Column(String)
    job_id = Column(String, unique=True)
    application_url = Column(String)
    job_title = Column(String)
    company_name = Column(String)
    company_url = Column(String)
    location = Column(String)

class NG2024Job(Base):
    __tablename__ = "ng_2024_jobs"

    id = Column(Integer, primary_key=True)
    description = Column(String)
    job_id = Column(String)
    application_url = Column(String)
    job_title = Column(String)
    company_name = Column(String)
    company_url = Column(String)
    location = Column(String)

# Freelancer-specific database setup
freelancer_engine = create_engine('sqlite:///freelancer_jobs.db')  # Database for Freelancer jobs
Base.metadata.create_all(freelancer_engine)
FreelancerSession = sessionmaker(bind=freelancer_engine)
#session = FreelancerSession()


class FreelancerJob(Base):
    __tablename__ = 'freelancer_jobs'

    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    link = Column(String, unique=True, nullable=False)
    description = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(freelancer_engine)

# --- Configuration ---
blacklist_companies = {
    'Team Remotely Inc',
    'HireMeFast LLC',
    'Get It Recruit - Information Technology',
    "Offered.ai",
    "4 Staffing Corp",
    "myGwork - LGBTQ+ Business Community",
    "Patterned Learning AI",
    "Mindpal",
    "Phoenix Recruiting",
    "SkyRecruitment",
    "Phoenix Recruitment",
    "Patterned Learning Career",
    "SysMind",
    "SysMind LLC",
    "Motion Recruitment"
}

bad_roles = {
    "unpaid",
    "manager",
    "director",
    "vp",
    "Manager",
    "Director",
    "VP",
    "II",
    "III"
}

quarantined_2025_terms = {
    '2024',
    'intern',
    'internship'
}

quarantined_2024_terms = {
    '2025',
    'intern',
    'internship'
}

# --- Discord Bot Class ---
class CombinedJobBot(commands.Bot):
    def __init__(self, generic_session=None, freelancer_session=None) -> None:
        intents = discord.Intents.default()
        intents.messages = True
        super().__init__(
            command_prefix=None,
            intents=intents,
            help_command=None,
        )
        self.logger = logger
        self.generic_session = generic_session # session for jobspy
        self.freelancer_session = freelancer_session # session for freelancer
        # NG 2024 & 2025 search terms
        self.ng_2024_search_terms = [
            "new grad software engineer",
            "recent graduate software engineer",
            "junior software engineer"
        ]
        self.ng_2024_search_index = 0

        self.ng_2025_search_terms = [
            "2025 software engineer",
            "new grad 2025 software engineer",
            "software engineer recent graduate 2025",
            "2025 Data Scientist",
            "2025 Data Analyst",
            "2025 Data Engineer"
        ]
        self.ng_2025_search_index = 0

        #Freelancer URL
        self.freelancer_url = 'https://www.freelancer.com/jobs/?fixed=true&hourly=true&languages=en'

    @tasks.loop(minutes=1.0)
    async def status_task(self) -> None:
        await self.change_presence(activity=discord.Game('with jobs! 🎉'))

    @status_task.before_loop
    async def before_status_task(self) -> None:
        await self.wait_until_ready()

    async def setup_hook(self) -> None:
        self.logger.info(f"Logged in as {self.user.name}")
        self.logger.info(f"discord.py API version: {discord.__version__}")
        self.logger.info(f"Python version: {platform.python_version()}")
        self.logger.info(
            f"Running on: {platform.system()} {platform.release()} ({os.name})"
        )
        self.logger.info("-------------------")
        self.status_task.start()
        self.job_posting_task.start()  # Start the generic job posting task
        self.freelancer_job_task.start() # start freelancer

    # --- Generic Job Posting ---
    async def post_jobs(self, jobs, channel_id: int):
        target_channel = self.get_channel(channel_id)
        if target_channel is None:
            self.logger.error(f"No channel with ID {channel_id} found.")
        else:
            if channel_id == int(os.getenv('FT_CHANNEL_ID')):
                JobModel = FullTimeJob
                quarantine_terms = set()
                channel_name = "Full-Time Jobs"
                required_terms = ["engineer", "technology", "developer", "software", "entry level", "entry", "mid level", "senior"]
            elif channel_id == int(os.getenv('BC_CHANNEL_ID')):
                JobModel = BlockchainJob
                quarantine_terms = set()
                channel_name = "Blockchain Jobs"
                required_terms = ["engineer", "technology", "developer", "software", "entry level", "entry", "blockchain", "web3", "solidity", "smart contract", "mid level", "senior"]
            elif channel_id == int(os.getenv('MO_CHANNEL_ID')):
                JobModel = MobileJob
                quarantine_terms = set()
                channel_name = "Mobile Jobs"
                required_terms = ["engineer", "technology", "developer", "software", "entry level", "entry", "mid level", "senior", "mobile", "ios", "swift", "react native"]
            elif channel_id == int(os.getenv('ML_CHANNEL_ID')):
                JobModel = MachinLearningJob
                quarantine_terms = set()
                channel_name = "ML Jobs"
                required_terms = ["engineer", "technology", "developer", "software", "entry level", "entry", "mid level", "senior", "machine learning", "ai", "ocr"]
            elif channel_id == int(os.getenv('INTERN_CHANNEL_ID')):
                JobModel = InternJob
                quarantine_terms = set()
                channel_name = "Intern Jobs"
                required_terms = ["intern"]
            elif channel_id == int(os.getenv('NG_2025_CHANNEL_ID')):
                JobModel = NG2025Job
                quarantine_terms = quarantined_2025_terms
                channel_name = "NG 2025 Jobs"
                required_terms = ["engineer", "technology", "developer", "software", "new grad", "entry level", "entry"]
            elif channel_id == int(os.getenv('NG_2024_CHANNEL_ID')):
                JobModel = NG2024Job
                quarantine_terms = quarantined_2024_terms
                channel_name = "NG 2024 Jobs"
                required_terms = ["engineer", "technology", "developer", "software", "new grad", "entry level", "entry"]
            else:
                self.logger.error(f"Unknown channel ID: {channel_id}")
                return

            for index, row in jobs.iterrows():
                if row['company'] in blacklist_companies:
                    self.logger.info(
                        f"Skipping job from blacklisted company: {row['company']} in channel: {channel_name} (ID: {channel_id})")
                    continue

                if not any(term.lower() in row['title'].lower() for term in required_terms):
                    self.logger.info(
                        f"Skipping job with title '{row['title']}' as it does not contain any of the required terms {required_terms} in channel: {channel_name} (ID: {channel_id})")
                    continue

                if any(term.lower() in row['title'].lower() for term in quarantine_terms):
                    self.logger.info(
                        f"Skipping job with quarantined term in title: {row['title']} in channel: {channel_name} (ID: {channel_id})")
                    continue

                if any(term.lower() in row['title'].lower() for term in bad_roles):
                    self.logger.info(
                        f"Skipping job with bad role in title: {row['title']} in channel: {channel_name} (ID: {channel_id})")
                    continue

                query = self.generic_session.query(JobModel).filter(JobModel.job_id == row['id']).first()
                if query is None:
                    job_info = f""">>> ## {''.join(random.choices(['🎉', '👏', '💼', '🔥', '💻'], k=1))} [{row['company']}](<{row['company_url']}>) just posted a new job! 

### **Role:** 
[**{row['title']}**](<{row['job_url']}>)

### **Location:** 
{row['location']}
---
                    """
                    self.logger.info(f"Posting job: {row['title']} to channel: {channel_name} (ID: {channel_id})")
                    self.generic_session.add(JobModel(job_id=row['id'], application_url=row['job_url'], job_title=row['title'],
                                              company_name=row['company'], company_url=row['company_url'], location=row['location']))
                    await target_channel.send(job_info)
                else:
                    self.logger.info(
                        f"Job already exists in the database: {row['title']} in channel: {channel_name} (ID: {channel_id})")

            try:
                self.generic_session.commit()
            except Exception as e:
                self.logger.error(f"Error committing session: {e}")
                self.generic_session.rollback()

    @tasks.loop(seconds=0)
    async def job_posting_task(self):
        import asyncio
        await self.full_time_job_task()
        await asyncio.sleep(10)
        await self.blockchain_job_task()
        await asyncio.sleep(10)
        await self.mobile_job_task()
        await asyncio.sleep(10)
        await self.ml_job_task()
        await asyncio.sleep(10)
        # await self.ng_2025_job_task()
        # await asyncio.sleep(10)
        # await self.ng_2024_job_task()
        await asyncio.sleep(10)
        # await self.intern_job_task()
        self.logger.info("Job posting task completed.")

    async def full_time_job_task(self):
        channel_id = int(os.getenv('FT_CHANNEL_ID'))
        full_time_jobs = await self.get_jobs(search_term="software engineer", results_wanted=50)
        await self.post_jobs(full_time_jobs, channel_id)

    async def blockchain_job_task(self):
        channel_id = int(os.getenv('BC_CHANNEL_ID'))
        full_time_jobs = await self.get_jobs(search_term="blockchain", results_wanted=50)
        await self.post_jobs(full_time_jobs, channel_id)

    async def mobile_job_task(self):
        channel_id = int(os.getenv('MO_CHANNEL_ID'))
        full_time_jobs = await self.get_jobs(search_term="mobile", results_wanted=50)
        await self.post_jobs(full_time_jobs, channel_id)

    async def ml_job_task(self):
        channel_id = int(os.getenv('ML_CHANNEL_ID'))
        full_time_jobs = await self.get_jobs(search_term="machine learning", results_wanted=50)
        await self.post_jobs(full_time_jobs, channel_id)

    async def intern_job_task(self):
        channel_id = int(os.getenv('INTERN_CHANNEL_ID'))
        intern_jobs = await self.get_jobs(hours_old=10)
        await self.post_jobs(intern_jobs, channel_id)

    async def ng_2025_job_task(self):
        channel_id = int(os.getenv('NG_2025_CHANNEL_ID'))
        ng_2025_search_term = self.ng_2025_search_terms[self.ng_2025_search_index]

        self.logger.info(
            f"Running NG 2025 job task for channel ID: {channel_id} with search term '{ng_2025_search_term}'")
        jobs = await self.get_jobs(search_term=ng_2025_search_term, hours_old=10)
        self.logger.info(f"Found {len(jobs)} jobs for NG 2025 channel using '{ng_2025_search_term}'.")

        await self.post_jobs(jobs, channel_id)
        self.ng_2025_search_index = (self.ng_2025_search_index + 1) % len(self.ng_2025_search_terms)

    async def ng_2024_job_task(self):
        channel_id = int(os.getenv('NG_2024_CHANNEL_ID'))
        current_search_term = self.ng_2024_search_terms[self.ng_2024_search_index]

        self.logger.info(
            f"Running NG 2024 job task for channel ID: {channel_id} with search term '{current_search_term}'")
        jobs = await self.get_jobs(search_term=current_search_term, hours_old=10)
        self.logger.info(f"Found {len(jobs)} jobs for NG 2024 channel using '{current_search_term}'.")

        await self.post_jobs(jobs, channel_id)
        self.ng_2024_search_index = (self.ng_2024_search_index + 1) % len(self.ng_2024_search_terms)

    async def get_jobs(self, sites=None, search_term='software engineer intern', location='United States, Remote',
                       results_wanted=50, hours_old=24):
        if sites is None:
            sites = ['linkedin']
        jobs = scrape_jobs(
            site_name=sites,
            search_term=search_term,
            location=location,
            results_wanted=results_wanted,
            hours_old=hours_old,
        )
        return jobs

    # --- Freelancer Job Posting ---
    async def fetch_freelancer_jobs(self):
        try:
            response = requests.get(self.freelancer_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            job_cards = soup.find_all('div', class_='JobSearchCard-item')

            new_jobs = []
            for job in job_cards:
                title_element = job.find('a', class_='JobSearchCard-primary-heading-link')
                title = title_element.text.strip() if title_element else "No Title"
                link = f"https://www.freelancer.com{title_element['href']}" if title_element else "No Link"
                description_element = job.find('p', class_='JobSearchCard-primary-description')
                description = description_element.text.strip() if description_element else "No Description"
                new_jobs.append((title, link, description))

            print(f"New Jobs length", len(new_jobs))
            return new_jobs
        except Exception as e:
            print(f"Error fetching jobs: {e}")
            return []

    async def post_freelancer_jobs(self):
        freelancer_channel_id = int(os.getenv('FREELANCER_CHANNEL_ID'))
        await self.wait_until_ready()
        channel = self.get_channel(freelancer_channel_id)

        if channel is None:
            self.logger.error(f"No channel with ID {freelancer_channel_id} found for Freelancer jobs.")
            return

        session = FreelancerSession()
        jobs = await self.fetch_freelancer_jobs()

        for title, link, description in jobs:
            existing_job = session.query(FreelancerJob).filter_by(link=link).first()
            if not existing_job:
                new_job = FreelancerJob(title=title, link=link, description=description)
                try:
                    session.add(new_job)
                    session.commit()

                    embed = discord.Embed(title=title, url=link, description=description, color=0x00ff00)
                    embed.set_footer(text="Freelancer Job Alert")
                    await channel.send(embed=embed)
                    print(f"Sent Freelancer job: {title}")
                except IntegrityError as e:
                    session.rollback()
                    print(f"Freelancer Job already exists: {title}, error: {e}")
            else:
                print(f"Freelancer job Existed: {title}")
        session.close()

    @tasks.loop(minutes=1)
    async def freelancer_job_task(self):
       await self.post_freelancer_jobs()


# --- Main ---
async def main():
    generic_session = Session() # Create session before bot
    freelancer_session = FreelancerSession()
    bot = CombinedJobBot(generic_session=generic_session, freelancer_session=freelancer_session) #inject sessions
    try:
        await bot.start(os.getenv("TOKEN"))
    finally:
        await bot.close()  # Ensure the bot connection is closed
        generic_session.close() # Close the session after the bot is done
        freelancer_session.close()

if __name__ == "__main__":
    asyncio.run(main())
